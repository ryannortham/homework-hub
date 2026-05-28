"""Tests for the per-source auth status helper used by the Settings tab."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from homework_hub.config import (
    ChildConfig,
    ChildSources,
    CompassConfig,
    Settings,
    SimpleSourceConfig,
)
from homework_hub.pipeline.auth_status import (
    build_source_auth_rows,
)
from homework_hub.state.store import StateStore

# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


def _settings(tmp_path: Path) -> Settings:
    tokens = tmp_path / "tokens"
    tokens.mkdir(parents=True, exist_ok=True)
    return Settings(
        config_dir=tmp_path,
        tokens_dir=tokens,
        state_db=tmp_path / "state.db",
        log_dir=tmp_path / "logs",
    )


def _write_classroom_token(path: Path, *, expires: int | float | None = None) -> None:
    cookies = [
        {
            "name": "SID",
            "value": "x",
            "domain": ".google.com",
            "path": "/",
            "expires": expires if expires is not None else -1,
            "httpOnly": True,
            "secure": True,
            "sameSite": "Lax",
        },
        {
            "name": "SAPISID",
            "value": "y",
            "domain": ".google.com",
            "path": "/",
            "expires": -1,
        },
    ]
    path.write_text(json.dumps({"cookies": cookies, "origins": []}))


def _write_edrolo_token(path: Path, *, expires: int | float = -1) -> None:
    cookies = [
        {
            "name": "sessionid",
            "value": "x",
            "domain": "app.edrolo.com",
            "path": "/",
            "expires": expires,
        }
    ]
    path.write_text(json.dumps({"cookies": cookies, "origins": []}))


def _write_eduperfect_token(path: Path, *, expires_at: datetime) -> None:
    path.write_text(
        json.dumps(
            {
                "access_token": "jwt.value.here",
                "expires_at": expires_at.isoformat(),
                "storage_state": {"cookies": [], "origins": []},
            }
        )
    )


def _write_compass_token(path: Path, *, captured_at: datetime) -> None:
    path.write_text(
        json.dumps(
            {
                "subdomain": "mcsc-vic",
                "cookie": "session-value",
                "captured_at": captured_at.isoformat(),
            }
        )
    )


def _child_cfg(*, classroom=True, compass=True, edrolo=False, eduperfect=False) -> ChildConfig:
    return ChildConfig(
        display_name="Test",
        sources=ChildSources(
            classroom=SimpleSourceConfig(enabled=classroom),
            compass=CompassConfig(enabled=compass, subdomain="mcsc-vic"),
            edrolo=SimpleSourceConfig(enabled=edrolo),
            eduperfect=SimpleSourceConfig(enabled=eduperfect),
        ),
        compass_user_id=10000,
    )


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


class TestBuildSourceAuthRows:
    def test_missing_tokens_yield_missing_status(self, tmp_path: Path) -> None:
        settings = _settings(tmp_path)
        state = StateStore(settings.state_db)
        rows = build_source_auth_rows(
            child="james",
            child_cfg=_child_cfg(classroom=True, compass=True),
            settings=settings,
            state=state,
            now=datetime(2026, 5, 1, tzinfo=UTC),
        )
        assert [r.source for r in rows] == ["classroom", "compass"]
        assert all(r.status == "missing" for r in rows)
        assert all(not r.token_present for r in rows)

    def test_classroom_token_expiry_read(self, tmp_path: Path) -> None:
        settings = _settings(tmp_path)
        state = StateStore(settings.state_db)
        # Token expires 1 hour from now → 'expiring' since <24h.
        now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        exp_ts = (now + timedelta(hours=1)).timestamp()
        _write_classroom_token(settings.child_token_path("james", "classroom"), expires=exp_ts)

        rows = build_source_auth_rows(
            child="james",
            child_cfg=_child_cfg(classroom=True, compass=False),
            settings=settings,
            state=state,
            now=now,
        )
        assert len(rows) == 1
        assert rows[0].source == "classroom"
        assert rows[0].token_present is True
        assert rows[0].token_expires_at is not None
        assert rows[0].status == "expiring"

    def test_classroom_token_expired(self, tmp_path: Path) -> None:
        settings = _settings(tmp_path)
        state = StateStore(settings.state_db)
        now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        exp_ts = (now - timedelta(hours=1)).timestamp()
        _write_classroom_token(settings.child_token_path("james", "classroom"), expires=exp_ts)

        rows = build_source_auth_rows(
            child="james",
            child_cfg=_child_cfg(classroom=True, compass=False),
            settings=settings,
            state=state,
            now=now,
        )
        assert rows[0].status == "expired"

    def test_classroom_session_cookie_treated_as_ok(self, tmp_path: Path) -> None:
        settings = _settings(tmp_path)
        state = StateStore(settings.state_db)
        # expires == -1 → session cookie, no proactive expiry signal.
        _write_classroom_token(settings.child_token_path("james", "classroom"), expires=-1)
        state.record_success("james", "classroom")
        rows = build_source_auth_rows(
            child="james",
            child_cfg=_child_cfg(classroom=True, compass=False),
            settings=settings,
            state=state,
            now=datetime.now(UTC),
        )
        assert rows[0].token_expires_at is None
        assert rows[0].status == "ok"

    def test_eduperfect_expired_jwt(self, tmp_path: Path) -> None:
        settings = _settings(tmp_path)
        state = StateStore(settings.state_db)
        now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        _write_eduperfect_token(
            settings.child_token_path("james", "eduperfect"),
            expires_at=now - timedelta(minutes=1),
        )
        rows = build_source_auth_rows(
            child="james",
            child_cfg=_child_cfg(classroom=False, compass=False, eduperfect=True),
            settings=settings,
            state=state,
            now=now,
        )
        assert rows[0].source == "eduperfect"
        assert rows[0].status == "expired"

    def test_compass_uses_shared_token_path(self, tmp_path: Path) -> None:
        settings = _settings(tmp_path)
        state = StateStore(settings.state_db)
        captured = datetime(2026, 4, 1, 9, 0, tzinfo=UTC)
        _write_compass_token(settings.tokens_dir / "compass-parent.json", captured_at=captured)
        state.record_success("james", "compass")

        rows = build_source_auth_rows(
            child="james",
            child_cfg=_child_cfg(classroom=False, compass=True),
            settings=settings,
            state=state,
            now=datetime(2026, 5, 1, tzinfo=UTC),
        )
        assert rows[0].source == "compass"
        assert rows[0].token_present is True
        # Compass returns captured_at via the expiry slot for display.
        assert rows[0].token_expires_at == captured.astimezone(UTC)
        assert rows[0].status == "ok"

    def test_edrolo_session_cookie(self, tmp_path: Path) -> None:
        settings = _settings(tmp_path)
        state = StateStore(settings.state_db)
        _write_edrolo_token(settings.child_token_path("tahlia", "edrolo"), expires=-1)
        state.record_success("tahlia", "edrolo")
        rows = build_source_auth_rows(
            child="tahlia",
            child_cfg=_child_cfg(classroom=False, compass=False, edrolo=True),
            settings=settings,
            state=state,
            now=datetime.now(UTC),
        )
        assert rows[0].source == "edrolo"
        assert rows[0].token_expires_at is None
        assert rows[0].status == "ok"

    def test_failure_after_success_marks_failing(self, tmp_path: Path) -> None:
        settings = _settings(tmp_path)
        state = StateStore(settings.state_db)
        _write_classroom_token(settings.child_token_path("james", "classroom"), expires=-1)
        state.record_success("james", "classroom", now=datetime(2026, 5, 1, tzinfo=UTC))
        state.record_failure(
            "james",
            "classroom",
            kind="auth_expired",
            message="redirected",
            now=datetime(2026, 5, 2, tzinfo=UTC),
        )
        rows = build_source_auth_rows(
            child="james",
            child_cfg=_child_cfg(classroom=True, compass=False),
            settings=settings,
            state=state,
            now=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert rows[0].status == "failing"

    def test_corrupt_token_does_not_raise(self, tmp_path: Path) -> None:
        settings = _settings(tmp_path)
        state = StateStore(settings.state_db)
        path = settings.child_token_path("james", "classroom")
        path.write_text("{not json")
        rows = build_source_auth_rows(
            child="james",
            child_cfg=_child_cfg(classroom=True, compass=False),
            settings=settings,
            state=state,
            now=datetime.now(UTC),
        )
        # File exists → token_present True, but expiry unreadable.
        assert rows[0].token_present is True
        assert rows[0].token_expires_at is None

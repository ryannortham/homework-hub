"""Tests for the Education Perfect source — mapper, token file, and status mapping."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from homework_hub.models import Source, Status
from homework_hub.sources.eduperfect import (
    EduPerfectSource,
    EduPerfectTokenFile,
    _activity_type_description,
    _decode_jwt_exp,
    _parse_ep_dt,
    map_ep_task_to_task,
)

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

FIXTURE = Path(__file__).parent / "fixtures" / "eduperfect_task.json"


@pytest.fixture()
def task() -> dict:
    return json.loads(FIXTURE.read_text())


def _token_file(
    *,
    access_token: str = "fake.jwt.token",
    expires_at: datetime | None = None,
    storage_state: dict | None = None,
) -> dict:
    if expires_at is None:
        expires_at = datetime.now(UTC) + timedelta(hours=1)
    return {
        "access_token": access_token,
        "expires_at": expires_at.isoformat(),
        "storage_state": storage_state or {"cookies": [], "origins": []},
    }


# --------------------------------------------------------------------------- #
# _parse_ep_dt
# --------------------------------------------------------------------------- #


class TestParseEpDt:
    def test_iso_z_suffix(self):
        dt = _parse_ep_dt("2026-05-05T23:59:00.000Z")
        assert dt == datetime(2026, 5, 5, 23, 59, 0, tzinfo=UTC)

    def test_iso_offset(self):
        dt = _parse_ep_dt("2026-05-05T09:00:00+10:00")
        assert dt == datetime(2026, 5, 4, 23, 0, 0, tzinfo=UTC)

    def test_none_returns_none(self):
        assert _parse_ep_dt(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_ep_dt("") is None

    def test_invalid_string_returns_none(self):
        assert _parse_ep_dt("not-a-date") is None


# --------------------------------------------------------------------------- #
# _activity_type_description
# --------------------------------------------------------------------------- #


class TestActivityTypeDescription:
    def test_lesson(self):
        assert _activity_type_description("LESSON") == "EP lesson"

    def test_quiz(self):
        assert _activity_type_description("QUIZ") == "EP quiz"

    def test_exam_revision(self):
        assert _activity_type_description("EXAM_REVISION") == "EP exam revision"

    def test_unknown_empty(self):
        assert _activity_type_description("UNKNOWN") == ""

    def test_case_insensitive(self):
        assert _activity_type_description("quiz") == "EP quiz"


# --------------------------------------------------------------------------- #
# map_ep_task_to_task
# --------------------------------------------------------------------------- #


class TestMapping:
    def test_basic_fields(self, task: dict):
        t = map_ep_task_to_task(
            child="james",
            assigned_work=task,
            class_names={"cc112233-4455-6677-8899-aabbccddeeff": "Year 9 English"},
        )
        assert t.source == Source.EDUPERFECT
        assert t.source_id == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        assert t.child == "james"
        assert t.title == "Chapter 5 Reading Quiz"
        assert t.subject == "Year 9 English"
        assert t.description == "EP quiz"
        assert t.status == Status.NOT_STARTED
        assert t.status_raw == "upcoming"
        assert t.url == "https://app.educationperfect.com/learning/tasks/a1b2c3d4-e5f6-7890-abcd-ef1234567890"

    def test_due_date_from_end_date(self, task: dict):
        t = map_ep_task_to_task(child="james", assigned_work=task)
        assert t.due_at == datetime(2026, 5, 5, 23, 59, 0, tzinfo=UTC)

    def test_assigned_at_from_start_date(self, task: dict):
        t = map_ep_task_to_task(child="james", assigned_work=task)
        assert t.assigned_at == datetime(2026, 4, 28, 9, 0, 0, tzinfo=UTC)

    def test_subject_fallback_when_class_not_resolved(self, task: dict):
        t = map_ep_task_to_task(child="james", assigned_work=task, class_names={})
        assert t.subject == "Education Perfect"

    def test_subject_fallback_when_no_class_ids(self, task: dict):
        task["assignedVia"]["classIds"] = []
        t = map_ep_task_to_task(child="james", assigned_work=task)
        assert t.subject == "Education Perfect"

    def test_subject_fallback_when_assigned_via_missing(self, task: dict):
        task["assignedVia"] = None
        t = map_ep_task_to_task(child="james", assigned_work=task)
        assert t.subject == "Education Perfect"

    def test_raises_on_missing_id(self, task: dict):
        del task["id"]
        from homework_hub.sources.base import SchemaBreakError
        with pytest.raises(SchemaBreakError):
            map_ep_task_to_task(child="james", assigned_work=task)

    def test_raises_on_missing_title(self, task: dict):
        del task["title"]
        from homework_hub.sources.base import SchemaBreakError
        with pytest.raises(SchemaBreakError):
            map_ep_task_to_task(child="james", assigned_work=task)

    def test_no_settings_gives_null_dates(self, task: dict):
        task["assignedWorkSettings"] = None
        t = map_ep_task_to_task(child="james", assigned_work=task)
        assert t.due_at is None
        assert t.assigned_at is None


# --------------------------------------------------------------------------- #
# Status mapping
# --------------------------------------------------------------------------- #


class TestStatusMapping:
    @pytest.mark.parametrize("raw,expected", [
        ("UPCOMING",    Status.NOT_STARTED),
        ("IN_PROGRESS", Status.IN_PROGRESS),
        ("PAST_DUE",    Status.OVERDUE),
        ("COMPLETED",   Status.SUBMITTED),
    ])
    def test_known_statuses(self, task: dict, raw: str, expected: Status):
        task["status"] = raw
        t = map_ep_task_to_task(child="james", assigned_work=task)
        assert t.status == expected
        assert t.status_raw == raw.lower()

    def test_unknown_status_falls_back_to_not_started(self, task: dict):
        task["status"] = "MYSTERY_STATUS"
        t = map_ep_task_to_task(child="james", assigned_work=task)
        assert t.status == Status.NOT_STARTED

    def test_null_status_falls_back_to_not_started(self, task: dict):
        task["status"] = None
        t = map_ep_task_to_task(child="james", assigned_work=task)
        assert t.status == Status.NOT_STARTED


# --------------------------------------------------------------------------- #
# EduPerfectTokenFile
# --------------------------------------------------------------------------- #


class TestEduPerfectTokenFile:
    def test_load_valid(self, tmp_path: Path):
        path = tmp_path / "james-eduperfect.json"
        path.write_text(json.dumps(_token_file()))
        tf = EduPerfectTokenFile.load(path)
        assert tf.access_token == "fake.jwt.token"
        assert not tf.is_expired()

    def test_load_missing_raises(self, tmp_path: Path):
        from homework_hub.sources.base import AuthExpiredError
        with pytest.raises(AuthExpiredError, match="run `homework-hub auth eduperfect"):
            EduPerfectTokenFile.load(tmp_path / "missing.json")

    def test_load_corrupt_raises(self, tmp_path: Path):
        from homework_hub.sources.base import AuthExpiredError
        path = tmp_path / "bad.json"
        path.write_text("not json{{{")
        with pytest.raises(AuthExpiredError):
            EduPerfectTokenFile.load(path)

    def test_load_missing_key_raises(self, tmp_path: Path):
        from homework_hub.sources.base import AuthExpiredError
        path = tmp_path / "incomplete.json"
        path.write_text(json.dumps({"access_token": "tok", "expires_at": "2026-01-01T00:00:00+00:00"}))
        with pytest.raises(AuthExpiredError, match="missing 'storage_state'"):
            EduPerfectTokenFile.load(path)

    def test_is_expired_true(self, tmp_path: Path):
        past = datetime.now(UTC) - timedelta(hours=1)
        path = tmp_path / "expired.json"
        path.write_text(json.dumps(_token_file(expires_at=past)))
        tf = EduPerfectTokenFile.load(path)
        assert tf.is_expired()

    def test_is_expired_within_buffer(self, tmp_path: Path):
        # 3 min from now — inside the 5 min buffer → considered expired.
        soon = datetime.now(UTC) + timedelta(minutes=3)
        path = tmp_path / "soon.json"
        path.write_text(json.dumps(_token_file(expires_at=soon)))
        tf = EduPerfectTokenFile.load(path)
        assert tf.is_expired()

    def test_is_expired_false(self, tmp_path: Path):
        future = datetime.now(UTC) + timedelta(hours=2)
        path = tmp_path / "fresh.json"
        path.write_text(json.dumps(_token_file(expires_at=future)))
        tf = EduPerfectTokenFile.load(path)
        assert not tf.is_expired()

    def test_save_round_trips(self, tmp_path: Path):
        path = tmp_path / "saved.json"
        raw = _token_file()
        tf = EduPerfectTokenFile(raw, path=path)
        tf.save(path)
        reloaded = EduPerfectTokenFile.load(path)
        assert reloaded.access_token == raw["access_token"]


# --------------------------------------------------------------------------- #
# _decode_jwt_exp
# --------------------------------------------------------------------------- #


class TestDecodeJwtExp:
    def _make_jwt(self, exp: int) -> str:
        import base64
        import json as _json
        header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
        payload = base64.urlsafe_b64encode(
            _json.dumps({"exp": exp, "sub": "test"}).encode()
        ).rstrip(b"=").decode()
        return f"{header}.{payload}.fakesig"

    def test_valid_jwt(self):
        exp = int((datetime(2026, 6, 1, 12, 0, tzinfo=UTC)).timestamp())
        token = self._make_jwt(exp)
        result = _decode_jwt_exp(token)
        assert result == datetime(2026, 6, 1, 12, 0, tzinfo=UTC)

    def test_invalid_jwt_falls_back_to_one_hour(self):
        before = datetime.now(UTC)
        result = _decode_jwt_exp("not.a.valid.jwt.at.all")
        after = datetime.now(UTC)
        # Should be approximately now + 1 hour.
        assert timedelta(minutes=59) < result - before < timedelta(minutes=61)


# --------------------------------------------------------------------------- #
# EduPerfectSource.fetch_raw — integration with fake client
# --------------------------------------------------------------------------- #


class FakeEpClient:
    def __init__(
        self,
        token: str,
        *,
        assigned_work: list | None = None,
        class_names_map: dict | None = None,
    ):
        self._work = assigned_work or []
        self._class_names_map = class_names_map or {}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def get_assigned_work(self, **_):
        return self._work

    def get_class_names(self, ids):
        return {i: self._class_names_map.get(i, f"Class {i}") for i in ids}


class TestEduPerfectSourceFetchRaw:
    def _source(
        self,
        tmp_path: Path,
        *,
        work: list | None = None,
        expired: bool = False,
    ) -> EduPerfectSource:
        expires_at = (
            datetime.now(UTC) - timedelta(hours=1)
            if expired
            else datetime.now(UTC) + timedelta(hours=1)
        )
        token_path = tmp_path / "james-eduperfect.json"
        token_path.write_text(json.dumps(_token_file(expires_at=expires_at)))

        def fake_client_factory(token):
            return FakeEpClient(token, assigned_work=work or [])

        def fake_refresh(tf):
            # Return a fresh token file (no Playwright in tests).
            new_raw = _token_file(expires_at=datetime.now(UTC) + timedelta(hours=1))
            return EduPerfectTokenFile(new_raw, path=tf.path)

        return EduPerfectSource(
            {"james": token_path},
            client_factory=fake_client_factory,
            refresh_fn=fake_refresh,
        )

    def test_returns_raw_records(self, tmp_path: Path, task: dict):
        source = self._source(tmp_path, work=[task])
        records = source.fetch_raw("james")
        assert len(records) == 1
        assert records[0].source == "eduperfect"
        assert records[0].source_id == task["id"]
        assert records[0].child == "james"
        assert "assigned_work" in records[0].payload

    def test_token_refresh_called_when_expired(self, tmp_path: Path, task: dict):
        refreshed = []

        expires_at = datetime.now(UTC) - timedelta(hours=1)
        token_path = tmp_path / "james-eduperfect.json"
        token_path.write_text(json.dumps(_token_file(expires_at=expires_at)))

        def fake_client_factory(token):
            return FakeEpClient(token, assigned_work=[task])

        def fake_refresh(tf):
            refreshed.append(True)
            new_raw = _token_file(expires_at=datetime.now(UTC) + timedelta(hours=1))
            return EduPerfectTokenFile(new_raw, path=tf.path)

        source = EduPerfectSource(
            {"james": token_path},
            client_factory=fake_client_factory,
            refresh_fn=fake_refresh,
        )
        source.fetch_raw("james")
        assert refreshed, "refresh should have been called for expired token"

    def test_no_refresh_when_token_fresh(self, tmp_path: Path, task: dict):
        refreshed = []
        token_path = tmp_path / "james-eduperfect.json"
        token_path.write_text(
            json.dumps(_token_file(expires_at=datetime.now(UTC) + timedelta(hours=2)))
        )

        def fake_refresh(tf):
            refreshed.append(True)
            return tf

        source = EduPerfectSource(
            {"james": token_path},
            client_factory=lambda tok: FakeEpClient(tok, assigned_work=[task]),
            refresh_fn=fake_refresh,
        )
        source.fetch_raw("james")
        assert not refreshed, "refresh should NOT be called for a fresh token"

    def test_missing_token_file_raises_auth_expired(self, tmp_path: Path):
        from homework_hub.sources.base import AuthExpiredError
        source = EduPerfectSource({"james": tmp_path / "missing.json"})
        with pytest.raises(AuthExpiredError):
            source.fetch_raw("james")

    def test_unknown_child_raises_schema_break(self, tmp_path: Path):
        from homework_hub.sources.base import SchemaBreakError
        token_path = tmp_path / "james-eduperfect.json"
        token_path.write_text(json.dumps(_token_file()))
        source = EduPerfectSource({"james": token_path})
        with pytest.raises(SchemaBreakError):
            source.fetch_raw("tahlia")

    def test_empty_work_list(self, tmp_path: Path):
        source = self._source(tmp_path, work=[])
        records = source.fetch_raw("james")
        assert records == []

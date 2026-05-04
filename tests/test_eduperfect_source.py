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
    _decode_jwt_exp,
    _parse_ep_dt,
    map_ep_classwork_to_task,
)

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

FIXTURE = Path(__file__).parent / "fixtures" / "eduperfect_task.json"


@pytest.fixture()
def classwork() -> dict:
    return json.loads(FIXTURE.read_text())


def _token_raw(
    *,
    access_token: str = "fake.jwt.token",
    expires_at: datetime | None = None,
    school_id: str | None = None,
) -> dict:
    if expires_at is None:
        expires_at = datetime.now(UTC) + timedelta(hours=1)
    raw: dict = {
        "access_token": access_token,
        "expires_at": expires_at.isoformat(),
        "storage_state": {"cookies": [], "origins": []},
    }
    if school_id:
        raw["school_id"] = school_id
    return raw


# --------------------------------------------------------------------------- #
# _parse_ep_dt
# --------------------------------------------------------------------------- #


class TestParseEpDt:
    def test_iso_z_suffix(self):
        dt = _parse_ep_dt("2026-05-14T23:59:59.000Z")
        assert dt == datetime(2026, 5, 14, 23, 59, 59, tzinfo=UTC)

    def test_iso_offset(self):
        dt = _parse_ep_dt("2026-05-14T09:00:00+10:00")
        assert dt == datetime(2026, 5, 13, 23, 0, 0, tzinfo=UTC)

    def test_none_returns_none(self):
        assert _parse_ep_dt(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_ep_dt("") is None

    def test_invalid_returns_none(self):
        assert _parse_ep_dt("not-a-date") is None


# --------------------------------------------------------------------------- #
# map_ep_classwork_to_task
# --------------------------------------------------------------------------- #


class TestMapping:
    def test_basic_fields(self, classwork: dict):
        t = map_ep_classwork_to_task(child="james", classwork=classwork)
        assert t.source == Source.EDUPERFECT
        assert t.source_id == "12345"
        assert t.child == "james"
        assert t.title == "Chapter 1 おいたち Kanji Practice"
        assert t.subject == "9年生 日本語 Capp Sensei (3)"
        assert t.description == "EP teacher-assigned task"
        assert t.status == Status.NOT_STARTED
        assert t.status_raw == "not_started"
        assert t.url == "https://app.educationperfect.com/learning/tasks/12345"

    def test_due_date(self, classwork: dict):
        t = map_ep_classwork_to_task(child="james", classwork=classwork)
        assert t.due_at == datetime(2026, 5, 14, 23, 59, 59, tzinfo=UTC)

    def test_assigned_at(self, classwork: dict):
        t = map_ep_classwork_to_task(child="james", classwork=classwork)
        assert t.assigned_at == datetime(2026, 5, 3, 0, 0, 0, tzinfo=UTC)

    def test_subject_fallback_no_classes(self, classwork: dict):
        classwork["classes"] = []
        t = map_ep_classwork_to_task(child="james", classwork=classwork)
        assert t.subject == "Education Perfect"

    def test_subject_fallback_null_classes(self, classwork: dict):
        classwork["classes"] = None
        t = map_ep_classwork_to_task(child="james", classwork=classwork)
        assert t.subject == "Education Perfect"

    def test_system_recommendation_description(self, classwork: dict):
        classwork["source"] = "SYSTEM_RECOMMENDATION"
        t = map_ep_classwork_to_task(child="james", classwork=classwork)
        assert t.description == "EP recommended task"

    def test_raises_on_missing_id(self, classwork: dict):
        del classwork["id"]
        from homework_hub.sources.base import SchemaBreakError
        with pytest.raises(SchemaBreakError):
            map_ep_classwork_to_task(child="james", classwork=classwork)

    def test_raises_on_missing_name(self, classwork: dict):
        del classwork["name"]
        from homework_hub.sources.base import SchemaBreakError
        with pytest.raises(SchemaBreakError):
            map_ep_classwork_to_task(child="james", classwork=classwork)


# --------------------------------------------------------------------------- #
# Status mapping
# --------------------------------------------------------------------------- #


class TestStatusMapping:
    @pytest.mark.parametrize("raw,expected", [
        ("NOT_STARTED", Status.NOT_STARTED),
        ("IN_PROGRESS",  Status.IN_PROGRESS),
        ("COMPLETE",     Status.SUBMITTED),
    ])
    def test_known_statuses(self, classwork: dict, raw: str, expected: Status):
        classwork["progressStatus"] = raw
        t = map_ep_classwork_to_task(child="james", classwork=classwork)
        assert t.status == expected
        assert t.status_raw == raw.lower()

    def test_unknown_falls_back_to_not_started(self, classwork: dict):
        classwork["progressStatus"] = "MYSTERY"
        t = map_ep_classwork_to_task(child="james", classwork=classwork)
        assert t.status == Status.NOT_STARTED

    def test_null_status_falls_back(self, classwork: dict):
        classwork["progressStatus"] = None
        t = map_ep_classwork_to_task(child="james", classwork=classwork)
        assert t.status == Status.NOT_STARTED


# --------------------------------------------------------------------------- #
# EduPerfectTokenFile
# --------------------------------------------------------------------------- #


class TestEduPerfectTokenFile:
    def test_load_valid(self, tmp_path: Path):
        path = tmp_path / "james-eduperfect.json"
        path.write_text(json.dumps(_token_raw()))
        tf = EduPerfectTokenFile.load(path)
        assert tf.access_token == "fake.jwt.token"
        assert not tf.is_expired()

    def test_load_missing_raises(self, tmp_path: Path):
        from homework_hub.sources.base import AuthExpiredError
        with pytest.raises(AuthExpiredError, match="run `homework-hub auth eduperfect"):
            EduPerfectTokenFile.load(tmp_path / "missing.json")

    def test_load_corrupt_raises(self, tmp_path: Path):
        from homework_hub.sources.base import AuthExpiredError
        p = tmp_path / "bad.json"
        p.write_text("not json{{")
        with pytest.raises(AuthExpiredError):
            EduPerfectTokenFile.load(p)

    def test_load_missing_key_raises(self, tmp_path: Path):
        from homework_hub.sources.base import AuthExpiredError
        p = tmp_path / "incomplete.json"
        p.write_text(json.dumps({"access_token": "tok"}))
        with pytest.raises(AuthExpiredError, match="missing 'expires_at'"):
            EduPerfectTokenFile.load(p)

    def test_is_expired_true(self, tmp_path: Path):
        p = tmp_path / "expired.json"
        p.write_text(json.dumps(_token_raw(expires_at=datetime.now(UTC) - timedelta(hours=1))))
        tf = EduPerfectTokenFile.load(p)
        assert tf.is_expired()

    def test_is_expired_within_buffer(self, tmp_path: Path):
        p = tmp_path / "soon.json"
        p.write_text(json.dumps(_token_raw(expires_at=datetime.now(UTC) + timedelta(minutes=3))))
        tf = EduPerfectTokenFile.load(p)
        assert tf.is_expired()

    def test_is_expired_false(self, tmp_path: Path):
        p = tmp_path / "fresh.json"
        p.write_text(json.dumps(_token_raw(expires_at=datetime.now(UTC) + timedelta(hours=2))))
        tf = EduPerfectTokenFile.load(p)
        assert not tf.is_expired()

    def test_school_id_cached_on_save(self, tmp_path: Path):
        p = tmp_path / "james.json"
        p.write_text(json.dumps(_token_raw()))
        tf = EduPerfectTokenFile.load(p)
        assert tf.school_id is None
        tf2 = tf.with_school_id("school-uuid-123")
        assert tf2.school_id == "school-uuid-123"
        reloaded = EduPerfectTokenFile.load(p)
        assert reloaded.school_id == "school-uuid-123"

    def test_save_round_trips(self, tmp_path: Path):
        p = tmp_path / "saved.json"
        raw = _token_raw()
        tf = EduPerfectTokenFile(raw, path=p)
        tf.save(p)
        reloaded = EduPerfectTokenFile.load(p)
        assert reloaded.access_token == raw["access_token"]


# --------------------------------------------------------------------------- #
# _decode_jwt_exp
# --------------------------------------------------------------------------- #


class TestDecodeJwtExp:
    def _make_jwt(self, exp: int) -> str:
        import base64
        header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
        payload = base64.urlsafe_b64encode(
            json.dumps({"exp": exp, "sub": "test"}).encode()
        ).rstrip(b"=").decode()
        return f"{header}.{payload}.fakesig"

    def test_valid_jwt(self):
        exp = int(datetime(2026, 6, 1, 12, 0, tzinfo=UTC).timestamp())
        token = self._make_jwt(exp)
        result = _decode_jwt_exp(token)
        assert result == datetime(2026, 6, 1, 12, 0, tzinfo=UTC)

    def test_invalid_falls_back_to_one_hour(self):
        before = datetime.now(UTC)
        result = _decode_jwt_exp("not.a.valid.jwt")
        assert timedelta(minutes=59) < result - before < timedelta(minutes=61)


# --------------------------------------------------------------------------- #
# EduPerfectSource.fetch_raw — integration with fake client
# --------------------------------------------------------------------------- #


class FakeEpClient:
    def __init__(
        self,
        token: str,
        *,
        school_id: str = "school-uuid",
        classwork: list | None = None,
    ):
        self._school_id = school_id
        self._classwork = classwork or []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def get_school_id(self, user_id: str) -> str:
        return self._school_id

    def get_assigned_classwork(self, school_id: str) -> list:
        return self._classwork


def _valid_jwt() -> str:
    """Build a minimal valid JWT with exp 1 hour from now."""
    import base64
    exp = int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
    sub = "8f58aa43-d517-4f0b-87a6-5bcbc515db85"
    header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(
        json.dumps({"exp": exp, "sub": sub, "userId": sub}).encode()
    ).rstrip(b"=").decode()
    return f"{header}.{payload}.fakesig"


class TestEduPerfectSourceFetchRaw:
    def _source(
        self,
        tmp_path: Path,
        *,
        classwork: list | None = None,
        expired: bool = False,
        school_id: str | None = None,
    ) -> EduPerfectSource:
        expires_at = (
            datetime.now(UTC) - timedelta(hours=1)
            if expired
            else datetime.now(UTC) + timedelta(hours=1)
        )
        token_path = tmp_path / "james-eduperfect.json"
        raw = _token_raw(access_token=_valid_jwt(), expires_at=expires_at)
        if school_id:
            raw["school_id"] = school_id
        token_path.write_text(json.dumps(raw))

        def fake_client_factory(token):
            return FakeEpClient(token, school_id="school-uuid", classwork=classwork or [])

        return EduPerfectSource({"james": token_path}, client_factory=fake_client_factory)

    def test_returns_raw_records(self, tmp_path: Path, classwork: dict):
        source = self._source(tmp_path, classwork=[classwork])
        records = source.fetch_raw("james")
        assert len(records) == 1
        assert records[0].source == "eduperfect"
        assert records[0].source_id == "12345"
        assert records[0].child == "james"
        assert "classwork" in records[0].payload

    def test_expired_token_raises_auth_expired(self, tmp_path: Path):
        from homework_hub.sources.base import AuthExpiredError
        source = self._source(tmp_path, expired=True)
        with pytest.raises(AuthExpiredError, match="token expired"):
            source.fetch_raw("james")

    def test_missing_token_file_raises_auth_expired(self, tmp_path: Path):
        from homework_hub.sources.base import AuthExpiredError
        source = EduPerfectSource({"james": tmp_path / "missing.json"})
        with pytest.raises(AuthExpiredError):
            source.fetch_raw("james")

    def test_unknown_child_raises_schema_break(self, tmp_path: Path):
        from homework_hub.sources.base import SchemaBreakError
        token_path = tmp_path / "james-eduperfect.json"
        token_path.write_text(json.dumps(_token_raw(access_token=_valid_jwt())))
        source = EduPerfectSource({"james": token_path})
        with pytest.raises(SchemaBreakError):
            source.fetch_raw("tahlia")

    def test_empty_classwork_returns_empty(self, tmp_path: Path):
        source = self._source(tmp_path, classwork=[])
        records = source.fetch_raw("james")
        assert records == []

    def test_school_id_resolved_and_cached(self, tmp_path: Path, classwork: dict):
        token_path = tmp_path / "james-eduperfect.json"
        token_path.write_text(json.dumps(_token_raw(access_token=_valid_jwt())))

        def fake_factory(token):
            return FakeEpClient(token, school_id="resolved-school", classwork=[classwork])

        source = EduPerfectSource({"james": token_path}, client_factory=fake_factory)
        source.fetch_raw("james")

        # School ID should now be persisted in the token file
        reloaded = EduPerfectTokenFile.load(token_path)
        assert reloaded.school_id == "resolved-school"

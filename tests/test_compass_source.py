"""Tests for the Compass source — pure mapping + token store + client behaviour.

Live HTTP is mocked via httpx.MockTransport so the client's full code path
(headers, cookies, error mapping) is exercised without hitting the network.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status
from homework_hub.sources.base import (
    AuthExpiredError,
    SchemaBreakError,
    TransientError,
)
from homework_hub.sources.compass import (
    CompassClient,
    CompassSource,
    CompassToken,
    _parse_compass_dt,
    _strip_html,
    map_learning_task_to_task,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def lt():
    return _load("compass_learning_task.json")


# --------------------------------------------------------------------------- #
# Pure mapping
# --------------------------------------------------------------------------- #


class TestMapping:
    def test_basic_mapping(self, lt):
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.source is SourceEnum.COMPASS
        assert t.source_id == "8842"
        assert t.child == "james"
        assert t.subject == "9MATH"
        assert t.title == "Pythagoras Investigation"
        # HTML stripped from description
        assert "<p>" not in t.description
        assert "Pythagorean triples" in t.description
        assert t.status is Status.NOT_STARTED

    def test_url_built_from_subdomain_and_id(self, lt):
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.url == (
            "https://mcsc-vic.compass.education/Communicate/"
            "LearningTasksStudentDetails.aspx?taskId=8842"
        )

    def test_due_date_parsed(self, lt):
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.due_at == datetime(2026, 4, 29, 15, 0, 0, tzinfo=UTC)

    def test_assigned_at_parsed(self, lt):
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.assigned_at == datetime(2026, 4, 15, 0, 0, 0, tzinfo=UTC)

    def test_status_submitted(self, lt):
        lt["students"] = [{"userId": 1, "submissionStatus": 1}]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.status is Status.SUBMITTED

    def test_status_submitted_late(self, lt):
        lt["students"] = [{"userId": 1, "submissionStatus": 2}]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.status is Status.SUBMITTED

    def test_status_graded(self, lt):
        lt["students"] = [{"userId": 1, "submissionStatus": 3}]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.status is Status.GRADED

    def test_status_4_inactive_enrolment_treated_as_submitted(self, lt):
        # submissionStatus=4 observed on inactive enrolments with a populated
        # submittedTimestamp; treated as submitted-but-ungraded.
        lt["students"] = [{"userId": 1, "submissionStatus": 4}]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.status is Status.SUBMITTED

    def test_legacy_status_field_name_no_longer_honoured(self, lt):
        # The defensive ``status`` fallback was dropped in the medallion
        # tail commit — only ``submissionStatus`` is read now. If Compass
        # ever flattens the schema this needs revisiting; treat absence as
        # NOT_STARTED rather than silently misreporting.
        lt["students"] = [{"userId": 1, "status": 1}]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.status is Status.NOT_STARTED

    def test_per_student_status_used_when_top_level_absent(self, lt):
        lt.pop("status", None)
        lt["students"] = [{"userId": 1, "submissionStatus": 0}]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.status is Status.NOT_STARTED

    def test_unknown_status_falls_back_to_not_started(self, lt):
        lt["students"] = [{"userId": 1, "submissionStatus": 999}]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.status is Status.NOT_STARTED
        assert t.status_raw == "999"

    def test_missing_id_raises(self, lt):
        del lt["id"]
        with pytest.raises(SchemaBreakError):
            map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")

    def test_missing_name_raises(self, lt):
        del lt["name"]
        with pytest.raises(SchemaBreakError):
            map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")

    def test_dotnet_date_parsed(self):
        result = _parse_compass_dt("/Date(1714521600000)/")
        # 1714521600000 ms = 2024-05-01 00:00:00 UTC
        assert result == datetime(2024, 5, 1, 0, 0, 0, tzinfo=UTC)

    def test_iso_with_z_parsed(self):
        result = _parse_compass_dt("2026-04-29T15:00:00Z")
        assert result == datetime(2026, 4, 29, 15, 0, 0, tzinfo=UTC)

    def test_iso_naive_assumed_utc(self):
        result = _parse_compass_dt("2026-04-29T15:00:00")
        assert result == datetime(2026, 4, 29, 15, 0, 0, tzinfo=UTC)

    def test_epoch_ms_int_parsed(self):
        result = _parse_compass_dt(1714521600000)
        assert result == datetime(2024, 5, 1, 0, 0, 0, tzinfo=UTC)

    def test_garbage_returns_none(self):
        assert _parse_compass_dt("not a date") is None
        assert _parse_compass_dt(None) is None
        assert _parse_compass_dt("") is None

    def test_strip_html(self):
        assert _strip_html("<p>Hello <b>world</b></p>") == "Hello world"
        assert _strip_html("plain text") == "plain text"
        assert _strip_html("") == ""

    def test_submitted_at_populated_from_submitted_timestamp(self, lt):
        lt["students"] = [
            {"userId": 1, "submissionStatus": 1, "submittedTimestamp": "2026-04-20T09:00:00Z"}
        ]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.submitted_at == datetime(2026, 4, 20, 9, 0, 0, tzinfo=UTC)

    def test_submitted_at_none_when_timestamp_null(self, lt):
        # Fixture has submittedTimestamp: null
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.submitted_at is None

    def test_due_at_falls_back_to_submitted_at_when_no_due_date(self, lt):
        del lt["dueDateTimestamp"]
        lt["students"] = [
            {"userId": 1, "submissionStatus": 1, "submittedTimestamp": "2026-04-20T09:00:00Z"}
        ]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        assert t.due_at == datetime(2026, 4, 20, 9, 0, 0, tzinfo=UTC)
        assert t.submitted_at == datetime(2026, 4, 20, 9, 0, 0, tzinfo=UTC)

    def test_explicit_due_at_takes_precedence_over_submitted_at(self, lt):
        lt["students"] = [
            {"userId": 1, "submissionStatus": 1, "submittedTimestamp": "2026-04-20T09:00:00Z"}
        ]
        t = map_learning_task_to_task(child="james", learning_task=lt, subdomain="mcsc-vic")
        # dueDateTimestamp from fixture is 2026-04-29T15:00:00 — must win
        assert t.due_at == datetime(2026, 4, 29, 15, 0, 0, tzinfo=UTC)
        assert t.submitted_at == datetime(2026, 4, 20, 9, 0, 0, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Token persistence
# --------------------------------------------------------------------------- #


class TestCompassToken:
    def test_round_trip(self, tmp_path: Path):
        path = tmp_path / "t.json"
        original = CompassToken(subdomain="mcsc-vic", cookie="ABC123")
        original.save(path)
        loaded = CompassToken.load(path)
        assert loaded.cookie == "ABC123"
        assert loaded.subdomain == "mcsc-vic"
        assert loaded.captured_at is not None

    def test_load_missing_raises_auth_expired(self, tmp_path: Path):
        with pytest.raises(AuthExpiredError):
            CompassToken.load(tmp_path / "nope.json")

    def test_save_creates_parent_dir(self, tmp_path: Path):
        path = tmp_path / "nested" / "deeper" / "t.json"
        CompassToken(subdomain="x", cookie="y").save(path)
        assert path.exists()


# --------------------------------------------------------------------------- #
# CompassClient — uses httpx.MockTransport for full code-path coverage
# --------------------------------------------------------------------------- #


class TestCompassClient:
    @staticmethod
    def _client(handler) -> CompassClient:
        transport = httpx.MockTransport(handler)
        http_client = httpx.Client(transport=transport, follow_redirects=False)
        token = CompassToken(subdomain="mcsc-vic", cookie="SESSION")
        return CompassClient(token, client=http_client)

    def test_get_learning_tasks_happy_path(self):
        captured: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["headers"] = dict(request.headers)
            captured["cookies"] = request.headers.get("cookie", "")
            captured["body"] = json.loads(request.content)
            return httpx.Response(200, json=_load("compass_response.json"))

        client = self._client(handler)
        result = client.get_learning_tasks(12345)

        assert "Services/LearningTasks.svc/GetAllLearningTasksByUserId" in captured["url"]
        assert "ASP.NET_SessionId=SESSION" in captured["cookies"]
        assert captured["body"]["userId"] == 12345
        assert "Compass" in captured["headers"]["user-agent"]
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["id"] == 8842

    def test_302_translates_to_auth_expired(self):
        def handler(_request):
            return httpx.Response(302, headers={"Location": "/Login.aspx"})

        client = self._client(handler)
        with pytest.raises(AuthExpiredError):
            client.get_learning_tasks(1)

    def test_401_translates_to_auth_expired(self):
        client = self._client(lambda _r: httpx.Response(401))
        with pytest.raises(AuthExpiredError):
            client.get_learning_tasks(1)

    def test_500_translates_to_transient(self):
        client = self._client(lambda _r: httpx.Response(500, text="boom"))
        with pytest.raises(TransientError):
            client.get_learning_tasks(1)

    def test_400_translates_to_schema_break(self):
        client = self._client(lambda _r: httpx.Response(400, text="bad"))
        with pytest.raises(SchemaBreakError):
            client.get_learning_tasks(1)

    def test_non_json_body_translates_to_schema_break(self):
        client = self._client(lambda _r: httpx.Response(200, text="<html>error</html>"))
        with pytest.raises(SchemaBreakError):
            client.get_learning_tasks(1)

    def test_unwraps_d_data_envelope(self):
        client = self._client(
            lambda _r: httpx.Response(
                200, json={"d": {"data": [{"id": 1, "name": "x"}], "h": "ok"}}
            )
        )
        result = client.get_learning_tasks(1)
        assert result == [{"id": 1, "name": "x"}]

    def test_handles_bare_d_list(self):
        client = self._client(lambda _r: httpx.Response(200, json={"d": [{"id": 1, "name": "x"}]}))
        result = client.get_learning_tasks(1)
        assert result == [{"id": 1, "name": "x"}]


# --------------------------------------------------------------------------- #
# CompassSource — orchestrates token + client + mapping
# --------------------------------------------------------------------------- #


class FakeCompassClient:
    def __init__(self, raw_tasks: list[dict]):
        self.raw_tasks = raw_tasks
        self.calls: list[int] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        pass

    def get_learning_tasks(self, user_id: int) -> list[dict]:
        self.calls.append(user_id)
        return self.raw_tasks


class TestCompassSource:
    def test_fetch_uses_per_child_user_id(self, tmp_path: Path, lt):
        token = CompassToken(subdomain="mcsc-vic", cookie="ABC")
        token_path = tmp_path / "compass.json"
        token.save(token_path)

        fake = FakeCompassClient([lt])
        source = CompassSource(
            token_path,
            user_id_for_child={"james": 12345, "tahlia": 67890},
            client_factory=lambda _t: fake,
        )
        tasks = source.fetch("james")

        assert fake.calls == [12345]
        assert len(tasks) == 1
        assert tasks[0].child == "james"
        assert tasks[0].source_id == "8842"

    def test_fetch_for_other_child_passes_their_user_id(self, tmp_path: Path, lt):
        token = CompassToken(subdomain="mcsc-vic", cookie="ABC")
        token_path = tmp_path / "compass.json"
        token.save(token_path)

        fake = FakeCompassClient([lt])
        source = CompassSource(
            token_path,
            user_id_for_child={"james": 12345, "tahlia": 67890},
            client_factory=lambda _t: fake,
        )
        source.fetch("tahlia")
        assert fake.calls == [67890]

    def test_unknown_child_raises_schema_break(self, tmp_path: Path):
        token = CompassToken(subdomain="mcsc-vic", cookie="ABC")
        token_path = tmp_path / "compass.json"
        token.save(token_path)
        source = CompassSource(
            token_path,
            user_id_for_child={"james": 1},
            client_factory=lambda _t: FakeCompassClient([]),
        )
        with pytest.raises(SchemaBreakError, match="compass_user_id"):
            source.fetch("nobody")

    def test_missing_token_raises_auth_expired(self, tmp_path: Path):
        source = CompassSource(
            tmp_path / "missing.json",
            user_id_for_child={"james": 1},
            client_factory=lambda _t: FakeCompassClient([]),
        )
        with pytest.raises(AuthExpiredError):
            source.fetch("james")


class TestCompassFetchRaw:
    """fetch_raw — raw payloads for the bronze layer (M2)."""

    def test_returns_one_record_per_learning_task(self, tmp_path: Path, lt):
        token = CompassToken(subdomain="mcsc-vic", cookie="ABC")
        token_path = tmp_path / "compass.json"
        token.save(token_path)
        fake = FakeCompassClient([lt, {**lt, "id": 9999}])
        source = CompassSource(
            token_path,
            user_id_for_child={"james": 12345},
            client_factory=lambda _t: fake,
        )
        records = source.fetch_raw("james")
        assert len(records) == 2
        assert {r.source_id for r in records} == {"8842", "9999"}
        assert all(r.source == "compass" for r in records)
        assert all(r.child == "james" for r in records)

    def test_payload_carries_subdomain_and_full_lt(self, tmp_path: Path, lt):
        token = CompassToken(subdomain="mcsc-vic", cookie="ABC")
        token_path = tmp_path / "compass.json"
        token.save(token_path)
        source = CompassSource(
            token_path,
            user_id_for_child={"james": 1},
            client_factory=lambda _t: FakeCompassClient([lt]),
        )
        rec = source.fetch_raw("james")[0]
        assert rec.payload["subdomain"] == "mcsc-vic"
        assert rec.payload["learning_task"] == lt

    def test_unknown_child_raises_schema_break(self, tmp_path: Path):
        token = CompassToken(subdomain="mcsc-vic", cookie="ABC")
        token_path = tmp_path / "compass.json"
        token.save(token_path)
        source = CompassSource(
            token_path,
            user_id_for_child={"james": 1},
            client_factory=lambda _t: FakeCompassClient([]),
        )
        with pytest.raises(SchemaBreakError, match="compass_user_id"):
            source.fetch_raw("nobody")

    def test_missing_id_raises_schema_break(self, tmp_path: Path):
        token = CompassToken(subdomain="mcsc-vic", cookie="ABC")
        token_path = tmp_path / "compass.json"
        token.save(token_path)
        source = CompassSource(
            token_path,
            user_id_for_child={"james": 1},
            client_factory=lambda _t: FakeCompassClient([{"name": "x"}]),
        )
        with pytest.raises(SchemaBreakError, match="missing id"):
            source.fetch_raw("james")

"""Tests for the Edrolo source — pure mapping + storage state + client + source."""

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
from homework_hub.sources.edrolo import (
    EdroloClient,
    EdroloSource,
    EdroloStorageState,
    _extract_tasks_payload,
    _parse_dt,
    map_edrolo_task_to_task,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def task():
    return _load("edrolo_task.json")


@pytest.fixture
def storage_raw():
    return _load("edrolo_storage_state.json")


# --------------------------------------------------------------------------- #
# Pure mapping
# --------------------------------------------------------------------------- #


class TestMapping:
    def test_basic_mapping(self, task):
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.source is SourceEnum.EDROLO
        assert t.source_id == "99821"
        assert t.child == "james"
        assert t.subject == "Mathematical Methods 3/4"
        assert t.title == "Chapter 4 — Calculus Practice"
        assert t.status is Status.NOT_STARTED
        assert t.url == "https://edrolo.com.au/student/tasks/99821/"

    def test_due_date_parsed(self, task):
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.due_at == datetime(2026, 5, 2, 13, 0, 0, tzinfo=UTC)

    def test_assigned_at_parsed(self, task):
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.assigned_at == datetime(2026, 4, 20, 8, 0, 0, tzinfo=UTC)

    def test_status_submitted_via_string(self, task):
        task["status"] = "submitted"
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.status is Status.SUBMITTED

    def test_status_graded_via_string(self, task):
        task["status"] = "graded"
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.status is Status.GRADED

    def test_status_inferred_from_submitted_at(self, task):
        del task["status"]
        task["submitted_at"] = "2026-04-30T10:00:00Z"
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.status is Status.SUBMITTED
        assert t.status_raw == "submitted"

    def test_status_inferred_from_graded_at(self, task):
        del task["status"]
        task["graded_at"] = "2026-04-30T10:00:00Z"
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.status is Status.GRADED

    def test_status_inferred_from_started_at(self, task):
        del task["status"]
        task["started_at"] = "2026-04-22T10:00:00Z"
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.status is Status.IN_PROGRESS

    def test_unknown_status_falls_back_to_not_started(self, task):
        task["status"] = "wibble"
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.status is Status.NOT_STARTED
        assert t.status_raw == "wibble"

    def test_default_url_when_missing(self, task):
        del task["url"]
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.url == "https://edrolo.com.au/student/tasks/99821/"

    def test_alternate_subject_field(self, task):
        del task["course_name"]
        task["subject"] = "Physics"
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.subject == "Physics"

    def test_nested_course_object(self, task):
        del task["course_name"]
        task["course"] = {"id": 5, "name": "Chemistry"}
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.subject == "Chemistry"

    def test_alternate_title_field(self, task):
        del task["title"]
        task["name"] = "Quiz 5"
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.title == "Quiz 5"

    def test_uuid_id_accepted(self, task):
        del task["id"]
        task["uuid"] = "abc-123-def"
        t = map_edrolo_task_to_task(child="james", edrolo_task=task)
        assert t.source_id == "abc-123-def"

    def test_missing_id_raises(self, task):
        del task["id"]
        with pytest.raises(SchemaBreakError):
            map_edrolo_task_to_task(child="james", edrolo_task=task)

    def test_missing_title_raises(self, task):
        del task["title"]
        with pytest.raises(SchemaBreakError):
            map_edrolo_task_to_task(child="james", edrolo_task=task)

    def test_iso_z_parsed(self):
        result = _parse_dt("2026-05-02T13:00:00Z")
        assert result == datetime(2026, 5, 2, 13, 0, 0, tzinfo=UTC)

    def test_iso_naive_assumed_utc(self):
        result = _parse_dt("2026-05-02T13:00:00")
        assert result == datetime(2026, 5, 2, 13, 0, 0, tzinfo=UTC)

    def test_garbage_returns_none(self):
        assert _parse_dt("not a date") is None
        assert _parse_dt(None) is None
        assert _parse_dt("") is None


# --------------------------------------------------------------------------- #
# Storage state
# --------------------------------------------------------------------------- #


class TestEdroloStorageState:
    def test_round_trip(self, tmp_path: Path, storage_raw):
        path = tmp_path / "edrolo.json"
        EdroloStorageState(storage_raw).save(path)
        loaded = EdroloStorageState.load(path)
        assert "sessionid" in loaded.cookies_for_domain("edrolo.com.au")

    def test_load_missing_raises_auth_expired(self, tmp_path: Path):
        with pytest.raises(AuthExpiredError):
            EdroloStorageState.load(tmp_path / "nope.json")

    def test_load_garbage_json_raises_auth_expired(self, tmp_path: Path):
        path = tmp_path / "edrolo.json"
        path.write_text("not json {{{")
        with pytest.raises(AuthExpiredError):
            EdroloStorageState.load(path)

    def test_validate_missing_sessionid_raises(self, tmp_path: Path, storage_raw):
        # Drop sessionid; csrftoken alone is insufficient.
        storage_raw["cookies"] = [c for c in storage_raw["cookies"] if c["name"] != "sessionid"]
        path = tmp_path / "edrolo.json"
        path.write_text(json.dumps(storage_raw))
        with pytest.raises(AuthExpiredError, match="sessionid"):
            EdroloStorageState.load(path)

    def test_save_creates_parent_dir(self, tmp_path: Path, storage_raw):
        path = tmp_path / "nested" / "deeper" / "t.json"
        EdroloStorageState(storage_raw).save(path)
        assert path.exists()

    def test_cookies_for_domain_matches_subdomain_cookies(self, storage_raw):
        # Cookies set with leading-dot domain (.edrolo.com.au) should match.
        state = EdroloStorageState(storage_raw)
        cookies = state.cookies_for_domain("edrolo.com.au")
        assert cookies["sessionid"] == "fake-session-abc123"
        assert cookies["csrftoken"] == "fake-csrf-xyz789"

    def test_cookie_header_renders_all_matching(self, storage_raw):
        state = EdroloStorageState(storage_raw)
        header = state.cookie_header()
        assert "sessionid=fake-session-abc123" in header
        assert "csrftoken=fake-csrf-xyz789" in header


# --------------------------------------------------------------------------- #
# EdroloClient — full HTTP code path via httpx.MockTransport
# --------------------------------------------------------------------------- #


class TestEdroloClient:
    @staticmethod
    def _client(handler, storage_raw) -> EdroloClient:
        transport = httpx.MockTransport(handler)
        http_client = httpx.Client(transport=transport, follow_redirects=False)
        storage = EdroloStorageState(storage_raw)
        return EdroloClient(storage, client=http_client)

    def test_get_tasks_happy_path(self, storage_raw):
        captured: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["headers"] = dict(request.headers)
            return httpx.Response(200, json=_load("edrolo_response.json"))

        client = self._client(handler, storage_raw)
        result = client.get_tasks()

        assert "/api/student/tasks/" in captured["url"]
        assert "sessionid=fake-session-abc123" in captured["headers"]["cookie"]
        assert captured["headers"]["x-requested-with"] == "XMLHttpRequest"
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["id"] == 99821

    def test_302_translates_to_auth_expired(self, storage_raw):
        client = self._client(
            lambda _r: httpx.Response(302, headers={"Location": "/account/login/"}),
            storage_raw,
        )
        with pytest.raises(AuthExpiredError):
            client.get_tasks()

    def test_401_translates_to_auth_expired(self, storage_raw):
        client = self._client(lambda _r: httpx.Response(401), storage_raw)
        with pytest.raises(AuthExpiredError):
            client.get_tasks()

    def test_403_translates_to_auth_expired(self, storage_raw):
        client = self._client(lambda _r: httpx.Response(403), storage_raw)
        with pytest.raises(AuthExpiredError):
            client.get_tasks()

    def test_500_translates_to_transient(self, storage_raw):
        client = self._client(lambda _r: httpx.Response(503, text="boom"), storage_raw)
        with pytest.raises(TransientError):
            client.get_tasks()

    def test_400_translates_to_schema_break(self, storage_raw):
        client = self._client(lambda _r: httpx.Response(400, text="bad"), storage_raw)
        with pytest.raises(SchemaBreakError):
            client.get_tasks()

    def test_non_json_body_translates_to_schema_break(self, storage_raw):
        client = self._client(
            lambda _r: httpx.Response(200, text="<html>error</html>"), storage_raw
        )
        with pytest.raises(SchemaBreakError):
            client.get_tasks()


# --------------------------------------------------------------------------- #
# Payload extraction
# --------------------------------------------------------------------------- #


class TestExtractTasksPayload:
    def test_bare_list(self):
        assert _extract_tasks_payload([{"id": 1}]) == [{"id": 1}]

    def test_results_envelope(self):
        assert _extract_tasks_payload({"results": [{"id": 1}]}) == [{"id": 1}]

    def test_tasks_envelope(self):
        assert _extract_tasks_payload({"tasks": [{"id": 1}]}) == [{"id": 1}]

    def test_data_envelope(self):
        assert _extract_tasks_payload({"data": [{"id": 1}]}) == [{"id": 1}]

    def test_unknown_envelope_raises(self):
        with pytest.raises(SchemaBreakError):
            _extract_tasks_payload({"weird": [{"id": 1}]})

    def test_non_dict_non_list_raises(self):
        with pytest.raises(SchemaBreakError):
            _extract_tasks_payload("nope")


# --------------------------------------------------------------------------- #
# EdroloSource
# --------------------------------------------------------------------------- #


class FakeEdroloClient:
    def __init__(self, raw_tasks: list[dict]):
        self.raw_tasks = raw_tasks
        self.calls: list[EdroloStorageState] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        pass

    def get_tasks(self) -> list[dict]:
        return self.raw_tasks


class TestEdroloSource:
    def _save_storage(self, tmp_path: Path, storage_raw, name: str) -> Path:
        path = tmp_path / name
        EdroloStorageState(storage_raw).save(path)
        return path

    def test_fetch_uses_per_child_storage_path(self, tmp_path: Path, task, storage_raw):
        james_path = self._save_storage(tmp_path, storage_raw, "james-edrolo.json")
        tahlia_path = self._save_storage(tmp_path, storage_raw, "tahlia-edrolo.json")

        loaded_paths: list[Path] = []

        def factory(storage):
            loaded_paths.append(storage.path)
            return FakeEdroloClient([task])

        source = EdroloSource(
            {"james": james_path, "tahlia": tahlia_path},
            client_factory=factory,
        )
        tasks = source.fetch("james")
        assert loaded_paths == [james_path]
        assert len(tasks) == 1
        assert tasks[0].child == "james"
        assert tasks[0].source_id == "99821"

    def test_unknown_child_raises_schema_break(self, tmp_path: Path, storage_raw):
        path = self._save_storage(tmp_path, storage_raw, "james-edrolo.json")
        source = EdroloSource({"james": path}, client_factory=lambda _s: FakeEdroloClient([]))
        with pytest.raises(SchemaBreakError, match="storage state"):
            source.fetch("nobody")

    def test_missing_storage_file_raises_auth_expired(self, tmp_path: Path):
        source = EdroloSource(
            {"james": tmp_path / "missing.json"},
            client_factory=lambda _s: FakeEdroloClient([]),
        )
        with pytest.raises(AuthExpiredError):
            source.fetch("james")

    def test_response_passed_through_mapper(self, tmp_path: Path, storage_raw):
        path = self._save_storage(tmp_path, storage_raw, "james-edrolo.json")
        raw = _load("edrolo_response.json")["results"]
        source = EdroloSource(
            {"james": path},
            client_factory=lambda _s: FakeEdroloClient(raw),
        )
        tasks = source.fetch("james")
        assert len(tasks) == 2
        # Second task has submitted_at → status inferred as SUBMITTED
        assert tasks[1].status is Status.SUBMITTED

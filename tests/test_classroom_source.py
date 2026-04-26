"""Tests for the Classroom source — pure mapping logic only.

The live API client is not unit-tested; it'll get an integration test once we
have real OAuth credentials wired up.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status
from homework_hub.sources.base import SchemaBreakError
from homework_hub.sources.classroom import (
    _extract_due_at,
    load_client_secret,
    map_coursework_to_task,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def course():
    return _load("classroom_course.json")


@pytest.fixture
def coursework():
    return _load("classroom_coursework.json")


@pytest.fixture
def submission():
    return _load("classroom_submission.json")


class TestMapping:
    def test_basic_mapping_produces_task(self, course, coursework, submission):
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.source is SourceEnum.CLASSROOM
        assert t.child == "james"
        assert t.subject == "Year 9 Mathematics"
        assert t.title == "Algebra worksheet 4"
        assert "quadratics" in t.description
        assert t.url.startswith("https://classroom.google.com/")

    def test_source_id_combines_course_and_coursework(self, course, coursework, submission):
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.source_id == "course-123:cw-456"

    def test_due_date_parsed_correctly(self, course, coursework, submission):
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.due_at == datetime(2026, 4, 28, 23, 59, 0, tzinfo=UTC)

    def test_assigned_at_from_creation_time(self, course, coursework, submission):
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.assigned_at == datetime(2026, 4, 20, 8, 0, 0, tzinfo=UTC)

    def test_state_created_maps_to_in_progress(self, course, coursework, submission):
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.status_raw == "CREATED"
        assert t.status is Status.IN_PROGRESS

    def test_state_turned_in_maps_to_submitted(self, course, coursework, submission):
        submission["state"] = "TURNED_IN"
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.status is Status.SUBMITTED

    def test_state_returned_maps_to_graded(self, course, coursework, submission):
        submission["state"] = "RETURNED"
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.status is Status.GRADED

    def test_state_new_maps_to_not_started(self, course, coursework, submission):
        submission["state"] = "NEW"
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.status is Status.NOT_STARTED

    def test_late_flag_overrides_to_overdue(self, course, coursework, submission):
        submission["late"] = True
        submission["state"] = "CREATED"
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.status is Status.OVERDUE

    def test_late_does_not_override_submitted(self, course, coursework, submission):
        submission["late"] = True
        submission["state"] = "TURNED_IN"
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.status is Status.SUBMITTED

    def test_no_submission_defaults_to_not_started(self, course, coursework):
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=None
        )
        assert t.status is Status.NOT_STARTED
        assert t.status_raw == "NEW"

    def test_unknown_state_falls_back_to_not_started(self, course, coursework, submission):
        submission["state"] = "WHO_KNOWS"
        t = map_coursework_to_task(
            child="james", course=course, coursework=coursework, submission=submission
        )
        assert t.status is Status.NOT_STARTED
        assert t.status_raw == "WHO_KNOWS"

    def test_missing_id_raises_schema_break(self, course, coursework, submission):
        del coursework["id"]
        with pytest.raises(SchemaBreakError):
            map_coursework_to_task(
                child="james",
                course=course,
                coursework=coursework,
                submission=submission,
            )

    def test_missing_title_raises_schema_break(self, course, coursework, submission):
        del coursework["title"]
        with pytest.raises(SchemaBreakError):
            map_coursework_to_task(
                child="james",
                course=course,
                coursework=coursework,
                submission=submission,
            )


class TestExtractDueAt:
    def test_full_date_and_time(self):
        cw = {
            "dueDate": {"year": 2026, "month": 4, "day": 1},
            "dueTime": {"hours": 9, "minutes": 0},
        }
        assert _extract_due_at(cw) == datetime(2026, 4, 1, 9, 0, 0, tzinfo=UTC)

    def test_date_only_defaults_to_end_of_day(self):
        cw = {"dueDate": {"year": 2026, "month": 4, "day": 1}}
        assert _extract_due_at(cw) == datetime(2026, 4, 1, 23, 59, 0, tzinfo=UTC)

    def test_no_due_date_returns_none(self):
        assert _extract_due_at({}) is None

    def test_partial_date_returns_none(self):
        cw = {"dueDate": {"year": 2026, "month": 4}}  # missing day
        assert _extract_due_at(cw) is None


class TestLoadClientSecret:
    def test_full_installed_wrapper_passed_through(self):
        raw = json.dumps({"installed": {"client_id": "x", "client_secret": "y"}})
        out = load_client_secret(raw)
        assert "installed" in out

    def test_web_wrapper_passed_through(self):
        raw = json.dumps({"web": {"client_id": "x"}})
        out = load_client_secret(raw)
        assert "web" in out

    def test_bare_inner_shape_wrapped(self):
        raw = json.dumps({"client_id": "x", "client_secret": "y"})
        out = load_client_secret(raw)
        assert out == {"installed": {"client_id": "x", "client_secret": "y"}}

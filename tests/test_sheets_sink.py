"""Tests for the Sheets sink — pure logic, no API calls."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

from homework_hub.models import Source, Status, Task
from homework_hub.sinks.sheets import RAW_HEADERS, task_to_row, tasks_to_matrix


def _task(**overrides) -> Task:
    base = {
        "source": Source.CLASSROOM,
        "source_id": "abc",
        "child": "james",
        "title": "Maths",
    }
    base.update(overrides)
    return Task(**base)


class TestTaskToRow:
    def test_row_length_matches_headers(self):
        assert len(task_to_row(_task())) == len(RAW_HEADERS)

    def test_field_ordering_matches_headers(self):
        t = _task(
            source=Source.COMPASS,
            source_id="lt-42",
            child="tahlia",
            subject="History",
            title="Essay draft",
            description="500 words on WW1",
            status=Status.IN_PROGRESS,
            status_raw="InProgress",
            url="https://compass.example/lt-42",
        )
        row = task_to_row(t)
        as_dict = dict(zip(RAW_HEADERS, row, strict=True))
        assert as_dict["child"] == "tahlia"
        assert as_dict["source"] == "compass"
        assert as_dict["source_id"] == "lt-42"
        assert as_dict["subject"] == "History"
        assert as_dict["title"] == "Essay draft"
        assert as_dict["description"] == "500 words on WW1"
        assert as_dict["status"] == "in_progress"
        assert as_dict["status_raw"] == "InProgress"
        assert as_dict["url"] == "https://compass.example/lt-42"

    def test_none_datetimes_render_empty(self):
        t = _task(due_at=None, assigned_at=None)
        as_dict = dict(zip(RAW_HEADERS, task_to_row(t), strict=True))
        assert as_dict["due_at"] == ""
        assert as_dict["assigned_at"] == ""

    def test_datetimes_rendered_as_iso_z(self):
        t = _task(
            due_at=datetime(2026, 4, 1, 9, 0, 0, tzinfo=UTC),
            assigned_at=datetime(2026, 3, 25, 0, 0, 0, tzinfo=UTC),
        )
        as_dict = dict(zip(RAW_HEADERS, task_to_row(t), strict=True))
        assert as_dict["due_at"] == "2026-04-01T09:00:00Z"
        assert as_dict["assigned_at"] == "2026-03-25T00:00:00Z"

    def test_aware_datetimes_converted_to_utc(self):
        melbourne = timezone(timedelta(hours=11))
        t = _task(due_at=datetime(2026, 4, 1, 20, 0, 0, tzinfo=melbourne))
        as_dict = dict(zip(RAW_HEADERS, task_to_row(t), strict=True))
        assert as_dict["due_at"] == "2026-04-01T09:00:00Z"


class TestTasksToMatrix:
    def test_includes_header_row(self):
        m = tasks_to_matrix([])
        assert m == [RAW_HEADERS]

    def test_one_row_per_task_plus_header(self):
        tasks = [_task(source_id="a"), _task(source_id="b"), _task(source_id="c")]
        m = tasks_to_matrix(tasks)
        assert len(m) == 4
        assert m[0] == RAW_HEADERS
        ids = [row[RAW_HEADERS.index("source_id")] for row in m[1:]]
        assert ids == ["a", "b", "c"]

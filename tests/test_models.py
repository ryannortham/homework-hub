"""Tests for the canonical Task model."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from homework_hub.models import Source, Status, Task, merge_tasks


def _task(**overrides) -> Task:
    base = {
        "source": Source.CLASSROOM,
        "source_id": "abc",
        "child": "james",
        "title": "Maths sheet 4",
    }
    base.update(overrides)
    return Task(**base)


class TestTaskValidation:
    def test_minimum_required_fields(self):
        t = _task()
        assert t.source is Source.CLASSROOM
        assert t.status is Status.NOT_STARTED
        assert t.last_synced.tzinfo is UTC

    def test_source_id_must_be_non_empty(self):
        with pytest.raises(ValidationError):
            _task(source_id="")

    def test_title_must_be_non_empty(self):
        with pytest.raises(ValidationError):
            _task(title="")

    def test_naive_datetimes_assumed_utc(self):
        naive = datetime(2026, 4, 1, 10, 0, 0)
        t = _task(due_at=naive)
        assert t.due_at == datetime(2026, 4, 1, 10, 0, 0, tzinfo=UTC)

    def test_aware_datetimes_converted_to_utc(self):
        melbourne = timezone(timedelta(hours=11))
        aware = datetime(2026, 4, 1, 21, 0, 0, tzinfo=melbourne)
        t = _task(due_at=aware)
        assert t.due_at == datetime(2026, 4, 1, 10, 0, 0, tzinfo=UTC)


class TestDedupKey:
    def test_dedup_key_components(self):
        t = _task(source_id="xyz")
        assert t.dedup_key == ("james", "classroom", "xyz")

    def test_same_source_id_different_child_not_equal(self):
        a = _task(child="james", source_id="x")
        b = _task(child="tahlia", source_id="x")
        assert a.dedup_key != b.dedup_key

    def test_same_source_id_different_source_not_equal(self):
        a = _task(source=Source.CLASSROOM, source_id="x")
        b = _task(source=Source.COMPASS, source_id="x")
        assert a.dedup_key != b.dedup_key


class TestOverdueCheck:
    def test_marks_overdue_when_past_due(self):
        past = datetime.now(UTC) - timedelta(days=1)
        t = _task(due_at=past, status=Status.NOT_STARTED)
        assert t.with_overdue_check().status is Status.OVERDUE

    def test_does_not_mark_overdue_when_future(self):
        future = datetime.now(UTC) + timedelta(days=1)
        t = _task(due_at=future, status=Status.NOT_STARTED)
        assert t.with_overdue_check().status is Status.NOT_STARTED

    def test_no_due_date_unchanged(self):
        t = _task(status=Status.NOT_STARTED)
        assert t.with_overdue_check().status is Status.NOT_STARTED

    def test_submitted_tasks_never_marked_overdue(self):
        past = datetime.now(UTC) - timedelta(days=1)
        t = _task(due_at=past, status=Status.SUBMITTED)
        assert t.with_overdue_check().status is Status.SUBMITTED

    def test_graded_tasks_never_marked_overdue(self):
        past = datetime.now(UTC) - timedelta(days=1)
        t = _task(due_at=past, status=Status.GRADED)
        assert t.with_overdue_check().status is Status.GRADED

    def test_supports_explicit_now(self):
        due = datetime(2026, 1, 1, tzinfo=UTC)
        t = _task(due_at=due)
        before = datetime(2025, 12, 31, tzinfo=UTC)
        after = datetime(2026, 1, 2, tzinfo=UTC)
        assert t.with_overdue_check(now=before).status is Status.NOT_STARTED
        assert t.with_overdue_check(now=after).status is Status.OVERDUE


class TestMergeTasks:
    def test_empty_existing_returns_incoming(self):
        incoming = [_task(source_id="a")]
        assert merge_tasks([], incoming) == incoming

    def test_empty_incoming_returns_existing(self):
        existing = [_task(source_id="a")]
        assert merge_tasks(existing, []) == existing

    def test_incoming_replaces_existing_with_same_key(self):
        old = _task(source_id="a", title="old title")
        new = _task(source_id="a", title="new title")
        result = merge_tasks([old], [new])
        assert len(result) == 1
        assert result[0].title == "new title"

    def test_unmatched_existing_preserved(self):
        kept = _task(source_id="keep")
        replaced_old = _task(source_id="r", title="old")
        replaced_new = _task(source_id="r", title="new")
        result = merge_tasks([kept, replaced_old], [replaced_new])
        assert len(result) == 2
        assert {t.source_id for t in result} == {"keep", "r"}
        replaced = next(t for t in result if t.source_id == "r")
        assert replaced.title == "new"

    def test_incoming_appears_first_in_result(self):
        existing = [_task(source_id="old1"), _task(source_id="old2")]
        incoming = [_task(source_id="new1"), _task(source_id="new2")]
        result = merge_tasks(existing, incoming)
        assert [t.source_id for t in result] == ["new1", "new2", "old1", "old2"]

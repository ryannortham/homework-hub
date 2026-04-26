"""Tests for the SQLite state store — seen-tasks dedup + auth status."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from homework_hub.models import Source, Status, Task
from homework_hub.state.store import StateStore, task_signature


def _task(
    child: str = "james",
    source: Source = Source.CLASSROOM,
    source_id: str = "abc",
    title: str = "Maths Q1-5",
    subject: str = "Maths",
    due: datetime | None = None,
    status: Status = Status.NOT_STARTED,
) -> Task:
    return Task(
        source=source,
        source_id=source_id,
        child=child,
        subject=subject,
        title=title,
        due_at=due or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        status=status,
    )


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


# --------------------------------------------------------------------------- #
# task_signature
# --------------------------------------------------------------------------- #


class TestTaskSignature:
    def test_same_task_same_signature(self):
        a = _task()
        b = _task()
        assert task_signature(a) == task_signature(b)

    def test_title_change_changes_signature(self):
        a = _task(title="Old")
        b = _task(title="New")
        assert task_signature(a) != task_signature(b)

    def test_due_change_changes_signature(self):
        a = _task(due=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))
        b = _task(due=datetime(2026, 5, 2, 12, 0, tzinfo=UTC))
        assert task_signature(a) != task_signature(b)

    def test_status_change_changes_signature(self):
        a = _task(status=Status.NOT_STARTED)
        b = _task(status=Status.SUBMITTED)
        assert task_signature(a) != task_signature(b)

    def test_description_change_does_not_change_signature(self):
        # Whitespace-different HTML shouldn't manufacture a 'change'.
        a = _task()
        b = _task()
        b_with_desc = b.model_copy(update={"description": "padded prose"})
        assert task_signature(a) == task_signature(b_with_desc)

    def test_subject_change_changes_signature(self):
        a = _task(subject="Maths")
        b = _task(subject="Science")
        assert task_signature(a) != task_signature(b)


# --------------------------------------------------------------------------- #
# StateStore — schema bootstrap
# --------------------------------------------------------------------------- #


class TestStoreLifecycle:
    def test_init_creates_db_and_parent_dirs(self, tmp_path: Path):
        path = tmp_path / "nested" / "deeper" / "state.db"
        StateStore(path)
        assert path.exists()

    def test_reopen_existing_db_does_not_clobber(self, tmp_path: Path):
        path = tmp_path / "state.db"
        s1 = StateStore(path)
        s1.upsert_seen([_task()])
        s2 = StateStore(path)
        assert s2.get_seen("james", "classroom", "abc") is not None


# --------------------------------------------------------------------------- #
# upsert_seen — new / changed / unchanged classification
# --------------------------------------------------------------------------- #


class TestUpsertSeen:
    def test_first_sync_marks_all_new(self, store: StateStore):
        result = store.upsert_seen([_task(source_id="a"), _task(source_id="b")])
        assert len(result.new) == 2
        assert result.changed == []
        assert result.unchanged == []

    def test_second_sync_with_no_changes_marks_all_unchanged(self, store: StateStore):
        store.upsert_seen([_task(source_id="a")])
        result = store.upsert_seen([_task(source_id="a")])
        assert result.new == []
        assert result.changed == []
        assert len(result.unchanged) == 1

    def test_due_date_change_classified_as_changed(self, store: StateStore):
        store.upsert_seen([_task(source_id="a", due=datetime(2026, 5, 1, 12, 0, tzinfo=UTC))])
        result = store.upsert_seen(
            [_task(source_id="a", due=datetime(2026, 5, 5, 12, 0, tzinfo=UTC))]
        )
        assert result.new == []
        assert len(result.changed) == 1
        assert result.unchanged == []

    def test_status_change_classified_as_changed(self, store: StateStore):
        store.upsert_seen([_task(source_id="a", status=Status.NOT_STARTED)])
        result = store.upsert_seen([_task(source_id="a", status=Status.SUBMITTED)])
        assert len(result.changed) == 1

    def test_mixed_batch_classified_correctly(self, store: StateStore):
        # Seed: a (initial), b (initial)
        store.upsert_seen([_task(source_id="a"), _task(source_id="b")])
        # Second sync: a unchanged, b changed (status flip), c is new
        result = store.upsert_seen(
            [
                _task(source_id="a"),  # unchanged
                _task(source_id="b", status=Status.SUBMITTED),  # changed
                _task(source_id="c"),  # new
            ]
        )
        assert [t.source_id for t in result.new] == ["c"]
        assert [t.source_id for t in result.changed] == ["b"]
        assert [t.source_id for t in result.unchanged] == ["a"]

    def test_dedup_key_scoped_per_child(self, store: StateStore):
        # Same source_id under two different children must coexist.
        store.upsert_seen([_task(child="james", source_id="x")])
        result = store.upsert_seen([_task(child="tahlia", source_id="x")])
        assert len(result.new) == 1

    def test_dedup_key_scoped_per_source(self, store: StateStore):
        # Same source_id from different sources must coexist.
        store.upsert_seen([_task(source=Source.CLASSROOM, source_id="x")])
        result = store.upsert_seen([_task(source=Source.COMPASS, source_id="x")])
        assert len(result.new) == 1

    def test_first_seen_at_preserved_across_changes(self, store: StateStore):
        t0 = datetime(2026, 4, 1, 10, 0, tzinfo=UTC)
        t1 = t0 + timedelta(days=2)
        store.upsert_seen([_task(source_id="a")], now=t0)
        store.upsert_seen([_task(source_id="a", status=Status.SUBMITTED)], now=t1)
        rec = store.get_seen("james", "classroom", "a")
        assert rec is not None
        assert rec.first_seen_at == t0
        assert rec.last_seen_at == t1

    def test_last_seen_at_advances_even_when_unchanged(self, store: StateStore):
        t0 = datetime(2026, 4, 1, 10, 0, tzinfo=UTC)
        t1 = t0 + timedelta(hours=1)
        store.upsert_seen([_task(source_id="a")], now=t0)
        store.upsert_seen([_task(source_id="a")], now=t1)
        rec = store.get_seen("james", "classroom", "a")
        assert rec is not None
        assert rec.last_seen_at == t1


# --------------------------------------------------------------------------- #
# get_seen / all_seen_for_child
# --------------------------------------------------------------------------- #


class TestGetSeen:
    def test_get_missing_returns_none(self, store: StateStore):
        assert store.get_seen("nobody", "classroom", "x") is None

    def test_all_seen_for_child_filters(self, store: StateStore):
        store.upsert_seen(
            [
                _task(child="james", source_id="a"),
                _task(child="james", source_id="b"),
                _task(child="tahlia", source_id="c"),
            ]
        )
        james_seen = store.all_seen_for_child("james")
        assert {r.source_id for r in james_seen} == {"a", "b"}


# --------------------------------------------------------------------------- #
# auth_status
# --------------------------------------------------------------------------- #


class TestAuthStatus:
    def test_record_success_then_get(self, store: StateStore):
        ts = datetime(2026, 4, 25, 10, 0, tzinfo=UTC)
        store.record_success("james", "classroom", now=ts)
        rec = store.get_auth("james", "classroom")
        assert rec is not None
        assert rec.last_success_at == ts
        assert rec.last_failure_at is None

    def test_record_failure_then_get(self, store: StateStore):
        ts = datetime(2026, 4, 25, 10, 0, tzinfo=UTC)
        store.record_failure(
            "james",
            "compass",
            kind="auth_expired",
            message="cookie rejected",
            now=ts,
        )
        rec = store.get_auth("james", "compass")
        assert rec is not None
        assert rec.last_failure_at == ts
        assert rec.last_failure_kind == "auth_expired"
        assert rec.last_failure_message == "cookie rejected"

    def test_success_then_failure_keeps_both(self, store: StateStore):
        success_at = datetime(2026, 4, 25, 9, 0, tzinfo=UTC)
        failure_at = datetime(2026, 4, 25, 10, 0, tzinfo=UTC)
        store.record_success("james", "edrolo", now=success_at)
        store.record_failure("james", "edrolo", kind="transient", message="timeout", now=failure_at)
        rec = store.get_auth("james", "edrolo")
        assert rec is not None
        assert rec.last_success_at == success_at
        assert rec.last_failure_at == failure_at
        assert rec.last_failure_kind == "transient"

    def test_recovery_overwrites_only_success(self, store: StateStore):
        # After a failure, a later success should advance success_at but
        # leave the failure record intact (so the operator can still see
        # the most recent failure context).
        store.record_failure("james", "compass", kind="auth_expired", message="boom")
        recovery = datetime(2026, 4, 25, 11, 0, tzinfo=UTC)
        store.record_success("james", "compass", now=recovery)
        rec = store.get_auth("james", "compass")
        assert rec is not None
        assert rec.last_success_at == recovery
        assert rec.last_failure_kind == "auth_expired"

    def test_get_missing_returns_none(self, store: StateStore):
        assert store.get_auth("nobody", "classroom") is None

    def test_all_auth_returns_every_pair(self, store: StateStore):
        store.record_success("james", "classroom")
        store.record_success("james", "compass")
        store.record_failure("tahlia", "edrolo", kind="schema_break", message="fields moved")
        records = store.all_auth()
        assert len(records) == 3
        keys = {(r.child, r.source) for r in records}
        assert keys == {
            ("james", "classroom"),
            ("james", "compass"),
            ("tahlia", "edrolo"),
        }

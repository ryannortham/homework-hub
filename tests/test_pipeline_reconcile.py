"""Tests for the silver-layer cleansing sweeps in ``pipeline.reconcile``.

Covers:

* ``reconcile_stale`` — streak increment, archive after grace, recovery
  on reappearance, skip for terminal/already-archived rows.
* ``apply_age_cap`` — archives non-terminal rows whose anchor is older
  than the cutoff, respecting terminals and existing archives.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task
from homework_hub.pipeline.reconcile import apply_age_cap, reconcile_stale
from homework_hub.pipeline.transform import SilverWriter
from homework_hub.state.store import StateStore

NOW = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


@pytest.fixture
def writer(store: StateStore) -> SilverWriter:
    return SilverWriter(store)


def _task(
    *,
    child: str = "james",
    source: SourceEnum = SourceEnum.COMPASS,
    source_id: str = "T1",
    subject: str = "9MATH",
    title: str = "Algebra worksheet",
    status: Status = Status.NOT_STARTED,
    due: datetime | None = None,
) -> Task:
    return Task(
        source=source,
        source_id=source_id,
        child=child,
        subject=subject,
        title=title,
        due_at=due,
        status=status,
    )


# --------------------------------------------------------------------------- #
# reconcile_stale
# --------------------------------------------------------------------------- #


class TestReconcileStale:
    def test_seen_resets_streak_and_archives_nothing(self, writer: SilverWriter, store: StateStore):
        writer.upsert_many([(_task(source_id="A"), None)], now=NOW)
        archived = reconcile_stale(
            store,
            child="james",
            source="compass",
            seen_ids=["A"],
            grace_syncs=2,
            now=NOW,
        )
        assert archived == []
        rows = store.active_silver_for(child="james", source="compass")
        assert rows[0]["archived_at"] is None

    def test_missing_one_sync_below_grace_does_not_archive(
        self, writer: SilverWriter, store: StateStore
    ):
        writer.upsert_many([(_task(source_id="A"), None)], now=NOW)
        # First sync without A — streak goes to 1, grace is 2 ⇒ no archive yet.
        archived = reconcile_stale(
            store,
            child="james",
            source="compass",
            seen_ids=[],
            grace_syncs=2,
            now=NOW,
        )
        assert archived == []

    def test_missing_two_syncs_archives_with_upstream_removed(
        self, writer: SilverWriter, store: StateStore
    ):
        writer.upsert_many([(_task(source_id="A"), None)], now=NOW)
        reconcile_stale(store, child="james", source="compass", seen_ids=[], grace_syncs=2, now=NOW)
        archived = reconcile_stale(
            store,
            child="james",
            source="compass",
            seen_ids=[],
            grace_syncs=2,
            now=NOW + timedelta(hours=1),
        )
        assert archived == ["A"]
        rows = store.list_archived(child="james")
        assert len(rows) == 1
        assert rows[0]["source_id"] == "A"
        assert rows[0]["archived_reason"] == "upstream_removed"

    def test_reappearance_clears_upstream_removed_archive(
        self, writer: SilverWriter, store: StateStore
    ):
        writer.upsert_many([(_task(source_id="A"), None)], now=NOW)
        # Disappear long enough to be archived.
        reconcile_stale(store, child="james", source="compass", seen_ids=[], grace_syncs=2, now=NOW)
        reconcile_stale(
            store,
            child="james",
            source="compass",
            seen_ids=[],
            grace_syncs=2,
            now=NOW + timedelta(hours=1),
        )
        assert len(store.list_archived(child="james")) == 1
        # Now the task reappears.
        reconcile_stale(
            store,
            child="james",
            source="compass",
            seen_ids=["A"],
            grace_syncs=2,
            now=NOW + timedelta(hours=2),
        )
        assert store.list_archived(child="james") == []
        rows = store.active_silver_for(child="james", source="compass")
        assert rows[0]["archived_at"] is None
        assert rows[0]["archived_reason"] is None

    def test_terminal_status_is_never_archived(self, writer: SilverWriter, store: StateStore):
        writer.upsert_many([(_task(source_id="A", status=Status.SUBMITTED), None)], now=NOW)
        # Missing for many syncs — should still not be archived.
        for i in range(5):
            reconcile_stale(
                store,
                child="james",
                source="compass",
                seen_ids=[],
                grace_syncs=2,
                now=NOW + timedelta(hours=i),
            )
        assert store.list_archived(child="james") == []

    def test_already_archived_row_not_re_archived(self, writer: SilverWriter, store: StateStore):
        writer.upsert_many([(_task(source_id="A"), None)], now=NOW)
        store.mark_archived(
            child="james",
            source="compass",
            source_id="A",
            reason="manual",
            now=NOW,
        )
        archived = reconcile_stale(
            store,
            child="james",
            source="compass",
            seen_ids=[],
            grace_syncs=1,
            now=NOW + timedelta(hours=1),
        )
        assert archived == []
        # Reason should still be "manual" — not overwritten.
        rows = store.list_archived(child="james")
        assert rows[0]["archived_reason"] == "manual"

    def test_other_source_rows_are_not_touched(self, writer: SilverWriter, store: StateStore):
        writer.upsert_many(
            [
                (_task(source=SourceEnum.COMPASS, source_id="A"), None),
                (_task(source=SourceEnum.CLASSROOM, source_id="B"), None),
            ],
            now=NOW,
        )
        # Reconcile only compass — classroom row should be untouched.
        reconcile_stale(store, child="james", source="compass", seen_ids=[], grace_syncs=1, now=NOW)
        classroom_rows = store.active_silver_for(child="james", source="classroom")
        assert classroom_rows[0]["archived_at"] is None


# --------------------------------------------------------------------------- #
# apply_age_cap
# --------------------------------------------------------------------------- #


class TestApplyAgeCap:
    def test_old_non_terminal_row_is_archived(self, writer: SilverWriter, store: StateStore):
        old_due = NOW - timedelta(days=200)
        writer.upsert_many([(_task(source_id="A", due=old_due), None)], now=NOW)
        archived = apply_age_cap(store, child="james", cutoff_days=60, now=NOW)
        assert archived == [("compass", "A")]
        rows = store.list_archived(child="james")
        assert rows[0]["archived_reason"] == "age_cap"

    def test_recent_row_is_not_archived(self, writer: SilverWriter, store: StateStore):
        recent_due = NOW - timedelta(days=5)
        writer.upsert_many([(_task(source_id="A", due=recent_due), None)], now=NOW)
        archived = apply_age_cap(store, child="james", cutoff_days=60, now=NOW)
        assert archived == []

    def test_terminal_row_not_archived_even_if_old(self, writer: SilverWriter, store: StateStore):
        old_due = NOW - timedelta(days=200)
        writer.upsert_many(
            [(_task(source_id="A", due=old_due, status=Status.SUBMITTED), None)],
            now=NOW,
        )
        archived = apply_age_cap(store, child="james", cutoff_days=60, now=NOW)
        assert archived == []

    def test_already_archived_row_skipped(self, writer: SilverWriter, store: StateStore):
        old_due = NOW - timedelta(days=200)
        writer.upsert_many([(_task(source_id="A", due=old_due), None)], now=NOW)
        store.mark_archived(
            child="james",
            source="compass",
            source_id="A",
            reason="manual",
            now=NOW,
        )
        archived = apply_age_cap(store, child="james", cutoff_days=60, now=NOW)
        assert archived == []
        rows = store.list_archived(child="james")
        assert rows[0]["archived_reason"] == "manual"

    def test_falls_back_to_first_seen_at_when_due_missing(
        self, writer: SilverWriter, store: StateStore
    ):
        old = NOW - timedelta(days=200)
        # No due date — first_seen_at is set by the writer; we override to old.
        writer.upsert_many([(_task(source_id="A", due=None), None)], now=old)
        archived = apply_age_cap(store, child="james", cutoff_days=60, now=NOW)
        assert archived == [("compass", "A")]

    def test_no_anchor_means_no_archive(self, writer: SilverWriter, store: StateStore):
        # Insert with NOW so first_seen_at is recent.
        writer.upsert_many([(_task(source_id="A", due=None), None)], now=NOW)
        archived = apply_age_cap(store, child="james", cutoff_days=60, now=NOW)
        assert archived == []

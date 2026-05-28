"""Tests for the SQLite state store — auth status.

The seen_tasks ledger and ``upsert_seen``/``task_signature`` helpers were
removed alongside the medallion redesign; bronze/silver state is the
system of record now (covered by ``test_state_schema_medallion.py``).
This file keeps the auth_status coverage.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from homework_hub.state.store import StateStore


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


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
        s1.record_success("james", "classroom")
        s2 = StateStore(path)
        assert s2.get_auth("james", "classroom") is not None


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


# --------------------------------------------------------------------------- #
# silver_tasks — archival / reconciliation helpers
# --------------------------------------------------------------------------- #


def _insert_silver(
    store: StateStore,
    *,
    child: str = "james",
    source: str = "compass",
    source_id: str = "T1",
    status: str = "not_started",
    due_at: str | None = None,
    first_seen_at: str | None = None,
    archived_at: str | None = None,
    archived_reason: str | None = None,
    missing_streak: int = 0,
) -> None:
    """Insert a silver_tasks row directly, bypassing SilverWriter, so tests
    can pin specific column values (timestamps, streaks, archive flags)
    without depending on writer behaviour."""
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            "INSERT INTO silver_tasks (child, source, source_id, subject_raw, "
            "subject_canonical, subject_short, title, description, due_at, "
            "status_raw, status, url, bronze_id, last_synced, first_seen_at, "
            "last_seen_at, missing_streak, archived_at, archived_reason) "
            "VALUES (?, ?, ?, '', '', '', '', '', ?, '', ?, '', '', "
            "'2026-05-26T00:00:00+00:00', ?, ?, ?, ?, ?)",
            (
                child,
                source,
                source_id,
                due_at,
                status,
                first_seen_at,
                first_seen_at,
                missing_streak,
                archived_at,
                archived_reason,
            ),
        )


class TestBumpLastSeen:
    def test_resets_streak_and_updates_last_seen(self, store: StateStore):
        _insert_silver(store, source_id="A", missing_streak=3)
        now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
        touched = store.bump_last_seen(child="james", source="compass", source_ids=["A"], now=now)
        assert touched == 1
        rows = store.active_silver_for(child="james", source="compass")
        assert rows[0]["archived_at"] is None

    def test_clears_upstream_removed_archive(self, store: StateStore):
        _insert_silver(
            store,
            source_id="A",
            status="archived",
            archived_at="2026-05-01T00:00:00+00:00",
            archived_reason="upstream_removed",
        )
        store.bump_last_seen(child="james", source="compass", source_ids=["A"])
        rows = store.list_archived(child="james")
        assert rows == []

    def test_does_not_clear_manual_archive(self, store: StateStore):
        _insert_silver(
            store,
            source_id="A",
            status="archived",
            archived_at="2026-05-01T00:00:00+00:00",
            archived_reason="manual",
        )
        store.bump_last_seen(child="james", source="compass", source_ids=["A"])
        rows = store.list_archived(child="james")
        assert len(rows) == 1
        assert rows[0]["archived_reason"] == "manual"

    def test_empty_source_ids_is_noop(self, store: StateStore):
        _insert_silver(store, source_id="A")
        touched = store.bump_last_seen(child="james", source="compass", source_ids=[])
        assert touched == 0


class TestMarkAndClearArchive:
    def test_mark_archived_sets_flags(self, store: StateStore):
        _insert_silver(store, source_id="A")
        ok = store.mark_archived(child="james", source="compass", source_id="A", reason="age_cap")
        assert ok is True
        rows = store.list_archived(child="james")
        assert rows[0]["archived_reason"] == "age_cap"

    def test_mark_archived_missing_row_returns_false(self, store: StateStore):
        assert (
            store.mark_archived(child="james", source="compass", source_id="GHOST", reason="manual")
            is False
        )

    def test_clear_archive_resets_to_not_started(self, store: StateStore):
        _insert_silver(
            store,
            source_id="A",
            status="archived",
            archived_at="2026-05-01T00:00:00+00:00",
            archived_reason="manual",
        )
        ok = store.clear_archive(child="james", source="compass", source_id="A")
        assert ok is True
        assert store.list_archived(child="james") == []
        rows = store.active_silver_for(child="james", source="compass")
        assert rows[0]["status"] == "not_started"


class TestListArchived:
    def test_filters_by_reason(self, store: StateStore):
        _insert_silver(
            store,
            source_id="A",
            status="archived",
            archived_at="2026-05-01T00:00:00+00:00",
            archived_reason="age_cap",
        )
        _insert_silver(
            store,
            source_id="B",
            status="archived",
            archived_at="2026-05-02T00:00:00+00:00",
            archived_reason="manual",
        )
        age_cap = store.list_archived(child="james", reason="age_cap")
        manual = store.list_archived(child="james", reason="manual")
        assert {r["source_id"] for r in age_cap} == {"A"}
        assert {r["source_id"] for r in manual} == {"B"}

    def test_ordered_by_archived_at_desc(self, store: StateStore):
        _insert_silver(
            store,
            source_id="OLD",
            status="archived",
            archived_at="2026-05-01T00:00:00+00:00",
            archived_reason="manual",
        )
        _insert_silver(
            store,
            source_id="NEW",
            status="archived",
            archived_at="2026-05-10T00:00:00+00:00",
            archived_reason="manual",
        )
        rows = store.list_archived(child="james")
        assert [r["source_id"] for r in rows] == ["NEW", "OLD"]


class TestIncrementMissingStreak:
    def test_only_rows_outside_seen_ids_incremented(self, store: StateStore):
        _insert_silver(store, source_id="A", missing_streak=0)
        _insert_silver(store, source_id="B", missing_streak=0)
        rows = store.increment_missing_streak(child="james", source="compass", seen_ids=["A"])
        by_id = {r["source_id"]: r for r in rows}
        assert by_id["A"]["missing_streak"] == 0
        assert by_id["B"]["missing_streak"] == 1

    def test_empty_seen_ids_increments_everything(self, store: StateStore):
        _insert_silver(store, source_id="A", missing_streak=2)
        rows = store.increment_missing_streak(child="james", source="compass", seen_ids=[])
        assert rows[0]["missing_streak"] == 3

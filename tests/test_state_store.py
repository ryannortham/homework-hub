"""Tests for the SQLite state store — auth status.

The seen_tasks ledger and ``upsert_seen``/``task_signature`` helpers were
removed alongside the medallion redesign; bronze/silver state is the
system of record now (covered by ``test_state_schema_medallion.py``).
This file keeps the auth_status coverage.
"""

from __future__ import annotations

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

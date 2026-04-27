"""Schema tests for the medallion tables (M1).

Verifies the new bronze/silver/dim_subjects/silver_task_links/sync_runs
tables are created with the expected columns and constraints. Pure DDL
checks — no behaviour yet (those land in M2+).
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from homework_hub.state.store import StateStore


@pytest.fixture
def conn(tmp_path: Path) -> sqlite3.Connection:
    StateStore(tmp_path / "state.db")
    return sqlite3.connect(tmp_path / "state.db")


def _columns(conn: sqlite3.Connection, table: str) -> dict[str, sqlite3.Row]:
    with closing(conn.execute(f"PRAGMA table_info({table})")) as cur:
        rows = cur.fetchall()
    # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
    return {r[1]: r for r in rows}


def _indexes(conn: sqlite3.Connection, table: str) -> set[str]:
    with closing(conn.execute(f"PRAGMA index_list({table})")) as cur:
        return {r[1] for r in cur.fetchall()}


class TestBronzeRecords:
    def test_table_exists_with_expected_columns(self, conn: sqlite3.Connection):
        cols = _columns(conn, "bronze_records")
        assert set(cols) == {
            "id",
            "child",
            "source",
            "source_id",
            "payload_json",
            "payload_hash",
            "fetched_at",
        }

    def test_id_is_autoincrement_pk(self, conn: sqlite3.Connection):
        cols = _columns(conn, "bronze_records")
        assert cols["id"][5] == 1  # pk flag
        assert cols["id"][2].upper() == "INTEGER"

    def test_unique_constraint_dedupes_identical_payloads(self, conn: sqlite3.Connection):
        with conn:
            conn.execute(
                "INSERT INTO bronze_records "
                "(child, source, source_id, payload_json, payload_hash, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("james", "compass", "1", "{}", "hash1", "2026-04-26T00:00:00+00:00"),
            )
        with pytest.raises(sqlite3.IntegrityError), conn:
            conn.execute(
                "INSERT INTO bronze_records "
                "(child, source, source_id, payload_json, payload_hash, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("james", "compass", "1", "{}", "hash1", "2026-04-26T01:00:00+00:00"),
            )

    def test_different_hash_for_same_id_is_allowed(self, conn: sqlite3.Connection):
        # Append-only: if upstream changes, we keep both rows.
        with conn:
            conn.execute(
                "INSERT INTO bronze_records "
                "(child, source, source_id, payload_json, payload_hash, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("james", "compass", "1", "{}", "hash1", "2026-04-26T00:00:00+00:00"),
            )
            conn.execute(
                "INSERT INTO bronze_records "
                "(child, source, source_id, payload_json, payload_hash, fetched_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("james", "compass", "1", '{"a":1}', "hash2", "2026-04-26T01:00:00+00:00"),
            )
        count = conn.execute(
            "SELECT COUNT(*) FROM bronze_records WHERE child=? AND source_id=?",
            ("james", "1"),
        ).fetchone()[0]
        assert count == 2

    def test_lookup_index_exists(self, conn: sqlite3.Connection):
        assert "ix_bronze_lookup" in _indexes(conn, "bronze_records")


class TestSilverTasks:
    def test_table_exists_with_expected_columns(self, conn: sqlite3.Connection):
        cols = _columns(conn, "silver_tasks")
        assert set(cols) == {
            "child",
            "source",
            "source_id",
            "subject_raw",
            "subject_canonical",
            "subject_short",
            "title",
            "description",
            "assigned_at",
            "due_at",
            "submitted_at",
            "status_raw",
            "status",
            "url",
            "bronze_id",
            "last_synced",
        }

    def test_composite_pk(self, conn: sqlite3.Connection):
        cols = _columns(conn, "silver_tasks")
        pk_cols = {name for name, row in cols.items() if row[5] > 0}
        assert pk_cols == {"child", "source", "source_id"}

    def test_pk_enforces_uniqueness(self, conn: sqlite3.Connection):
        row = (
            "james",
            "compass",
            "1",
            "raw",
            "canon",
            "short",
            "title",
            "",
            None,
            None,
            "",
            "not_started",
            "",
            None,
            "2026-04-26T00:00:00+00:00",
        )
        with conn:
            conn.execute(
                "INSERT INTO silver_tasks "
                "(child, source, source_id, subject_raw, subject_canonical, "
                "subject_short, title, description, assigned_at, due_at, "
                "status_raw, status, url, bronze_id, last_synced) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row,
            )
        with pytest.raises(sqlite3.IntegrityError), conn:
            conn.execute(
                "INSERT INTO silver_tasks "
                "(child, source, source_id, subject_raw, subject_canonical, "
                "subject_short, title, description, assigned_at, due_at, "
                "status_raw, status, url, bronze_id, last_synced) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row,
            )


class TestDimSubjects:
    def test_table_exists_with_expected_columns(self, conn: sqlite3.Connection):
        cols = _columns(conn, "dim_subjects")
        assert set(cols) == {
            "id",
            "match_type",
            "pattern",
            "canonical",
            "short",
            "priority",
        }

    def test_match_type_check_constraint(self, conn: sqlite3.Connection):
        with pytest.raises(sqlite3.IntegrityError), conn:
            conn.execute(
                "INSERT INTO dim_subjects "
                "(match_type, pattern, canonical, short, priority) "
                "VALUES (?, ?, ?, ?, ?)",
                ("fuzzy", "9SCI", "Year 9 Science", "Sci", 50),
            )

    def test_unique_match_type_pattern(self, conn: sqlite3.Connection):
        with conn:
            conn.execute(
                "INSERT INTO dim_subjects "
                "(match_type, pattern, canonical, short, priority) "
                "VALUES (?, ?, ?, ?, ?)",
                ("prefix", "9SCI", "Year 9 Science", "Sci", 50),
            )
        with pytest.raises(sqlite3.IntegrityError), conn:
            conn.execute(
                "INSERT INTO dim_subjects "
                "(match_type, pattern, canonical, short, priority) "
                "VALUES (?, ?, ?, ?, ?)",
                ("prefix", "9SCI", "different canonical", "Sci2", 50),
            )

    def test_same_pattern_across_match_types_allowed(self, conn: sqlite3.Connection):
        # An exact rule and a prefix rule may share the same pattern string.
        with conn:
            conn.execute(
                "INSERT INTO dim_subjects "
                "(match_type, pattern, canonical, short, priority) "
                "VALUES (?, ?, ?, ?, ?)",
                ("exact", "9SCI", "Year 9 Science", "Sci", 100),
            )
            conn.execute(
                "INSERT INTO dim_subjects "
                "(match_type, pattern, canonical, short, priority) "
                "VALUES (?, ?, ?, ?, ?)",
                ("prefix", "9SCI", "Year 9 Science", "Sci", 50),
            )


class TestSilverTaskLinks:
    def test_table_exists_with_expected_columns(self, conn: sqlite3.Connection):
        cols = _columns(conn, "silver_task_links")
        assert set(cols) == {
            "id",
            "child",
            "primary_source",
            "primary_source_id",
            "secondary_source",
            "secondary_source_id",
            "confidence",
            "state",
            "score_subject",
            "score_title",
            "score_due",
            "detected_at",
        }

    def test_default_state_is_pending(self, conn: sqlite3.Connection):
        with conn:
            conn.execute(
                "INSERT INTO silver_task_links "
                "(child, primary_source, primary_source_id, "
                "secondary_source, secondary_source_id, confidence, detected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    "james",
                    "compass",
                    "1",
                    "classroom",
                    "abc",
                    "auto_high",
                    "2026-04-26T00:00:00+00:00",
                ),
            )
        state = conn.execute("SELECT state FROM silver_task_links").fetchone()[0]
        assert state == "pending"

    def test_confidence_check_constraint(self, conn: sqlite3.Connection):
        with pytest.raises(sqlite3.IntegrityError), conn:
            conn.execute(
                "INSERT INTO silver_task_links "
                "(child, primary_source, primary_source_id, "
                "secondary_source, secondary_source_id, confidence, detected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("james", "compass", "1", "classroom", "abc", "guess", "2026-04-26T00:00:00+00:00"),
            )

    def test_state_check_constraint(self, conn: sqlite3.Connection):
        with pytest.raises(sqlite3.IntegrityError), conn:
            conn.execute(
                "INSERT INTO silver_task_links "
                "(child, primary_source, primary_source_id, "
                "secondary_source, secondary_source_id, confidence, "
                "state, detected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "james",
                    "compass",
                    "1",
                    "classroom",
                    "abc",
                    "auto_high",
                    "maybe",
                    "2026-04-26T00:00:00+00:00",
                ),
            )

    def test_unique_pair_per_child(self, conn: sqlite3.Connection):
        row = (
            "james",
            "compass",
            "1",
            "classroom",
            "abc",
            "auto_high",
            "2026-04-26T00:00:00+00:00",
        )
        with conn:
            conn.execute(
                "INSERT INTO silver_task_links "
                "(child, primary_source, primary_source_id, "
                "secondary_source, secondary_source_id, confidence, detected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                row,
            )
        with pytest.raises(sqlite3.IntegrityError), conn:
            conn.execute(
                "INSERT INTO silver_task_links "
                "(child, primary_source, primary_source_id, "
                "secondary_source, secondary_source_id, confidence, detected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                row,
            )


class TestSyncRuns:
    def test_table_exists_with_expected_columns(self, conn: sqlite3.Connection):
        cols = _columns(conn, "sync_runs")
        assert set(cols) == {
            "id",
            "started_at",
            "finished_at",
            "child",
            "source",
            "outcome",
            "bronze_inserted",
            "silver_upserted",
            "error",
        }

    def test_default_counters_zero(self, conn: sqlite3.Connection):
        with conn:
            conn.execute(
                "INSERT INTO sync_runs (started_at, child, source, outcome) " "VALUES (?, ?, ?, ?)",
                ("2026-04-26T00:00:00+00:00", "james", "compass", "ok"),
            )
        row = conn.execute(
            "SELECT bronze_inserted, silver_upserted, finished_at, error FROM sync_runs"
        ).fetchone()
        assert row[0] == 0
        assert row[1] == 0
        assert row[2] is None
        assert row[3] is None


class TestSchemaCoexistence:
    """Medallion tables coexist with auth_status (M1)."""

    def test_seen_tasks_removed(self, conn: sqlite3.Connection):
        # Legacy ``seen_tasks`` ledger was dropped in the medallion tail
        # commit; bronze/silver are the system of record now.
        names = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "seen_tasks" not in names
        assert "auth_status" in names

    def test_all_medallion_tables_present(self, conn: sqlite3.Connection):
        names = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert {
            "bronze_records",
            "silver_tasks",
            "dim_subjects",
            "silver_task_links",
            "sync_runs",
        }.issubset(names)


class TestSyncRunsRecord:
    """Behaviour tests for ``StateStore.record_sync_run`` /
    ``recent_sync_runs`` (M6)."""

    def test_record_sync_run_round_trip(self, tmp_path: Path):
        from datetime import UTC, datetime, timedelta

        store = StateStore(tmp_path / "state.db")
        started = datetime.now(UTC)
        finished = started + timedelta(seconds=2)
        rid = store.record_sync_run(
            child="james",
            source="classroom",
            outcome="ok",
            started_at=started,
            finished_at=finished,
            bronze_inserted=4,
            silver_upserted=3,
        )
        assert rid > 0

        rows = store.recent_sync_runs(child="james")
        assert len(rows) == 1
        r = rows[0]
        assert r["source"] == "classroom"
        assert r["outcome"] == "ok"
        assert r["bronze_inserted"] == 4
        assert r["silver_upserted"] == 3
        assert r["error"] is None

    def test_record_sync_run_failure_stores_error(self, tmp_path: Path):
        from datetime import UTC, datetime

        store = StateStore(tmp_path / "state.db")
        store.record_sync_run(
            child="james",
            source="compass",
            outcome="auth_expired",
            started_at=datetime.now(UTC),
            error="cookie expired",
        )
        rows = store.recent_sync_runs(child="james")
        assert rows[0]["outcome"] == "auth_expired"
        assert rows[0]["error"] == "cookie expired"
        assert rows[0]["finished_at"] is None

    def test_recent_sync_runs_orders_newest_first_and_filters_child(self, tmp_path: Path):
        from datetime import UTC, datetime, timedelta

        store = StateStore(tmp_path / "state.db")
        base = datetime.now(UTC)
        store.record_sync_run(
            child="james",
            source="classroom",
            outcome="ok",
            started_at=base,
        )
        store.record_sync_run(
            child="james",
            source="compass",
            outcome="ok",
            started_at=base + timedelta(seconds=10),
        )
        store.record_sync_run(
            child="tahlia",
            source="classroom",
            outcome="ok",
            started_at=base + timedelta(seconds=20),
        )

        rows = store.recent_sync_runs(child="james")
        assert [r["source"] for r in rows] == ["compass", "classroom"]

        rows_t = store.recent_sync_runs(child="tahlia")
        assert len(rows_t) == 1
        assert rows_t[0]["source"] == "classroom"

    def test_recent_sync_runs_respects_limit(self, tmp_path: Path):
        from datetime import UTC, datetime, timedelta

        store = StateStore(tmp_path / "state.db")
        base = datetime.now(UTC)
        for i in range(5):
            store.record_sync_run(
                child="james",
                source="classroom",
                outcome="ok",
                started_at=base + timedelta(seconds=i),
            )
        rows = store.recent_sync_runs(child="james", limit=2)
        assert len(rows) == 2

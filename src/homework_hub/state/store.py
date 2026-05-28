"""Persistent state — SQLite tracking of medallion data + per-source auth status.

Tables:

* **bronze_records / silver_tasks / dim_subjects / sync_runs** — the
  medallion data plane. Append-only bronze, latest-wins silver, subject
  canonicalisation rules, operational sync ledger.
* **auth_status** — most-recent sync outcome per (child, source) so the
  ``status`` CLI command and Discord alerting can show "Compass last
  succeeded 2 hours ago, Edrolo failed 6 hours ago: auth_expired".

Pure stdlib ``sqlite3`` — no ORM. The DB lives at ``Settings.state_db``
(``/config/state.db`` in container, overridable for tests).
"""

from __future__ import annotations

import sqlite3
from contextlib import closing, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS auth_status (
    child TEXT NOT NULL,
    source TEXT NOT NULL,
    last_success_at TEXT,
    last_failure_at TEXT,
    last_failure_kind TEXT,
    last_failure_message TEXT,
    PRIMARY KEY (child, source)
);

-- ------------------------------------------------------------------ --
-- Medallion architecture (M1) — bronze / silver / dim / links / runs --
-- ------------------------------------------------------------------ --

-- Bronze: append-only raw upstream payloads. System of record for replay.
-- payload_hash is a sha256 of the canonical JSON; (child, source, source_id,
-- payload_hash) is unique so re-fetching an unchanged record is a no-op.
CREATE TABLE IF NOT EXISTS bronze_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    child TEXT NOT NULL,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    UNIQUE (child, source, source_id, payload_hash)
);
CREATE INDEX IF NOT EXISTS ix_bronze_child_source
    ON bronze_records (child, source, fetched_at);
CREATE INDEX IF NOT EXISTS ix_bronze_lookup
    ON bronze_records (child, source, source_id);

-- Silver: canonical typed tasks. One row per (child, source, source_id);
-- latest-wins on resync. The data body for the gold publish layer.
CREATE TABLE IF NOT EXISTS silver_tasks (
    child TEXT NOT NULL,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    subject_raw TEXT NOT NULL DEFAULT '',
    subject_canonical TEXT NOT NULL DEFAULT '',
    subject_short TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    assigned_at TEXT,
    due_at TEXT,
    submitted_at TEXT,
    status_raw TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    task_type TEXT NOT NULL DEFAULT 'homework',
    checkpoints_json TEXT NOT NULL DEFAULT '[]',
    url TEXT NOT NULL DEFAULT '',
    bronze_id INTEGER,
    last_synced TEXT NOT NULL,
    first_seen_at TEXT,
    last_seen_at TEXT,
    missing_streak INTEGER NOT NULL DEFAULT 0,
    archived_at TEXT,
    archived_reason TEXT,
    PRIMARY KEY (child, source, source_id),
    FOREIGN KEY (bronze_id) REFERENCES bronze_records(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS ix_silver_child_due
    ON silver_tasks (child, due_at);
CREATE INDEX IF NOT EXISTS ix_silver_subject_canonical
    ON silver_tasks (child, subject_canonical);

-- dim_subjects: subject canonicalisation lookup. Seeded from
-- config/subjects.yaml; mutable via the `subjects` CLI. Resolution
-- precedence: exact (priority 100) > prefix (50) > regex (10).
CREATE TABLE IF NOT EXISTS dim_subjects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_type TEXT NOT NULL CHECK (match_type IN ('exact', 'prefix', 'regex')),
    pattern TEXT NOT NULL,
    canonical TEXT NOT NULL,
    short TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    UNIQUE (match_type, pattern)
);
CREATE INDEX IF NOT EXISTS ix_dim_subjects_priority
    ON dim_subjects (priority DESC, match_type);

-- sync_runs: operational ledger. One row per orchestrator tick per
-- (child, source); powers Settings tab + /health.
CREATE TABLE IF NOT EXISTS sync_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    child TEXT NOT NULL,
    source TEXT NOT NULL,
    outcome TEXT NOT NULL,
    bronze_inserted INTEGER NOT NULL DEFAULT 0,
    silver_upserted INTEGER NOT NULL DEFAULT 0,
    error TEXT
);
CREATE INDEX IF NOT EXISTS ix_sync_runs_recent
    ON sync_runs (child, source, started_at DESC);
"""


@dataclass(frozen=True)
class AuthRecord:
    child: str
    source: str
    last_success_at: datetime | None
    last_failure_at: datetime | None
    last_failure_kind: str | None
    last_failure_message: str | None


class StateStore:
    """Wrapper around the homework-hub SQLite state database."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with closing(self._connect()) as conn, conn:
            conn.executescript(_SCHEMA)
            _migrate(conn)

    # ------------------------------------------------------------------ #
    # auth_status
    # ------------------------------------------------------------------ #

    def record_success(self, child: str, source: str, *, now: datetime | None = None) -> None:
        ts = (now or datetime.now(UTC)).isoformat()
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO auth_status (child, source, last_success_at)
                VALUES (?, ?, ?)
                ON CONFLICT(child, source) DO UPDATE SET
                    last_success_at = excluded.last_success_at
                """,
                (child, source, ts),
            )

    def record_failure(
        self,
        child: str,
        source: str,
        *,
        kind: str,
        message: str,
        now: datetime | None = None,
    ) -> None:
        ts = (now or datetime.now(UTC)).isoformat()
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                INSERT INTO auth_status
                    (child, source, last_failure_at, last_failure_kind,
                     last_failure_message)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(child, source) DO UPDATE SET
                    last_failure_at = excluded.last_failure_at,
                    last_failure_kind = excluded.last_failure_kind,
                    last_failure_message = excluded.last_failure_message
                """,
                (child, source, ts, kind, message),
            )

    def get_auth(self, child: str, source: str) -> AuthRecord | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM auth_status WHERE child = ? AND source = ?",
                (child, source),
            ).fetchone()
        if row is None:
            return None
        return AuthRecord(
            child=row["child"],
            source=row["source"],
            last_success_at=_parse_opt_dt(row["last_success_at"]),
            last_failure_at=_parse_opt_dt(row["last_failure_at"]),
            last_failure_kind=row["last_failure_kind"],
            last_failure_message=row["last_failure_message"],
        )

    def all_auth(self) -> list[AuthRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT * FROM auth_status ORDER BY child, source").fetchall()
        return [
            AuthRecord(
                child=r["child"],
                source=r["source"],
                last_success_at=_parse_opt_dt(r["last_success_at"]),
                last_failure_at=_parse_opt_dt(r["last_failure_at"]),
                last_failure_kind=r["last_failure_kind"],
                last_failure_message=r["last_failure_message"],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------ #
    # sync_runs
    # ------------------------------------------------------------------ #

    def record_sync_run(
        self,
        *,
        child: str,
        source: str,
        outcome: str,
        started_at: datetime,
        finished_at: datetime | None = None,
        bronze_inserted: int = 0,
        silver_upserted: int = 0,
        error: str | None = None,
    ) -> int:
        """Append a row to ``sync_runs``. Returns the new row id."""
        with closing(self._connect()) as conn, conn:
            cur = conn.execute(
                """
                INSERT INTO sync_runs
                    (started_at, finished_at, child, source, outcome,
                     bronze_inserted, silver_upserted, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    started_at.isoformat(),
                    finished_at.isoformat() if finished_at else None,
                    child,
                    source,
                    outcome,
                    bronze_inserted,
                    silver_upserted,
                    error,
                ),
            )
            return int(cur.lastrowid or 0)

    def recent_sync_runs(self, *, child: str, limit: int = 20) -> list[dict]:
        """Most-recent sync_runs rows for a child, newest first."""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT id, started_at, finished_at, child, source, outcome, "
                "bronze_inserted, silver_upserted, error "
                "FROM sync_runs WHERE child = ? "
                "ORDER BY started_at DESC LIMIT ?",
                (child, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # silver_tasks — archival / reconciliation helpers
    # ------------------------------------------------------------------ #

    def bump_last_seen(
        self,
        *,
        child: str,
        source: str,
        source_ids: list[str],
        now: datetime | None = None,
    ) -> int:
        """Set ``last_seen_at = now`` and ``missing_streak = 0`` for each
        ``(child, source, source_id)`` that came back in the current sync.
        Also clears ``archived_at`` / ``archived_reason`` when the row had
        previously been archived for ``upstream_removed`` — this is the
        recovery path when a teacher re-adds a task. Returns the number of
        rows touched."""
        if not source_ids:
            return 0
        ts = (now or datetime.now(UTC)).isoformat()
        placeholders = ",".join("?" * len(source_ids))
        with closing(self._connect()) as conn, conn:
            cur = conn.execute(
                f"UPDATE silver_tasks SET last_seen_at = ?, missing_streak = 0, "
                f"archived_at = CASE WHEN archived_reason = 'upstream_removed' "
                f"  THEN NULL ELSE archived_at END, "
                f"archived_reason = CASE WHEN archived_reason = 'upstream_removed' "
                f"  THEN NULL ELSE archived_reason END "
                f"WHERE child = ? AND source = ? AND source_id IN ({placeholders})",
                (ts, child, source, *source_ids),
            )
            return cur.rowcount

    def increment_missing_streak(
        self,
        *,
        child: str,
        source: str,
        seen_ids: list[str],
    ) -> list[dict]:
        """Increment ``missing_streak`` for every silver row in
        ``(child, source)`` whose ``source_id`` is NOT in ``seen_ids``.
        Returns the list of affected rows with their new streak value so the
        caller can decide which to archive.

        Rows whose ``source_id`` starts with ``workplan:`` are excluded:
        those rows are written by the workplan fetcher (a separate
        post-ingest hook) and would otherwise be archived by the regular
        classroom reconciler because they never appear in classroom's
        ``seen_ids`` list."""
        with closing(self._connect()) as conn, conn:
            if seen_ids:
                placeholders = ",".join("?" * len(seen_ids))
                conn.execute(
                    f"UPDATE silver_tasks SET missing_streak = missing_streak + 1 "
                    f"WHERE child = ? AND source = ? "
                    f"AND source_id NOT IN ({placeholders}) "
                    f"AND source_id NOT LIKE 'workplan:%'",
                    (child, source, *seen_ids),
                )
            else:
                conn.execute(
                    "UPDATE silver_tasks SET missing_streak = missing_streak + 1 "
                    "WHERE child = ? AND source = ? "
                    "AND source_id NOT LIKE 'workplan:%'",
                    (child, source),
                )
            rows = conn.execute(
                "SELECT source_id, missing_streak, status, archived_at "
                "FROM silver_tasks WHERE child = ? AND source = ? "
                "AND source_id NOT LIKE 'workplan:%'",
                (child, source),
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_archived(
        self,
        *,
        child: str,
        source: str,
        source_id: str,
        reason: str,
        now: datetime | None = None,
    ) -> bool:
        """Mark a silver row archived. Sets ``status = 'archived'``,
        ``archived_at = now``, ``archived_reason = reason``. Returns True if
        a row was updated."""
        ts = (now or datetime.now(UTC)).isoformat()
        with closing(self._connect()) as conn, conn:
            cur = conn.execute(
                "UPDATE silver_tasks SET status = 'archived', "
                "archived_at = ?, archived_reason = ? "
                "WHERE child = ? AND source = ? AND source_id = ?",
                (ts, reason, child, source, source_id),
            )
            return cur.rowcount > 0

    def clear_archive(self, *, child: str, source: str, source_id: str) -> bool:
        """Clear archive flags on a silver row. Status is reset to
        ``not_started`` so the silver mapper / overdue derivation can take
        over again on the next sync. Returns True if a row was updated."""
        with closing(self._connect()) as conn, conn:
            cur = conn.execute(
                "UPDATE silver_tasks SET archived_at = NULL, "
                "archived_reason = NULL, "
                "status = CASE WHEN status = 'archived' THEN 'not_started' ELSE status END "
                "WHERE child = ? AND source = ? AND source_id = ?",
                (child, source, source_id),
            )
            return cur.rowcount > 0

    def list_archived(self, *, child: str, reason: str | None = None) -> list[dict]:
        """Return all archived silver rows for a child, optionally filtered by reason."""
        sql = (
            "SELECT child, source, source_id, title, subject_raw, due_at, "
            "archived_at, archived_reason "
            "FROM silver_tasks WHERE child = ? AND archived_at IS NOT NULL"
        )
        params: list = [child]
        if reason is not None:
            sql += " AND archived_reason = ?"
            params.append(reason)
        sql += " ORDER BY archived_at DESC"
        with closing(self._connect()) as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def active_silver_for(self, *, child: str, source: str) -> list[dict]:
        """Return minimal columns of all silver rows for ``(child, source)``
        needed by reconciliation: source_id, status, due_at, first_seen_at,
        archived_at, archived_reason."""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT source_id, status, due_at, first_seen_at, "
                "archived_at, archived_reason "
                "FROM silver_tasks WHERE child = ? AND source = ?",
                (child, source),
            ).fetchall()
        return [dict(r) for r in rows]

    def silver_for_child(self, *, child: str) -> list[dict]:
        """Return minimal silver columns across all sources for a child —
        used by the age-cap sweep."""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT child, source, source_id, status, due_at, first_seen_at, "
                "archived_at FROM silver_tasks WHERE child = ?",
                (child,),
            ).fetchall()
        return [dict(r) for r in rows]


def _parse_opt_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _migrate(conn: sqlite3.Connection) -> None:
    """Apply additive column migrations for databases created before a schema
    change. Each ALTER TABLE is wrapped in a try/except so the function is
    idempotent — re-running on an up-to-date DB is a safe no-op."""
    migrations = [
        "ALTER TABLE silver_tasks ADD COLUMN task_type TEXT NOT NULL DEFAULT 'homework'",
        "ALTER TABLE silver_tasks ADD COLUMN checkpoints_json TEXT NOT NULL DEFAULT '[]'",
        # Cleansing/archival fields (added with the stale-task sweep feature).
        # All nullable / defaulted so existing rows survive the migration.
        "ALTER TABLE silver_tasks ADD COLUMN first_seen_at TEXT",
        "ALTER TABLE silver_tasks ADD COLUMN last_seen_at TEXT",
        "ALTER TABLE silver_tasks ADD COLUMN missing_streak INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE silver_tasks ADD COLUMN archived_at TEXT",
        "ALTER TABLE silver_tasks ADD COLUMN archived_reason TEXT",
    ]
    for sql in migrations:
        with suppress(sqlite3.OperationalError):
            conn.execute(sql)

    # One-shot backfill: any silver row missing first_seen_at / last_seen_at
    # inherits the existing last_synced timestamp. Safe to run on every start
    # because it only updates NULL columns.
    with suppress(sqlite3.OperationalError):
        conn.execute(
            "UPDATE silver_tasks SET first_seen_at = last_synced WHERE first_seen_at IS NULL"
        )
        conn.execute(
            "UPDATE silver_tasks SET last_seen_at = last_synced WHERE last_seen_at IS NULL"
        )

    # One-shot backfill: for any silver row that is currently in a done
    # state (submitted / graded) but has no ``submitted_at`` — typically
    # classroom / edrolo / eduperfect rows where the source doesn't
    # expose a submission timestamp — infer the completion time from
    # bronze. We pick the ``fetched_at`` of the earliest bronze record
    # whose status-relevant field looks done.
    #
    # This gives "Done this week" something to filter on for legacy
    # rows. Going forward, ``SilverWriter.upsert_many`` stamps
    # ``submitted_at`` at the moment a row transitions into a done
    # state, so this backfill is a one-time catch-up.
    #
    # Safe to re-run: only updates rows where submitted_at IS NULL.
    with suppress(sqlite3.OperationalError):
        _backfill_submitted_at_from_bronze(conn)


# Status tokens that mark a bronze payload as "done" for each source.
# Mirrors the source-side status mappers but operates on the raw JSON
# substring so the backfill stays cheap (no payload parsing).
_BRONZE_DONE_MARKERS: dict[str, tuple[str, ...]] = {
    "eduperfect": ('"progressStatus":"COMPLETE"',),
    "classroom": (
        '"state":"TURNED_IN"',
        '"state":"RETURNED"',
        '"late":true',
    ),
    "edrolo": (
        '"completion_status":"COMPLETED"',
        '"resolved_stage":"ARCHIVED"',  # edrolo auto-archives old completions
    ),
    "compass": (
        '"submissionStatus":1',  # submitted on time
        '"submissionStatus":2',  # submitted late
        '"submissionStatus":3',  # marked / returned
    ),
}


def _backfill_submitted_at_from_bronze(conn: sqlite3.Connection) -> None:
    """For every (child, source, source_id) that's silver-done with a
    NULL ``submitted_at``, look up the earliest bronze record whose
    payload contains a done-state marker and copy its ``fetched_at``
    into ``submitted_at``.

    Implementation: load candidate rows + their bronze trails into
    memory, do the marker test in Python, and write back the values
    with executemany. The done set is small (tens to low hundreds per
    child) so the memory cost is negligible compared to a per-row
    correlated subquery.
    """
    candidates = conn.execute(
        "SELECT child, source, source_id FROM silver_tasks "
        "WHERE status IN ('submitted','graded') AND submitted_at IS NULL"
    ).fetchall()
    if not candidates:
        return

    updates: list[tuple[str, str, str, str]] = []
    for row in candidates:
        source = row["source"] if isinstance(row, sqlite3.Row) else row[0]
        # Schema-flexible row access — fall back to indices when row_factory
        # isn't sqlite3.Row.
        if isinstance(row, sqlite3.Row):
            child = row["child"]
            source = row["source"]
            source_id = row["source_id"]
        else:
            child, source, source_id = row

        markers = _BRONZE_DONE_MARKERS.get(source)
        if not markers:
            continue

        bronze_trail = conn.execute(
            "SELECT fetched_at, payload_json FROM bronze_records "
            "WHERE child = ? AND source = ? AND source_id = ? "
            "ORDER BY fetched_at ASC",
            (child, source, source_id),
        ).fetchall()

        for br in bronze_trail:
            fetched_at = br["fetched_at"] if isinstance(br, sqlite3.Row) else br[0]
            payload = br["payload_json"] if isinstance(br, sqlite3.Row) else br[1]
            if any(m in payload for m in markers):
                updates.append((fetched_at, child, source, source_id))
                break

    if updates:
        conn.executemany(
            "UPDATE silver_tasks SET submitted_at = ? "
            "WHERE child = ? AND source = ? AND source_id = ? "
            "AND submitted_at IS NULL",
            updates,
        )

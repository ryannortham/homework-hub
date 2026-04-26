"""Persistent state — SQLite tracking of seen tasks and per-source auth status.

Purpose:

* ``seen_tasks`` records every task we've successfully written to the sheet so
  the orchestrator can detect which tasks are *new* on each sync (foundation
  for future Discord notifications) and detect content drift via a stable
  signature hash.
* ``auth_status`` records the most-recent sync outcome per (child, source) so
  the ``status`` CLI command and future alerting can show "Compass last
  succeeded 2 hours ago, Edrolo failed 6 hours ago: auth_expired".

Pure stdlib ``sqlite3`` — no ORM. The DB lives at ``Settings.state_db``
(``/config/state.db`` in container, overridable for tests).
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from homework_hub.models import Task

_SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_tasks (
    child TEXT NOT NULL,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    signature TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    PRIMARY KEY (child, source, source_id)
);

CREATE TABLE IF NOT EXISTS auth_status (
    child TEXT NOT NULL,
    source TEXT NOT NULL,
    last_success_at TEXT,
    last_failure_at TEXT,
    last_failure_kind TEXT,
    last_failure_message TEXT,
    PRIMARY KEY (child, source)
);
"""


def task_signature(task: Task) -> str:
    """Hash of the fields we care about for change detection.

    Stable across runs — uses ISO-Z datetimes and string status. Description
    is excluded because Edrolo/Compass sometimes return whitespace-different
    HTML on otherwise-unchanged tasks; including it would manufacture false
    'changes' on every other sync.
    """
    parts = [
        task.title,
        task.subject,
        task.due_at.isoformat() if task.due_at else "",
        task.status.value,
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class SeenRecord:
    child: str
    source: str
    source_id: str
    signature: str
    first_seen_at: datetime
    last_seen_at: datetime


@dataclass(frozen=True)
class AuthRecord:
    child: str
    source: str
    last_success_at: datetime | None
    last_failure_at: datetime | None
    last_failure_kind: str | None
    last_failure_message: str | None


@dataclass(frozen=True)
class UpsertResult:
    """Outcome of upserting a batch of tasks for a child+source.

    ``new`` and ``changed`` together let the orchestrator decide what to
    notify on (when notifications land in a later phase).
    """

    new: list[Task]
    changed: list[Task]
    unchanged: list[Task]


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

    # ------------------------------------------------------------------ #
    # seen_tasks
    # ------------------------------------------------------------------ #

    def upsert_seen(self, tasks: Iterable[Task], *, now: datetime | None = None) -> UpsertResult:
        """Record that we've seen this batch of tasks; classify each as
        new / changed / unchanged versus the last sync.
        """
        ts = (now or datetime.now(UTC)).isoformat()
        new: list[Task] = []
        changed: list[Task] = []
        unchanged: list[Task] = []

        with closing(self._connect()) as conn, conn:
            for task in tasks:
                sig = task_signature(task)
                row = conn.execute(
                    "SELECT signature FROM seen_tasks "
                    "WHERE child = ? AND source = ? AND source_id = ?",
                    (task.child, task.source.value, task.source_id),
                ).fetchone()

                if row is None:
                    new.append(task)
                    conn.execute(
                        "INSERT INTO seen_tasks "
                        "(child, source, source_id, signature, "
                        "first_seen_at, last_seen_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            task.child,
                            task.source.value,
                            task.source_id,
                            sig,
                            ts,
                            ts,
                        ),
                    )
                elif row["signature"] != sig:
                    changed.append(task)
                    conn.execute(
                        "UPDATE seen_tasks SET signature = ?, last_seen_at = ? "
                        "WHERE child = ? AND source = ? AND source_id = ?",
                        (sig, ts, task.child, task.source.value, task.source_id),
                    )
                else:
                    unchanged.append(task)
                    conn.execute(
                        "UPDATE seen_tasks SET last_seen_at = ? "
                        "WHERE child = ? AND source = ? AND source_id = ?",
                        (ts, task.child, task.source.value, task.source_id),
                    )

        return UpsertResult(new=new, changed=changed, unchanged=unchanged)

    def get_seen(self, child: str, source: str, source_id: str) -> SeenRecord | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM seen_tasks WHERE child = ? AND source = ? AND source_id = ?",
                (child, source, source_id),
            ).fetchone()
        if row is None:
            return None
        return SeenRecord(
            child=row["child"],
            source=row["source"],
            source_id=row["source_id"],
            signature=row["signature"],
            first_seen_at=datetime.fromisoformat(row["first_seen_at"]),
            last_seen_at=datetime.fromisoformat(row["last_seen_at"]),
        )

    def all_seen_for_child(self, child: str) -> list[SeenRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM seen_tasks WHERE child = ? ORDER BY first_seen_at",
                (child,),
            ).fetchall()
        return [
            SeenRecord(
                child=r["child"],
                source=r["source"],
                source_id=r["source_id"],
                signature=r["signature"],
                first_seen_at=datetime.fromisoformat(r["first_seen_at"]),
                last_seen_at=datetime.fromisoformat(r["last_seen_at"]),
            )
            for r in rows
        ]

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


def _parse_opt_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None

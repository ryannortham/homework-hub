"""Bronze ingest — write raw upstream payloads to ``bronze_records``.

The bronze layer is the system-of-record. Every successful upstream fetch
returns a list of ``RawRecord``s; ``BronzeWriter`` persists each as JSON with
a content hash. The unique ``(child, source, source_id, payload_hash)``
constraint means re-fetching unchanged data is a no-op insert.

Sources expose ``fetch_raw(child) -> list[RawRecord]``. Mapping into the
canonical ``Task`` shape is the silver layer's job (``pipeline.transform``).

Hash strategy: SHA-256 over the canonical JSON serialisation of the payload
(``sort_keys=True``, no whitespace). Stable across runs and Python versions.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from homework_hub.state.store import StateStore


@dataclass(frozen=True)
class RawRecord:
    """One raw upstream payload, pre-hash, pre-write.

    ``source_id`` is the upstream's stable identifier — same key the silver
    layer will use as part of ``silver_tasks``'s composite primary key.
    ``payload`` is JSON-serialisable; non-trivial Python objects (datetimes,
    enums) must be stringified by the caller before construction.
    """

    child: str
    source: str
    source_id: str
    payload: dict[str, Any]
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def canonical_json(self) -> str:
        """Stable JSON for hashing — sort_keys + compact separators."""
        return json.dumps(self.payload, sort_keys=True, separators=(",", ":"))

    def payload_hash(self) -> str:
        """SHA-256 of ``canonical_json()``. Hex digest (64 chars)."""
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class IngestResult:
    """Outcome of writing a batch of raw records for one (child, source).

    ``inserted`` are records new-to-bronze (either first sighting or content
    change). ``skipped`` are exact duplicates of a payload already on file.
    ``ids`` lists the bronze row id for every record in input order — fresh
    inserts get the new id, skipped records get the id of the existing row
    they collide with so the silver layer can still reference them.
    """

    inserted: int
    skipped: int
    ids: list[int]


class BronzeWriter:
    """Writes ``RawRecord``s to ``bronze_records`` with hash-skip dedup."""

    def __init__(self, store: StateStore):
        self.store = store

    def write_many(self, records: list[RawRecord]) -> IngestResult:
        """Insert each record; skip those whose (child, source, source_id,
        payload_hash) already exists. Returns the row id for every input
        record (new or pre-existing) so callers can wire silver → bronze.
        """
        inserted = 0
        skipped = 0
        ids: list[int] = []

        with closing(_connect(self.store)) as conn, conn:
            for rec in records:
                fetched_at = rec.fetched_at.astimezone(UTC).isoformat()
                payload_json = rec.canonical_json()
                payload_hash = rec.payload_hash()

                cur = conn.execute(
                    "INSERT OR IGNORE INTO bronze_records "
                    "(child, source, source_id, payload_json, "
                    "payload_hash, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        rec.child,
                        rec.source,
                        rec.source_id,
                        payload_json,
                        payload_hash,
                        fetched_at,
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
                    ids.append(int(cur.lastrowid or 0))
                else:
                    skipped += 1
                    row = conn.execute(
                        "SELECT id FROM bronze_records "
                        "WHERE child = ? AND source = ? AND source_id = ? "
                        "AND payload_hash = ?",
                        (rec.child, rec.source, rec.source_id, payload_hash),
                    ).fetchone()
                    ids.append(int(row[0]) if row else 0)

        return IngestResult(inserted=inserted, skipped=skipped, ids=ids)

    def latest_for(
        self, child: str, source: str
    ) -> list[tuple[int, str, dict[str, Any], datetime]]:
        """Return the most recent payload per source_id for (child, source).

        Used by the silver layer when re-deriving canonical rows. Returns
        a list of ``(bronze_id, source_id, payload, fetched_at)`` tuples.
        """
        with closing(_connect(self.store)) as conn:
            rows = conn.execute(
                """
                SELECT b.id, b.source_id, b.payload_json, b.fetched_at
                FROM bronze_records b
                JOIN (
                    SELECT source_id, MAX(id) AS max_id
                    FROM bronze_records
                    WHERE child = ? AND source = ?
                    GROUP BY source_id
                ) latest ON latest.max_id = b.id
                ORDER BY b.id
                """,
                (child, source),
            ).fetchall()
        return [
            (
                int(r[0]),
                str(r[1]),
                json.loads(r[2]),
                datetime.fromisoformat(r[3]),
            )
            for r in rows
        ]


def _connect(store: StateStore) -> sqlite3.Connection:
    """Open a fresh SQLite connection on the store's DB path."""
    conn = sqlite3.connect(store.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

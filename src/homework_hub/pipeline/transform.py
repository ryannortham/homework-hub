"""Silver transform — bronze JSON → canonical ``Task`` rows.

Each source has a ``bronze_to_silver_<source>`` function that takes one
bronze payload dict and returns a canonical ``Task``. The functions delegate
to the existing pure mappers in ``sources.<x>`` (M2 left those untouched);
the job here is wiring payload shape → mapper kwargs and adding the
medallion-only fields (``subject_raw``, plus the silver-only Edrolo subject
prefix extraction).

``SilverWriter.upsert_many`` writes canonical rows to ``silver_tasks``.
Latest-write-wins on ``(child, source, source_id)``. Subject canonicalisation
(``subject_canonical``, ``subject_short``) is left to the M4 resolver — until
that lands, both fields default to ``subject_raw``.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Task
from homework_hub.pipeline.subjects import SubjectResolver
from homework_hub.sources.classroom import map_classroom_card_to_task
from homework_hub.sources.compass import map_learning_task_to_task
from homework_hub.sources.edrolo import map_edrolo_task_to_task
from homework_hub.state.store import StateStore

# --------------------------------------------------------------------------- #
# Bronze → Task adapters
# --------------------------------------------------------------------------- #


def bronze_to_silver_compass(*, child: str, payload: dict[str, Any]) -> Task:
    """Convert a Compass bronze payload to a canonical ``Task``.

    Bronze stores the full ``learning_task`` dict alongside the ``subdomain``
    captured at fetch time, so the mapper has everything it needs without
    re-loading the token.
    """
    return map_learning_task_to_task(
        child=child,
        learning_task=payload["learning_task"],
        subdomain=payload["subdomain"],
    )


def bronze_to_silver_classroom(*, child: str, payload: dict[str, Any]) -> Task:
    """Convert a Classroom bronze payload to a canonical ``Task``.

    The bronze layer keeps one record per (card, view); silver dedup happens
    at the writer level (last view wins). ``today=None`` lets the mapper use
    the current date for relative due-date strings — silver re-derivation is
    always fresh.
    """
    return map_classroom_card_to_task(
        child=child,
        view=payload["view"],
        card=payload["card"],
        base_url=payload["base_url"],
    )


def bronze_to_silver_edrolo(*, child: str, payload: dict[str, Any]) -> Task:
    """Convert an Edrolo bronze payload to a canonical ``Task``.

    After the upstream mapper runs, attempt to refine the subject for tasks
    whose ``course_ids`` reference past-year enrolments and fell back to the
    generic ``"Edrolo"`` string — pull the course code prefix out of the
    title (e.g. ``"11BIO 3 - 14 Jul"`` → ``"11BIO 3"``).
    """
    task = map_edrolo_task_to_task(
        child=child,
        edrolo_task=payload["task"],
        course_titles=payload.get("course_titles") or {},
    )
    if task.subject == "Edrolo":
        prefix = extract_edrolo_subject_prefix(task.title)
        if prefix:
            return task.model_copy(update={"subject": prefix})
    return task


# Matches a leading course code like "11BIO 3", "11ENG", "9MATHS 2A".
# Two letter-or-digit clusters (the year-prefixed code and an optional
# stream/group number), separated by whitespace, before any " - " divider.
_EDROLO_PREFIX_RE = re.compile(
    r"""
    ^                            # start
    (
        \d{1,2}                  # year prefix: 9, 10, 11, 12
        [A-Za-z]{2,5}            # subject code: BIO, MATHS, ENG, …
        (?:\s+[A-Za-z0-9]+)?     # optional stream/group: " 3", " 2A"
    )
    (?:\s+-\s+|\s*$)             # delimiter or end
    """,
    re.VERBOSE,
)


def extract_edrolo_subject_prefix(title: str) -> str:
    """Pull a course-code prefix out of an Edrolo task title.

    Examples:
        "11BIO 3 - 14 Jul: Photosynthesis" -> "11BIO 3"
        "11ENG - Essay practice"           -> "11ENG"
        "Random task"                      -> ""

    Returns empty string when no recognisable prefix is present, leaving
    the caller to keep the original ``"Edrolo"`` fallback.
    """
    if not title:
        return ""
    match = _EDROLO_PREFIX_RE.match(title.strip())
    return match.group(1) if match else ""


# --------------------------------------------------------------------------- #
# SilverWriter
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class TransformResult:
    """Outcome of upserting a batch of silver rows for one (child, source).

    ``inserted`` and ``updated`` together count the rows that hit the
    database; ``unchanged`` are rows whose canonical content matched what
    was already on file.
    """

    inserted: int
    updated: int
    unchanged: int


class SilverWriter:
    """Writes canonical ``Task`` rows to ``silver_tasks`` (latest wins)."""

    def __init__(self, store: StateStore, *, resolver: SubjectResolver | None = None):
        self.store = store
        self.resolver = resolver or SubjectResolver(store)

    def upsert_many(
        self,
        rows: list[tuple[Task, int | None]],
        *,
        now: datetime | None = None,
    ) -> TransformResult:
        """Upsert a batch of (Task, bronze_id) pairs.

        ``bronze_id`` may be ``None`` when the silver row is being derived
        outside the normal pipeline (e.g. a unit test or a future ``replay``
        run that's already validated bronze separately).
        """
        ts = (now or datetime.now(UTC)).astimezone(UTC).isoformat()
        inserted = 0
        updated = 0
        unchanged = 0

        with closing(_connect(self.store)) as conn, conn:
            for task, bronze_id in rows:
                existing = conn.execute(
                    "SELECT subject_raw, subject_canonical, subject_short, "
                    "title, description, assigned_at, due_at, submitted_at, "
                    "status_raw, status, url "
                    "FROM silver_tasks "
                    "WHERE child = ? AND source = ? AND source_id = ?",
                    (task.child, task.source.value, task.source_id),
                ).fetchone()

                # Subject canonicalisation via dim_subjects (M4). When no
                # rule fires, fall back to the raw subject for both fields
                # so the gold layer always has something to display.
                subject_raw = task.subject
                match = self.resolver.resolve(subject_raw)
                if match is not None:
                    subject_canonical = match.canonical
                    subject_short = match.short
                else:
                    subject_canonical = subject_raw
                    subject_short = subject_raw

                new_row = (
                    subject_raw,
                    subject_canonical,
                    subject_short,
                    task.title,
                    task.description,
                    task.assigned_at.isoformat() if task.assigned_at else None,
                    task.due_at.isoformat() if task.due_at else None,
                    task.submitted_at.isoformat() if task.submitted_at else None,
                    task.status_raw,
                    task.status.value,
                    task.url,
                )

                if existing is None:
                    conn.execute(
                        "INSERT INTO silver_tasks "
                        "(child, source, source_id, subject_raw, "
                        "subject_canonical, subject_short, title, description, "
                        "assigned_at, due_at, submitted_at, status_raw, status, url, "
                        "bronze_id, last_synced) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            task.child,
                            task.source.value,
                            task.source_id,
                            *new_row,
                            bronze_id,
                            ts,
                        ),
                    )
                    inserted += 1
                elif tuple(existing) == new_row:
                    # No content change — still touch last_synced so the
                    # Settings tab can show a fresh timestamp.
                    conn.execute(
                        "UPDATE silver_tasks SET last_synced = ?, bronze_id = ? "
                        "WHERE child = ? AND source = ? AND source_id = ?",
                        (
                            ts,
                            bronze_id,
                            task.child,
                            task.source.value,
                            task.source_id,
                        ),
                    )
                    unchanged += 1
                else:
                    conn.execute(
                        "UPDATE silver_tasks SET "
                        "subject_raw = ?, subject_canonical = ?, "
                        "subject_short = ?, title = ?, description = ?, "
                        "assigned_at = ?, due_at = ?, submitted_at = ?, status_raw = ?, "
                        "status = ?, url = ?, bronze_id = ?, last_synced = ? "
                        "WHERE child = ? AND source = ? AND source_id = ?",
                        (
                            *new_row,
                            bronze_id,
                            ts,
                            task.child,
                            task.source.value,
                            task.source_id,
                        ),
                    )
                    updated += 1

        return TransformResult(inserted=inserted, updated=updated, unchanged=unchanged)

    def all_for_child(self, child: str) -> list[Task]:
        """Return every silver row for a child, hydrated as ``Task`` objects."""
        with closing(_connect(self.store)) as conn:
            rows = conn.execute(
                "SELECT source, source_id, subject_raw, title, description, "
                "assigned_at, due_at, submitted_at, status_raw, status, url "
                "FROM silver_tasks WHERE child = ? "
                "ORDER BY source, source_id",
                (child,),
            ).fetchall()
        return [
            Task(
                source=SourceEnum(r["source"]),
                source_id=r["source_id"],
                child=child,
                subject=r["subject_raw"],
                title=r["title"],
                description=r["description"],
                assigned_at=(
                    datetime.fromisoformat(r["assigned_at"]) if r["assigned_at"] else None
                ),
                due_at=(datetime.fromisoformat(r["due_at"]) if r["due_at"] else None),
                submitted_at=(
                    datetime.fromisoformat(r["submitted_at"]) if r["submitted_at"] else None
                ),
                status_raw=r["status_raw"],
                status=r["status"],
                url=r["url"],
            )
            for r in rows
        ]


def _connect(store: StateStore) -> sqlite3.Connection:
    conn = sqlite3.connect(store.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

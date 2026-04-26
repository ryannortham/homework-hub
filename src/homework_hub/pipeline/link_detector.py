"""Cross-source duplicate detection — populates ``silver_task_links``.

Compass↔Classroom only. Edrolo is excluded because Edrolo lessons rarely
collide with the Compass/Classroom assessment items the kids care about,
and Edrolo's noisy titles ("Lesson 14 — Photosynthesis") would generate
constant false positives.

Two-tier classification:

* ``auto_high``    — same canonical subject, due-date within ±7 days, title
                     Jaccard ≥ 0.5
* ``auto_medium``  — same canonical subject, due-date within ±14 days, title
                     Jaccard ≥ 0.3

The detector picks the highest-confidence tier that matches; pairs already
present in ``silver_task_links`` keep their existing ``state`` so kids'
checkbox decisions on the Possible Duplicates sheet survive a re-run. Score
columns are refreshed every detection so a borderline pair that drifts back
above the threshold updates in place.

Compass is always the ``primary_source`` because Compass is the school's
authoritative gradebook — Classroom typically mirrors a subset.

Tokeniser drops a small noise list (``benchmark``, ``lesson``, ``task``,
``test``, ``assessment``, ``unit``) so that "WW1 Benchmark" and "WW1" still
score 1.0 on title overlap.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime

from homework_hub.state.store import StateStore

# --------------------------------------------------------------------------- #
# Tunables
# --------------------------------------------------------------------------- #

NOISE_TOKENS: frozenset[str] = frozenset(
    {
        "benchmark",
        "lesson",
        "task",
        "test",
        "assessment",
        "unit",
        "the",
        "a",
        "an",
        "of",
        "and",
        "or",
        "to",
        "for",
    }
)

HIGH_DUE_DAYS = 7
HIGH_TITLE_JACCARD = 0.5
MEDIUM_DUE_DAYS = 14
MEDIUM_TITLE_JACCARD = 0.3

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


# --------------------------------------------------------------------------- #
# Internal row + result types
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _SilverRow:
    source: str
    source_id: str
    subject_canonical: str
    title: str
    due_at: datetime | None


@dataclass(frozen=True)
class DetectedLink:
    child: str
    primary_source: str
    primary_source_id: str
    secondary_source: str
    secondary_source_id: str
    confidence: str  # auto_high | auto_medium
    score_subject: float
    score_title: float
    score_due: int  # absolute days delta


@dataclass(frozen=True)
class DetectionResult:
    inserted: int
    updated: int
    unchanged: int

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.unchanged


# --------------------------------------------------------------------------- #
# Public detector
# --------------------------------------------------------------------------- #


class LinkDetector:
    """Scans ``silver_tasks`` for one child and writes ``silver_task_links``.

    Only auto-detected pairs (``confidence`` in ``auto_high``/``auto_medium``)
    are managed by this class. Manual links (``confidence='manual'``) are
    untouched on every run.
    """

    def __init__(self, store: StateStore):
        self.store = store

    # ------------------------------------------------------------------ #
    # Read path
    # ------------------------------------------------------------------ #

    def candidates(self, child: str) -> list[DetectedLink]:
        """Return every (compass, classroom) pair that meets the auto thresholds.

        Stateless — does not read or write ``silver_task_links``. Useful
        for the ``links detect --dry-run`` CLI verb.
        """
        compass, classroom = self._load_silver(child)
        return list(_pairs(child, compass, classroom))

    def detect(self, child: str, *, now: datetime | None = None) -> DetectionResult:
        """Detect candidates and upsert into ``silver_task_links``.

        Existing rows keep their ``state`` (so kid checkbox decisions
        survive). Score columns are refreshed in place. Returns counts.
        """
        ref = now or datetime.now(UTC)
        candidates = self.candidates(child)

        with closing(_connect(self.store)) as conn, conn:
            existing = {
                (
                    r["primary_source"],
                    r["primary_source_id"],
                    r["secondary_source"],
                    r["secondary_source_id"],
                ): {
                    "id": int(r["id"]),
                    "confidence": r["confidence"],
                    "score_subject": r["score_subject"],
                    "score_title": r["score_title"],
                    "score_due": r["score_due"],
                }
                for r in conn.execute(
                    "SELECT id, primary_source, primary_source_id, "
                    "secondary_source, secondary_source_id, confidence, "
                    "score_subject, score_title, score_due "
                    "FROM silver_task_links WHERE child = ? "
                    "AND confidence IN ('auto_high', 'auto_medium')",
                    (child,),
                ).fetchall()
            }

            inserted = updated = unchanged = 0
            seen: set[tuple[str, str, str, str]] = set()
            for cand in candidates:
                key = (
                    cand.primary_source,
                    cand.primary_source_id,
                    cand.secondary_source,
                    cand.secondary_source_id,
                )
                seen.add(key)
                prior = existing.get(key)
                if prior is None:
                    conn.execute(
                        "INSERT INTO silver_task_links "
                        "(child, primary_source, primary_source_id, "
                        "secondary_source, secondary_source_id, confidence, "
                        "state, score_subject, score_title, score_due, detected_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)",
                        (
                            child,
                            cand.primary_source,
                            cand.primary_source_id,
                            cand.secondary_source,
                            cand.secondary_source_id,
                            cand.confidence,
                            cand.score_subject,
                            cand.score_title,
                            cand.score_due,
                            ref.isoformat(),
                        ),
                    )
                    inserted += 1
                    continue

                if (
                    prior["confidence"] == cand.confidence
                    and _close(prior["score_subject"], cand.score_subject)
                    and _close(prior["score_title"], cand.score_title)
                    and prior["score_due"] == cand.score_due
                ):
                    unchanged += 1
                    continue

                conn.execute(
                    "UPDATE silver_task_links SET confidence = ?, "
                    "score_subject = ?, score_title = ?, score_due = ?, "
                    "detected_at = ? WHERE id = ?",
                    (
                        cand.confidence,
                        cand.score_subject,
                        cand.score_title,
                        cand.score_due,
                        ref.isoformat(),
                        prior["id"],
                    ),
                )
                updated += 1

            # Auto-links that no longer meet the threshold are dropped only if
            # they're still 'pending' — a kid's confirmed/dismissed decision
            # survives a transient detection miss.
            stale = [k for k in existing if k not in seen]
            for k in stale:
                conn.execute(
                    "DELETE FROM silver_task_links " "WHERE id = ? AND state = 'pending'",
                    (existing[k]["id"],),
                )

        return DetectionResult(inserted=inserted, updated=updated, unchanged=unchanged)

    def list_for_child(self, child: str) -> list[dict]:
        """Return every link row for a child for the ``links list`` CLI."""
        with closing(_connect(self.store)) as conn:
            rows = conn.execute(
                "SELECT id, primary_source, primary_source_id, "
                "secondary_source, secondary_source_id, confidence, state, "
                "score_subject, score_title, score_due, detected_at "
                "FROM silver_task_links WHERE child = ? "
                "ORDER BY state, confidence DESC, detected_at DESC",
                (child,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _load_silver(self, child: str) -> tuple[list[_SilverRow], list[_SilverRow]]:
        with closing(_connect(self.store)) as conn:
            rows = conn.execute(
                "SELECT source, source_id, subject_canonical, title, due_at "
                "FROM silver_tasks WHERE child = ? "
                "AND source IN ('compass', 'classroom')",
                (child,),
            ).fetchall()

        compass: list[_SilverRow] = []
        classroom: list[_SilverRow] = []
        for r in rows:
            row = _SilverRow(
                source=r["source"],
                source_id=r["source_id"],
                subject_canonical=r["subject_canonical"] or "",
                title=r["title"] or "",
                due_at=(datetime.fromisoformat(r["due_at"]) if r["due_at"] else None),
            )
            if row.source == "compass":
                compass.append(row)
            else:
                classroom.append(row)
        return compass, classroom


# --------------------------------------------------------------------------- #
# Pure helpers (importable for tests)
# --------------------------------------------------------------------------- #


def tokenise(text: str) -> set[str]:
    """Lowercase, alphanumeric tokens with the noise list stripped."""
    return {t.lower() for t in _TOKEN_RE.findall(text or "") if t.lower() not in NOISE_TOKENS}


def jaccard(a: str, b: str) -> float:
    """Jaccard similarity between tokenised titles. Empty-vs-empty → 0.0."""
    ta, tb = tokenise(a), tokenise(b)
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0


def classify(
    *,
    subject_match: bool,
    due_delta_days: int | None,
    title_score: float,
) -> str | None:
    """Return ``auto_high``/``auto_medium``/None for a candidate pair."""
    if not subject_match:
        return None
    if due_delta_days is None:
        return None
    if due_delta_days <= HIGH_DUE_DAYS and title_score >= HIGH_TITLE_JACCARD:
        return "auto_high"
    if due_delta_days <= MEDIUM_DUE_DAYS and title_score >= MEDIUM_TITLE_JACCARD:
        return "auto_medium"
    return None


def _pairs(
    child: str,
    compass: list[_SilverRow],
    classroom: list[_SilverRow],
):
    for c in compass:
        if not c.subject_canonical:
            continue
        for k in classroom:
            if not k.subject_canonical or k.subject_canonical != c.subject_canonical:
                continue
            if c.due_at is None or k.due_at is None:
                continue
            delta = abs((c.due_at - k.due_at).days)
            score = jaccard(c.title, k.title)
            tier = classify(
                subject_match=True,
                due_delta_days=delta,
                title_score=score,
            )
            if tier is None:
                continue
            yield DetectedLink(
                child=child,
                primary_source="compass",
                primary_source_id=c.source_id,
                secondary_source="classroom",
                secondary_source_id=k.source_id,
                confidence=tier,
                score_subject=1.0,
                score_title=round(score, 4),
                score_due=delta,
            )


def _close(a: float | None, b: float | None, *, eps: float = 1e-6) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) < eps


def _connect(store: StateStore) -> sqlite3.Connection:
    conn = sqlite3.connect(store.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

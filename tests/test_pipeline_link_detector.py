"""Tests for the cross-source duplicate detector (M9)."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from homework_hub.pipeline.link_detector import (
    LinkDetector,
    classify,
    jaccard,
    tokenise,
)
from homework_hub.state.store import StateStore


def _store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


def _insert_silver(
    store: StateStore,
    *,
    child: str,
    source: str,
    source_id: str,
    subject_canonical: str,
    title: str,
    due_at: datetime | None,
) -> None:
    with closing(sqlite3.connect(store.db_path)) as conn, conn:
        conn.execute(
            "INSERT INTO silver_tasks "
            "(child, source, source_id, subject_raw, subject_canonical, "
            "subject_short, title, status, last_synced, due_at) "
            "VALUES (?, ?, ?, '', ?, '', ?, 'not_started', ?, ?)",
            (
                child,
                source,
                source_id,
                subject_canonical,
                title,
                datetime.now(UTC).isoformat(),
                due_at.isoformat() if due_at else None,
            ),
        )


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


class TestTokenise:
    def test_drops_noise_words(self):
        assert tokenise("WW1 Benchmark") == {"ww1"}

    def test_lowercases(self):
        assert tokenise("Camp REFLECTION") == {"camp", "reflection"}

    def test_alphanumeric_split(self):
        assert tokenise("Topic 3 - Cells & Energy") == {"topic", "3", "cells", "energy"}

    def test_empty(self):
        assert tokenise("") == set()


class TestJaccard:
    def test_ww1_benchmark_vs_ww1(self):
        # "WW1 Benchmark" → {ww1}, "WW1" → {ww1}; perfect overlap.
        assert jaccard("WW1 Benchmark", "WW1") == 1.0

    def test_camp_reflection_pair(self):
        # {camp, reflection} vs {camp, reflection, final, draft} → 2/4
        assert jaccard("Camp Reflection", "Camp Reflection — final draft") == pytest.approx(
            0.5, rel=1e-3
        )

    def test_no_overlap(self):
        assert jaccard("Photosynthesis", "Algebra") == 0.0

    def test_one_empty(self):
        assert jaccard("Anything", "") == 0.0


class TestClassify:
    def test_subject_mismatch_returns_none(self):
        assert classify(subject_match=False, due_delta_days=0, title_score=1.0) is None

    def test_no_due_returns_none(self):
        assert classify(subject_match=True, due_delta_days=None, title_score=1.0) is None

    def test_high_tier(self):
        assert classify(subject_match=True, due_delta_days=3, title_score=0.6) == "auto_high"

    def test_medium_tier(self):
        assert classify(subject_match=True, due_delta_days=10, title_score=0.35) == "auto_medium"

    def test_below_thresholds(self):
        assert classify(subject_match=True, due_delta_days=20, title_score=0.6) is None
        assert classify(subject_match=True, due_delta_days=3, title_score=0.1) is None


# --------------------------------------------------------------------------- #
# Detector — end-to-end against a real SQLite store
# --------------------------------------------------------------------------- #


class TestLinkDetectorDetect:
    def test_ww1_benchmark_pair_flagged_high(self, tmp_path: Path):
        store = _store(tmp_path)
        due = datetime(2026, 5, 1, tzinfo=UTC)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C1",
            subject_canonical="Year 9 Humanities",
            title="WW1 Benchmark",
            due_at=due,
        )
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K1",
            subject_canonical="Year 9 Humanities",
            title="WW1",
            due_at=due,
        )

        detector = LinkDetector(store)
        result = detector.detect("james")
        assert result.inserted == 1

        rows = detector.list_for_child("james")
        assert len(rows) == 1
        assert rows[0]["primary_source"] == "compass"
        assert rows[0]["primary_source_id"] == "C1"
        assert rows[0]["secondary_source"] == "classroom"
        assert rows[0]["secondary_source_id"] == "K1"
        assert rows[0]["confidence"] == "auto_high"
        assert rows[0]["state"] == "pending"
        assert rows[0]["score_due"] == 0
        assert rows[0]["score_title"] == 1.0

    def test_camp_reflection_pair_flagged(self, tmp_path: Path):
        store = _store(tmp_path)
        due_c = datetime(2026, 6, 1, tzinfo=UTC)
        due_k = due_c + timedelta(days=2)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C2",
            subject_canonical="Year 9 English",
            title="Camp Reflection",
            due_at=due_c,
        )
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K2",
            subject_canonical="Year 9 English",
            title="Camp Reflection — final draft",
            due_at=due_k,
        )

        result = LinkDetector(store).detect("james")
        assert result.inserted == 1

    def test_subject_mismatch_no_link(self, tmp_path: Path):
        store = _store(tmp_path)
        due = datetime(2026, 5, 1, tzinfo=UTC)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C3",
            subject_canonical="Year 9 Humanities",
            title="WW1",
            due_at=due,
        )
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K3",
            subject_canonical="Year 9 Science",
            title="WW1",
            due_at=due,
        )

        result = LinkDetector(store).detect("james")
        assert result.inserted == 0
        assert LinkDetector(store).list_for_child("james") == []

    def test_due_delta_too_large_no_link(self, tmp_path: Path):
        store = _store(tmp_path)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C4",
            subject_canonical="Year 9 Maths",
            title="Pythagoras Practice",
            due_at=datetime(2026, 5, 1, tzinfo=UTC),
        )
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K4",
            subject_canonical="Year 9 Maths",
            title="Pythagoras Practice",
            due_at=datetime(2026, 6, 1, tzinfo=UTC),
        )
        assert LinkDetector(store).detect("james").inserted == 0

    def test_medium_tier(self, tmp_path: Path):
        store = _store(tmp_path)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C5",
            subject_canonical="Year 9 Science",
            title="Cells diagram",
            due_at=datetime(2026, 5, 1, tzinfo=UTC),
        )
        # 10 days apart, partial title overlap → medium.
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K5",
            subject_canonical="Year 9 Science",
            title="Cells worksheet",
            due_at=datetime(2026, 5, 11, tzinfo=UTC),
        )
        result = LinkDetector(store).detect("james")
        assert result.inserted == 1
        assert LinkDetector(store).list_for_child("james")[0]["confidence"] == "auto_medium"

    def test_edrolo_excluded(self, tmp_path: Path):
        store = _store(tmp_path)
        due = datetime(2026, 5, 1, tzinfo=UTC)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C6",
            subject_canonical="Year 9 Science",
            title="Photosynthesis Test",
            due_at=due,
        )
        _insert_silver(
            store,
            child="james",
            source="edrolo",
            source_id="E6",
            subject_canonical="Year 9 Science",
            title="Photosynthesis",
            due_at=due,
        )
        assert LinkDetector(store).detect("james").inserted == 0

    def test_missing_due_no_link(self, tmp_path: Path):
        store = _store(tmp_path)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C7",
            subject_canonical="Year 9 English",
            title="Essay",
            due_at=None,
        )
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K7",
            subject_canonical="Year 9 English",
            title="Essay",
            due_at=datetime(2026, 5, 1, tzinfo=UTC),
        )
        assert LinkDetector(store).detect("james").inserted == 0

    def test_empty_subject_canonical_no_link(self, tmp_path: Path):
        store = _store(tmp_path)
        due = datetime(2026, 5, 1, tzinfo=UTC)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C8",
            subject_canonical="",
            title="Mystery",
            due_at=due,
        )
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K8",
            subject_canonical="",
            title="Mystery",
            due_at=due,
        )
        assert LinkDetector(store).detect("james").inserted == 0


class TestLinkDetectorIdempotency:
    def _seed_pair(self, store: StateStore) -> None:
        due = datetime(2026, 5, 1, tzinfo=UTC)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C1",
            subject_canonical="Year 9 Humanities",
            title="WW1 Benchmark",
            due_at=due,
        )
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K1",
            subject_canonical="Year 9 Humanities",
            title="WW1",
            due_at=due,
        )

    def test_second_run_unchanged(self, tmp_path: Path):
        store = _store(tmp_path)
        self._seed_pair(store)
        det = LinkDetector(store)
        det.detect("james")
        result = det.detect("james")
        assert result.inserted == 0
        assert result.unchanged == 1
        assert result.updated == 0

    def test_state_preserved_across_runs(self, tmp_path: Path):
        store = _store(tmp_path)
        self._seed_pair(store)
        det = LinkDetector(store)
        det.detect("james")
        # Simulate kid confirming via the Possible Duplicates sheet.
        with closing(sqlite3.connect(store.db_path)) as conn, conn:
            conn.execute("UPDATE silver_task_links SET state = 'confirmed'")
        det.detect("james")
        rows = det.list_for_child("james")
        assert rows[0]["state"] == "confirmed"

    def test_pending_link_dropped_when_no_longer_matches(self, tmp_path: Path):
        store = _store(tmp_path)
        self._seed_pair(store)
        det = LinkDetector(store)
        det.detect("james")
        # Mutate Compass title so the pair drops below threshold.
        with closing(sqlite3.connect(store.db_path)) as conn, conn:
            conn.execute(
                "UPDATE silver_tasks SET title = 'Cold War Essay' "
                "WHERE source = 'compass' AND source_id = 'C1'"
            )
        det.detect("james")
        assert det.list_for_child("james") == []

    def test_confirmed_link_kept_when_no_longer_matches(self, tmp_path: Path):
        store = _store(tmp_path)
        self._seed_pair(store)
        det = LinkDetector(store)
        det.detect("james")
        with closing(sqlite3.connect(store.db_path)) as conn, conn:
            conn.execute("UPDATE silver_task_links SET state = 'confirmed'")
            conn.execute(
                "UPDATE silver_tasks SET title = 'Cold War Essay' "
                "WHERE source = 'compass' AND source_id = 'C1'"
            )
        det.detect("james")
        assert len(det.list_for_child("james")) == 1

    def test_tier_upgrade_updates_in_place(self, tmp_path: Path):
        store = _store(tmp_path)
        # Identical titles (jaccard = 1.0); tier driven purely by date delta.
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C9",
            subject_canonical="Year 9 Science",
            title="Cells worksheet",
            due_at=datetime(2026, 5, 1, tzinfo=UTC),
        )
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K9",
            subject_canonical="Year 9 Science",
            title="Cells worksheet",
            due_at=datetime(2026, 5, 11, tzinfo=UTC),  # 10 days → medium
        )
        det = LinkDetector(store)
        det.detect("james")
        assert det.list_for_child("james")[0]["confidence"] == "auto_medium"
        # Tighten the dates → upgrade to high-tier.
        with closing(sqlite3.connect(store.db_path)) as conn, conn:
            conn.execute(
                "UPDATE silver_tasks SET due_at = ? "
                "WHERE source = 'classroom' AND source_id = 'K9'",
                (datetime(2026, 5, 3, tzinfo=UTC).isoformat(),),  # 2 days → high
            )
        result = det.detect("james")
        assert result.updated == 1
        assert det.list_for_child("james")[0]["confidence"] == "auto_high"


class TestCandidatesIsStateless:
    def test_candidates_does_not_write(self, tmp_path: Path):
        store = _store(tmp_path)
        due = datetime(2026, 5, 1, tzinfo=UTC)
        _insert_silver(
            store,
            child="james",
            source="compass",
            source_id="C1",
            subject_canonical="Year 9 Humanities",
            title="WW1 Benchmark",
            due_at=due,
        )
        _insert_silver(
            store,
            child="james",
            source="classroom",
            source_id="K1",
            subject_canonical="Year 9 Humanities",
            title="WW1",
            due_at=due,
        )
        det = LinkDetector(store)
        cands = det.candidates("james")
        assert len(cands) == 1
        assert det.list_for_child("james") == []

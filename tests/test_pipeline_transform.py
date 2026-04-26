"""Tests for the silver transform layer (M3)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task
from homework_hub.pipeline.transform import (
    SilverWriter,
    bronze_to_silver_classroom,
    bronze_to_silver_compass,
    bronze_to_silver_edrolo,
    extract_edrolo_subject_prefix,
)
from homework_hub.state.store import StateStore

# --------------------------------------------------------------------------- #
# extract_edrolo_subject_prefix
# --------------------------------------------------------------------------- #


class TestEdroloSubjectPrefix:
    def test_with_date_suffix(self):
        assert extract_edrolo_subject_prefix("11BIO 3 - 14 Jul: Photosynthesis") == "11BIO 3"

    def test_without_stream_number(self):
        assert extract_edrolo_subject_prefix("11ENG - Essay practice") == "11ENG"

    def test_year_9_subject(self):
        assert extract_edrolo_subject_prefix("9MATH 2 - Pythagoras") == "9MATH 2"

    def test_alphanumeric_stream(self):
        assert extract_edrolo_subject_prefix("11CHEM 2A - Reactions") == "11CHEM 2A"

    def test_no_match_returns_empty(self):
        assert extract_edrolo_subject_prefix("Random task name") == ""

    def test_empty_input(self):
        assert extract_edrolo_subject_prefix("") == ""

    def test_strips_whitespace(self):
        assert extract_edrolo_subject_prefix("  11BIO 3 - whatever") == "11BIO 3"

    def test_lowercase_subject_still_matches(self):
        # Edrolo titles are case-mixed in the wild; be tolerant.
        assert extract_edrolo_subject_prefix("11bio 3 - whatever") == "11bio 3"


# --------------------------------------------------------------------------- #
# Bronze → Task adapters
# --------------------------------------------------------------------------- #


class TestBronzeToSilverCompass:
    def test_maps_via_existing_compass_mapper(self):
        payload = {
            "learning_task": {
                "id": 8842,
                "name": "Pythagoras Investigation",
                "subjectName": "9MATH",
                "description": "<p>Do it</p>",
                "students": [{"submissionStatus": 0}],
            },
            "subdomain": "mcsc-vic",
        }
        task = bronze_to_silver_compass(child="james", payload=payload)
        assert task.source is SourceEnum.COMPASS
        assert task.source_id == "8842"
        assert task.subject == "9MATH"
        assert task.title == "Pythagoras Investigation"
        assert "<p>" not in task.description


class TestBronzeToSilverClassroom:
    def test_maps_via_existing_classroom_mapper(self):
        payload = {
            "card": {
                "course_id": "C1",
                "stream_item_id": "S1",
                "title": "Essay 1",
                "subject": "Year 11 English",
                "due_or_status": "Due tomorrow, 11:59 PM",
                "href": "/u/0/c/C1/a/S1/details",
            },
            "view": "assigned",
            "base_url": "https://classroom.google.com",
        }
        task = bronze_to_silver_classroom(child="tahlia", payload=payload)
        assert task.source is SourceEnum.CLASSROOM
        assert task.source_id == "C1:S1"
        assert task.title == "Essay 1"


class TestBronzeToSilverEdrolo:
    def _payload(self, **task_overrides):
        task = {
            "id": 99821,
            "title": "Photosynthesis revision",
            "type": "spaced_retrieval",
            "course_ids": [66921],
            "task_assignments": [],
            **task_overrides,
        }
        return {"task": task, "course_titles": {"66921": "VCE Biology Units 3&4 [2026]"}}

    def test_uses_course_title_when_available(self):
        task = bronze_to_silver_edrolo(child="tahlia", payload=self._payload())
        assert task.subject == "VCE Biology Units 3&4 [2026]"

    def test_falls_back_to_title_prefix_when_course_unknown(self):
        # Past-year course_ids land on the "Edrolo" fallback in the upstream
        # mapper; the silver layer rescues the subject from the title prefix.
        payload = {
            "task": {
                "id": 99822,
                "title": "11BIO 3 - 14 Jul: Cell signalling",
                "type": "created",
                "course_ids": [9999],  # not in course_titles
                "task_assignments": [],
            },
            "course_titles": {},
        }
        task = bronze_to_silver_edrolo(child="tahlia", payload=payload)
        assert task.subject == "11BIO 3"

    def test_keeps_edrolo_when_no_prefix_extractable(self):
        payload = {
            "task": {
                "id": 99823,
                "title": "Generic study task",
                "type": "created",
                "course_ids": [9999],
                "task_assignments": [],
            },
            "course_titles": {},
        }
        task = bronze_to_silver_edrolo(child="tahlia", payload=payload)
        assert task.subject == "Edrolo"


# --------------------------------------------------------------------------- #
# SilverWriter
# --------------------------------------------------------------------------- #


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
    source_id: str = "1",
    subject: str = "9MATH",
    title: str = "Algebra Q1-5",
    status: Status = Status.NOT_STARTED,
    due: datetime | None = None,
) -> Task:
    return Task(
        source=source,
        source_id=source_id,
        child=child,
        subject=subject,
        title=title,
        due_at=due or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        status=status,
    )


class TestSilverWriterUpsert:
    def test_first_write_is_insert(self, writer: SilverWriter):
        result = writer.upsert_many([(_task(source_id="1"), None)])
        assert result.inserted == 1
        assert result.updated == 0
        assert result.unchanged == 0

    def test_identical_resync_is_unchanged(self, writer: SilverWriter):
        writer.upsert_many([(_task(source_id="1"), None)])
        result = writer.upsert_many([(_task(source_id="1"), None)])
        assert result.inserted == 0
        assert result.updated == 0
        assert result.unchanged == 1

    def test_status_change_is_update(self, writer: SilverWriter):
        writer.upsert_many([(_task(source_id="1", status=Status.NOT_STARTED), None)])
        result = writer.upsert_many([(_task(source_id="1", status=Status.SUBMITTED), None)])
        assert result.updated == 1

    def test_due_change_is_update(self, writer: SilverWriter):
        writer.upsert_many([(_task(source_id="1", due=datetime(2026, 5, 1, tzinfo=UTC)), None)])
        result = writer.upsert_many(
            [(_task(source_id="1", due=datetime(2026, 5, 5, tzinfo=UTC)), None)]
        )
        assert result.updated == 1

    def test_pk_separation_between_children(self, writer: SilverWriter):
        writer.upsert_many([(_task(child="james", source_id="1"), None)])
        result = writer.upsert_many([(_task(child="tahlia", source_id="1"), None)])
        assert result.inserted == 1

    def test_pk_separation_between_sources(self, writer: SilverWriter):
        writer.upsert_many([(_task(source=SourceEnum.COMPASS, source_id="1"), None)])
        result = writer.upsert_many([(_task(source=SourceEnum.CLASSROOM, source_id="1"), None)])
        assert result.inserted == 1

    def test_bronze_id_persisted_when_real(self, writer: SilverWriter, store: StateStore):
        # Use a real bronze row so the FK constraint is satisfied.
        from homework_hub.pipeline.ingest import BronzeWriter, RawRecord

        bronze = BronzeWriter(store).write_many(
            [RawRecord(child="james", source="compass", source_id="1", payload={"v": 1})]
        )
        bronze_id = bronze.ids[0]
        writer.upsert_many([(_task(source_id="1"), bronze_id)])
        import sqlite3

        conn = sqlite3.connect(store.db_path)
        stored = conn.execute("SELECT bronze_id FROM silver_tasks").fetchone()[0]
        conn.close()
        assert stored == bronze_id

    def test_bronze_id_updates_on_resync(self, writer: SilverWriter, store: StateStore):
        from homework_hub.pipeline.ingest import BronzeWriter, RawRecord

        bw = BronzeWriter(store)
        first = bw.write_many(
            [RawRecord(child="james", source="compass", source_id="1", payload={"v": 1})]
        )
        writer.upsert_many([(_task(source_id="1"), first.ids[0])])
        # New bronze row (different payload).
        second = bw.write_many(
            [RawRecord(child="james", source="compass", source_id="1", payload={"v": 2})]
        )
        writer.upsert_many([(_task(source_id="1"), second.ids[0])])
        import sqlite3

        conn = sqlite3.connect(store.db_path)
        stored = conn.execute("SELECT bronze_id FROM silver_tasks").fetchone()[0]
        conn.close()
        assert stored == second.ids[0]

    def test_last_synced_is_utc_iso(self, writer: SilverWriter, store: StateStore):
        ts = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        writer.upsert_many([(_task(source_id="1"), None)], now=ts)
        import sqlite3

        conn = sqlite3.connect(store.db_path)
        ls = conn.execute("SELECT last_synced FROM silver_tasks").fetchone()[0]
        conn.close()
        assert ls.startswith("2026-04-26T12:00:00")

    def test_subject_raw_canonical_short_all_set_to_subject_pre_m4(
        self, writer: SilverWriter, store: StateStore
    ):
        # Until M4 lands the resolver, all three subject_* columns mirror
        # the raw subject so the gold layer has something to display.
        writer.upsert_many([(_task(source_id="1", subject="9MATH"), None)])
        import sqlite3

        conn = sqlite3.connect(store.db_path)
        row = conn.execute(
            "SELECT subject_raw, subject_canonical, subject_short FROM silver_tasks"
        ).fetchone()
        conn.close()
        assert row == ("9MATH", "9MATH", "9MATH")


class TestSilverWriterAllForChild:
    def test_round_trips_tasks(self, writer: SilverWriter):
        writer.upsert_many(
            [
                (_task(child="james", source_id="1", title="A"), None),
                (_task(child="james", source_id="2", title="B"), None),
                (_task(child="tahlia", source_id="3", title="C"), None),
            ]
        )
        james = writer.all_for_child("james")
        assert {t.title for t in james} == {"A", "B"}
        assert all(isinstance(t, Task) for t in james)

    def test_returns_empty_for_unknown_child(self, writer: SilverWriter):
        assert writer.all_for_child("nobody") == []

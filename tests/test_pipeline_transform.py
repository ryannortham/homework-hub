"""Tests for the silver transform layer (M3)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task, TaskType
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
                "students": [{"userId": 12345, "submissionStatus": 0}],
            },
            "subdomain": "mcsc-vic",
            "student_user_id": 12345,
        }
        task = bronze_to_silver_compass(child="james", payload=payload)
        assert task.source is SourceEnum.COMPASS
        assert task.source_id == "8842"
        assert task.subject == "9MATH"
        assert task.title == "Pythagoras Investigation"
        assert "<p>" not in task.description
        assert task.url.endswith("/Records/UserNew.aspx?userId=12345#learningTasks")


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
    submitted_at: datetime | None = None,
) -> Task:
    return Task(
        source=source,
        source_id=source_id,
        child=child,
        subject=subject,
        title=title,
        due_at=due or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        status=status,
        submitted_at=submitted_at,
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

    def test_submitted_at_persisted_and_round_trips(self, writer: SilverWriter, store: StateStore):
        ts = datetime(2026, 4, 20, 9, 0, 0, tzinfo=UTC)
        writer.upsert_many([(_task(source_id="1", submitted_at=ts), None)])
        import sqlite3

        conn = sqlite3.connect(store.db_path)
        val = conn.execute("SELECT submitted_at FROM silver_tasks").fetchone()[0]
        conn.close()
        assert val is not None
        assert val.startswith("2026-04-20T09:00:00")

    def test_submitted_at_none_stored_as_null(self, writer: SilverWriter, store: StateStore):
        writer.upsert_many([(_task(source_id="1", submitted_at=None), None)])
        import sqlite3

        conn = sqlite3.connect(store.db_path)
        val = conn.execute("SELECT submitted_at FROM silver_tasks").fetchone()[0]
        conn.close()
        assert val is None

    def test_submitted_at_change_triggers_update(self, writer: SilverWriter):
        writer.upsert_many([(_task(source_id="1", submitted_at=None), None)])
        ts = datetime(2026, 4, 21, 10, 0, 0, tzinfo=UTC)
        result = writer.upsert_many([(_task(source_id="1", submitted_at=ts), None)])
        assert result.updated == 1

    def test_submitted_at_synthesised_on_insert_when_status_done(
        self, writer: SilverWriter, store: StateStore
    ):
        """Sources like classroom / edrolo / eduperfect don't emit a
        submission timestamp. The writer stamps ``submitted_at`` on
        insert when the row arrives in a done state so the Dashboard's
        'Done this week' has something to filter on."""
        import sqlite3

        now = datetime(2026, 5, 28, 9, 0, 0, tzinfo=UTC)
        writer.upsert_many(
            [(_task(source_id="1", status=Status.SUBMITTED, submitted_at=None), None)],
            now=now,
        )
        conn = sqlite3.connect(store.db_path)
        val = conn.execute("SELECT submitted_at FROM silver_tasks").fetchone()[0]
        conn.close()
        assert val == now.isoformat()

    def test_submitted_at_not_synthesised_for_non_done(
        self, writer: SilverWriter, store: StateStore
    ):
        import sqlite3

        writer.upsert_many(
            [(_task(source_id="1", status=Status.NOT_STARTED, submitted_at=None), None)]
        )
        conn = sqlite3.connect(store.db_path)
        val = conn.execute("SELECT submitted_at FROM silver_tasks").fetchone()[0]
        conn.close()
        assert val is None

    def test_submitted_at_stamped_on_transition_to_done(
        self, writer: SilverWriter, store: StateStore
    ):
        """Existing non-done row → done. ``submitted_at`` is stamped
        even though the source provides no value."""
        import sqlite3

        writer.upsert_many(
            [(_task(source_id="1", status=Status.NOT_STARTED, submitted_at=None), None)]
        )
        now = datetime(2026, 5, 28, 9, 0, 0, tzinfo=UTC)
        writer.upsert_many(
            [(_task(source_id="1", status=Status.SUBMITTED, submitted_at=None), None)],
            now=now,
        )
        conn = sqlite3.connect(store.db_path)
        val = conn.execute("SELECT submitted_at FROM silver_tasks").fetchone()[0]
        conn.close()
        assert val == now.isoformat()

    def test_submitted_at_preserved_across_done_resync(
        self, writer: SilverWriter, store: StateStore
    ):
        """A second sync of an already-done row with no source-provided
        ``submitted_at`` must preserve the originally-stamped value, not
        bump it to the new sync time."""
        import sqlite3

        first = datetime(2026, 5, 20, 9, 0, 0, tzinfo=UTC)
        writer.upsert_many(
            [(_task(source_id="1", status=Status.SUBMITTED, submitted_at=None), None)],
            now=first,
        )
        later = datetime(2026, 5, 28, 9, 0, 0, tzinfo=UTC)
        writer.upsert_many(
            [(_task(source_id="1", status=Status.SUBMITTED, submitted_at=None), None)],
            now=later,
        )
        conn = sqlite3.connect(store.db_path)
        val = conn.execute("SELECT submitted_at FROM silver_tasks").fetchone()[0]
        conn.close()
        assert val == first.isoformat()

    def test_submitted_at_cleared_on_transition_off_done(
        self, writer: SilverWriter, store: StateStore
    ):
        """If a source flips the status back from done (e.g. teacher
        un-submits in Classroom), the stale stamp is cleared so the
        next transition re-records the true completion time."""
        import sqlite3

        writer.upsert_many(
            [(_task(source_id="1", status=Status.SUBMITTED, submitted_at=None), None)],
            now=datetime(2026, 5, 20, 9, 0, 0, tzinfo=UTC),
        )
        writer.upsert_many(
            [(_task(source_id="1", status=Status.IN_PROGRESS, submitted_at=None), None)]
        )
        conn = sqlite3.connect(store.db_path)
        val = conn.execute("SELECT submitted_at FROM silver_tasks").fetchone()[0]
        conn.close()
        assert val is None

    def test_source_provided_submitted_at_wins_over_synthesis(
        self, writer: SilverWriter, store: StateStore
    ):
        """Compass supplies a real submission timestamp; we must not
        clobber it with the sync-time stamp."""
        import sqlite3

        source_ts = datetime(2026, 5, 15, 14, 30, 0, tzinfo=UTC)
        writer.upsert_many(
            [(_task(source_id="1", status=Status.SUBMITTED, submitted_at=source_ts), None)],
            now=datetime(2026, 5, 28, 9, 0, 0, tzinfo=UTC),
        )
        conn = sqlite3.connect(store.db_path)
        val = conn.execute("SELECT submitted_at FROM silver_tasks").fetchone()[0]
        conn.close()
        assert val == source_ts.isoformat()

    def test_task_type_persisted(self, writer: SilverWriter, store: StateStore):
        t = Task(
            source=SourceEnum.COMPASS,
            source_id="1",
            child="james",
            subject="11BIO",
            title="SAC 1",
            task_type=TaskType.ASSESSMENT,
        )
        writer.upsert_many([(t, None)])
        import sqlite3

        conn = sqlite3.connect(store.db_path)
        val = conn.execute("SELECT task_type FROM silver_tasks").fetchone()[0]
        conn.close()
        assert val == "assessment"

    def test_checkpoints_json_persisted_and_round_trips(self, writer: SilverWriter):
        checkpoints = [{"id": 101, "name": "Part A"}, {"id": 102, "name": "Part B"}]
        t = Task(
            source=SourceEnum.COMPASS,
            source_id="1",
            child="james",
            subject="11MAM",
            title="Chapter 1",
            checkpoints=checkpoints,
        )
        writer.upsert_many([(t, None)])
        tasks = writer.all_for_child("james")
        assert len(tasks) == 1
        assert tasks[0].checkpoints == checkpoints

    def test_empty_checkpoints_round_trips(self, writer: SilverWriter):
        writer.upsert_many([(_task(source_id="1"), None)])
        tasks = writer.all_for_child("james")
        assert tasks[0].checkpoints == []

    def test_task_type_change_triggers_update(self, writer: SilverWriter):
        t1 = Task(
            source=SourceEnum.COMPASS,
            source_id="1",
            child="james",
            subject="9MATH",
            title="Task",
            task_type=TaskType.HOMEWORK,
        )
        t2 = t1.model_copy(update={"task_type": TaskType.ASSESSMENT})
        writer.upsert_many([(t1, None)])
        result = writer.upsert_many([(t2, None)])
        assert result.updated == 1


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

    def test_submitted_at_survives_round_trip(self, writer: SilverWriter):
        ts = datetime(2026, 4, 20, 9, 0, 0, tzinfo=UTC)
        writer.upsert_many([(_task(child="james", source_id="1", submitted_at=ts), None)])
        tasks = writer.all_for_child("james")
        assert len(tasks) == 1
        assert tasks[0].submitted_at == ts


# --------------------------------------------------------------------------- #
# SilverWriter — archival / first_seen_at / recovery
# --------------------------------------------------------------------------- #


class TestSilverWriterArchival:
    def test_first_seen_at_set_on_insert(self, writer: SilverWriter, store: StateStore):
        ts = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        writer.upsert_many([(_task(source_id="A"), None)], now=ts)
        tasks = writer.all_for_child("james")
        assert tasks[0].first_seen_at == ts
        assert tasks[0].last_seen_at == ts

    def test_first_seen_at_stable_across_resyncs(self, writer: SilverWriter, store: StateStore):
        ts1 = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        ts2 = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
        writer.upsert_many([(_task(source_id="A"), None)], now=ts1)
        writer.upsert_many([(_task(source_id="A"), None)], now=ts2)
        tasks = writer.all_for_child("james")
        assert tasks[0].first_seen_at == ts1
        assert tasks[0].last_seen_at == ts2

    def test_resync_clears_upstream_removed_archive(self, writer: SilverWriter, store: StateStore):
        """An archived-for-upstream_removed row that reappears in the next
        sync should have its archive flags cleared (recovery path)."""
        writer.upsert_many([(_task(source_id="A"), None)])
        store.mark_archived(
            child="james",
            source="compass",
            source_id="A",
            reason="upstream_removed",
        )
        assert len(store.list_archived(child="james")) == 1
        # Re-sync the same task with a content change so the update path runs.
        writer.upsert_many([(_task(source_id="A", subject="9SCI"), None)])
        assert store.list_archived(child="james") == []

    def test_age_cap_archive_survives_resync(self, writer: SilverWriter, store: StateStore):
        """``age_cap`` (and ``manual``) archives must NOT be cleared by a
        resync — only ``upstream_removed`` is recoverable that way."""
        writer.upsert_many([(_task(source_id="A"), None)])
        store.mark_archived(child="james", source="compass", source_id="A", reason="age_cap")
        # Resync with a content change — silver writer must short-circuit
        # and refuse to overwrite the archived row's content.
        writer.upsert_many([(_task(source_id="A", subject="9SCI"), None)])
        rows = store.list_archived(child="james")
        assert len(rows) == 1
        assert rows[0]["archived_reason"] == "age_cap"

    def test_missing_streak_not_reset_by_writer(self, writer: SilverWriter, store: StateStore):
        """The silver writer must NOT reset ``missing_streak`` on rewrite.

        Why: the transform stage replays the bronze back-catalogue every
        sync, so resetting the streak from re-derivation would mask
        upstream disappearance. The streak is owned by
        ``reconcile_stale`` (which sees the actual freshly-fetched ids
        from this sync) and ``bump_last_seen`` (called by reconcile)."""
        writer.upsert_many([(_task(source_id="A"), None)])
        store.increment_missing_streak(child="james", source="compass", seen_ids=[])
        # Rewrite with a content change — streak must persist.
        writer.upsert_many([(_task(source_id="A", subject="9SCI"), None)])
        import sqlite3

        with sqlite3.connect(store.db_path) as conn:
            (streak,) = conn.execute(
                "SELECT missing_streak FROM silver_tasks WHERE source_id = 'A'"
            ).fetchone()
        assert streak == 1


# --------------------------------------------------------------------------- #
# One-shot bronze → silver.submitted_at backfill (in StateStore._migrate)
# --------------------------------------------------------------------------- #


class TestSubmittedAtBackfillFromBronze:
    """When the medallion db pre-dates the transition-stamp logic, many
    silver done rows have ``submitted_at IS NULL``. ``_migrate`` walks
    each one's bronze trail to find the earliest ``fetched_at`` whose
    payload contains a source-specific done marker and uses that as the
    historical completion time."""

    def _seed_silver_done(
        self, store: StateStore, *, source: str, source_id: str, status_raw: str = "complete"
    ) -> None:
        import sqlite3

        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                "INSERT INTO silver_tasks (child, source, source_id, title, status, "
                "status_raw, last_synced) VALUES (?,?,?,?,?,?,?)",
                (
                    "james",
                    source,
                    source_id,
                    "T",
                    "submitted",
                    status_raw,
                    "2026-05-28T00:00:00+00:00",
                ),
            )

    def _seed_bronze(
        self, store: StateStore, *, source: str, source_id: str, fetched_at: str, payload: str
    ) -> None:
        import sqlite3

        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                "INSERT INTO bronze_records (child, source, source_id, payload_json, "
                "payload_hash, fetched_at) VALUES (?,?,?,?,?,?)",
                ("james", source, source_id, payload, f"{source_id}-{fetched_at}", fetched_at),
            )

    def _run_migrate(self, store: StateStore) -> None:
        import sqlite3

        from homework_hub.state.store import _migrate

        with sqlite3.connect(store.db_path) as conn:
            conn.row_factory = sqlite3.Row
            _migrate(conn)
            conn.commit()

    def _read_submitted(self, store: StateStore, source_id: str) -> str | None:
        import sqlite3

        with sqlite3.connect(store.db_path) as conn:
            row = conn.execute(
                "SELECT submitted_at FROM silver_tasks WHERE source_id = ?",
                (source_id,),
            ).fetchone()
        return row[0] if row else None

    def test_eduperfect_first_complete_payload_wins(self, store: StateStore):
        self._seed_silver_done(store, source="eduperfect", source_id="100")
        self._seed_bronze(
            store,
            source="eduperfect",
            source_id="100",
            fetched_at="2026-05-20T00:00:00+00:00",
            payload='{"progressStatus":"IN_PROGRESS"}',
        )
        self._seed_bronze(
            store,
            source="eduperfect",
            source_id="100",
            fetched_at="2026-05-22T10:00:00+00:00",
            payload='{"progressStatus":"COMPLETE"}',
        )
        self._seed_bronze(
            store,
            source="eduperfect",
            source_id="100",
            fetched_at="2026-05-28T00:00:00+00:00",
            payload='{"progressStatus":"COMPLETE"}',
        )
        self._run_migrate(store)
        assert self._read_submitted(store, "100") == "2026-05-22T10:00:00+00:00"

    def test_compass_submission_status_marker(self, store: StateStore):
        self._seed_silver_done(store, source="compass", source_id="200")
        self._seed_bronze(
            store,
            source="compass",
            source_id="200",
            fetched_at="2026-05-10T00:00:00+00:00",
            payload='{"submissionStatus":0}',
        )
        self._seed_bronze(
            store,
            source="compass",
            source_id="200",
            fetched_at="2026-05-15T09:00:00+00:00",
            payload='{"submissionStatus":1,"submittedTimestamp":"2026-05-15T09:00:00"}',
        )
        self._run_migrate(store)
        assert self._read_submitted(store, "200") == "2026-05-15T09:00:00+00:00"

    def test_edrolo_resolved_stage_archived(self, store: StateStore):
        self._seed_silver_done(
            store, source="edrolo", source_id="300", status_raw="archived"
        )
        self._seed_bronze(
            store,
            source="edrolo",
            source_id="300",
            fetched_at="2026-03-01T00:00:00+00:00",
            payload='{"task":{"resolved_stage":"OPEN"}}',
        )
        self._seed_bronze(
            store,
            source="edrolo",
            source_id="300",
            fetched_at="2026-04-15T00:00:00+00:00",
            payload='{"task":{"resolved_stage":"ARCHIVED"}}',
        )
        self._run_migrate(store)
        assert self._read_submitted(store, "300") == "2026-04-15T00:00:00+00:00"

    def test_no_matching_marker_leaves_null(self, store: StateStore):
        self._seed_silver_done(store, source="eduperfect", source_id="400")
        self._seed_bronze(
            store,
            source="eduperfect",
            source_id="400",
            fetched_at="2026-05-01T00:00:00+00:00",
            payload='{"progressStatus":"IN_PROGRESS"}',
        )
        self._run_migrate(store)
        assert self._read_submitted(store, "400") is None

    def test_does_not_overwrite_existing_submitted_at(self, store: StateStore):
        import sqlite3

        # Seed silver with an existing submitted_at value — backfill
        # must leave it alone.
        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                "INSERT INTO silver_tasks (child, source, source_id, title, status, "
                "status_raw, submitted_at, last_synced) VALUES (?,?,?,?,?,?,?,?)",
                (
                    "james",
                    "compass",
                    "500",
                    "T",
                    "graded",
                    "graded",
                    "2026-05-01T00:00:00+00:00",
                    "2026-05-28T00:00:00+00:00",
                ),
            )
        self._seed_bronze(
            store,
            source="compass",
            source_id="500",
            fetched_at="2026-05-15T00:00:00+00:00",
            payload='{"submissionStatus":3}',
        )
        self._run_migrate(store)
        assert self._read_submitted(store, "500") == "2026-05-01T00:00:00+00:00"

    def test_non_done_rows_untouched(self, store: StateStore):
        import sqlite3

        with sqlite3.connect(store.db_path) as conn:
            conn.execute(
                "INSERT INTO silver_tasks (child, source, source_id, title, status, "
                "status_raw, last_synced) VALUES (?,?,?,?,?,?,?)",
                (
                    "james",
                    "eduperfect",
                    "600",
                    "T",
                    "not_started",
                    "not_started",
                    "2026-05-28T00:00:00+00:00",
                ),
            )
        self._seed_bronze(
            store,
            source="eduperfect",
            source_id="600",
            fetched_at="2026-05-22T00:00:00+00:00",
            payload='{"progressStatus":"COMPLETE"}',
        )
        self._run_migrate(store)
        # Row isn't in done state — backfill skips it.
        assert self._read_submitted(store, "600") is None

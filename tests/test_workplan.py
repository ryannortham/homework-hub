"""Tests for ``homework_hub.sources.workplan``.

Everything lives in one file so the whole feature can be removed by
deleting this test module alongside ``sources/workplan.py``.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from homework_hub.models import Source
from homework_hub.pipeline.transform import SilverWriter
from homework_hub.sources.workplan import (
    FormQuestion,
    MaterialPost,
    SchoolCalendar,
    Semester,
    WorkplanChildConfig,
    WorkplanFetcher,
    _build_task,
    _slug,
    course_id_to_b64,
    filter_section_questions,
    parse_form_questions,
    parse_workplan_child_config,
)
from homework_hub.state.store import StateStore

MELBOURNE = ZoneInfo("Australia/Melbourne")


# --------------------------------------------------------------------------- #
# parse_workplan_child_config
# --------------------------------------------------------------------------- #


class TestParseWorkplanChildConfig:
    def test_none_when_block_absent(self):
        assert parse_workplan_child_config(None) is None
        assert parse_workplan_child_config({}) is None

    def test_none_when_disabled(self):
        cfg = parse_workplan_child_config({"enabled": False, "course_id": "1"})
        assert cfg is None

    def test_none_when_course_id_missing(self):
        cfg = parse_workplan_child_config({"enabled": True})
        assert cfg is None

    def test_parses_full_block(self):
        cfg = parse_workplan_child_config(
            {
                "enabled": True,
                "course_id": "829769119654",
                "topic": "Student Workplan Tracker",
                "subject": "Mathematics Methods",
            }
        )
        assert cfg is not None
        assert cfg.course_id == "829769119654"
        assert cfg.topic == "Student Workplan Tracker"
        assert cfg.subject == "Mathematics Methods"


# --------------------------------------------------------------------------- #
# SchoolCalendar
# --------------------------------------------------------------------------- #


@pytest.fixture
def calendar_file(tmp_path: Path) -> Path:
    path = tmp_path / "school_calendar.yaml"
    path.write_text(
        """
years:
  2026:
    semesters:
      S1: {start: 2026-01-28, end: 2026-06-26}
      S2: {start: 2026-07-13, end: 2026-12-18}
  2027:
    semesters:
      S1: {start: 2027-01-28, end: 2027-06-25}
""".lstrip()
    )
    return path


class TestSchoolCalendar:
    def test_load_parses_semesters(self, calendar_file: Path):
        cal = SchoolCalendar.load(calendar_file)
        assert 2026 in cal.semesters_by_year
        s1 = cal.semesters_by_year[2026][0]
        assert s1.label == "S1"
        assert s1.start == date(2026, 1, 28)
        assert s1.end == date(2026, 6, 26)

    def test_missing_file_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            SchoolCalendar.load(tmp_path / "nope.yaml")

    def test_current_semester_during_term(self, calendar_file: Path):
        cal = SchoolCalendar.load(calendar_file)
        due = cal.current_semester_end(date(2026, 5, 1))
        assert due is not None
        # Melbourne 26 Jun 2026 23:59 -> UTC.
        expected = (
            datetime(2026, 6, 26, 23, 59, tzinfo=MELBOURNE).astimezone(UTC)
        )
        assert due == expected

    def test_holiday_window_picks_next_semester(self, calendar_file: Path):
        cal = SchoolCalendar.load(calendar_file)
        # 1 Jul 2026 is between S1 (ends 26 Jun) and S2 (starts 13 Jul).
        due = cal.current_semester_end(date(2026, 7, 1))
        assert due is not None
        assert due.astimezone(MELBOURNE).date() == date(2026, 12, 18)

    def test_returns_none_for_undefined_year(self, calendar_file: Path):
        cal = SchoolCalendar.load(calendar_file)
        assert cal.current_semester_end(date(2030, 1, 1)) is None


# --------------------------------------------------------------------------- #
# course_id_to_b64
# --------------------------------------------------------------------------- #


def test_course_id_to_b64_strips_padding():
    # Spike-verified: 829769119654 -> "ODI5NzY5MTE5NjU0"
    assert course_id_to_b64("829769119654") == "ODI5NzY5MTE5NjU0"


# --------------------------------------------------------------------------- #
# _slug
# --------------------------------------------------------------------------- #


class TestSlug:
    def test_basic(self):
        assert _slug("7A Translations") == "7a-translations"

    def test_strips_diacritics(self):
        assert _slug("Café Society") == "cafe-society"

    def test_collapses_punctuation(self):
        assert _slug("Ch.7 — Section 1!") == "ch-7-section-1"

    def test_empty_falls_back(self):
        assert _slug("***") == "section"


# --------------------------------------------------------------------------- #
# parse_form_questions
# --------------------------------------------------------------------------- #


def _build_form_html(questions: list[list]) -> str:
    """Build a minimal HTML page with a FB_PUBLIC_LOAD_DATA_ blob."""
    import json as _json

    payload = [None, [None, questions, None, None]]
    return (
        "<html><head></head><body>"
        f"<script>var FB_PUBLIC_LOAD_DATA_ = {_json.dumps(payload)};</script>"
        "</body></html>"
    )


class TestParseFormQuestions:
    def test_extracts_titles_and_types(self):
        html = _build_form_html(
            [
                [0, "Student Name", None, 0],
                [1, "7A Translations", None, 2],
                [2, "7B Reflections", None, 2],
            ]
        )
        out = parse_form_questions(html)
        assert [(q.title, q.qtype) for q in out] == [
            ("Student Name", 0),
            ("7A Translations", 2),
            ("7B Reflections", 2),
        ]

    def test_missing_var_raises(self):
        from homework_hub.sources.base import SchemaBreakError

        with pytest.raises(SchemaBreakError):
            parse_form_questions("<html></html>")

    def test_skips_malformed_question_rows(self):
        html = _build_form_html(
            [
                [0, "Valid", None, 2],
                "garbage",
                [0, "Short"],  # too short
                [0, "Bad title type", None, "not-an-int"],
                [0, 12345, None, 2],  # title not a string
            ]
        )
        out = parse_form_questions(html)
        assert [q.title for q in out] == ["Valid"]


# --------------------------------------------------------------------------- #
# filter_section_questions
# --------------------------------------------------------------------------- #


class TestFilterSectionQuestions:
    def test_drops_identity_fields(self):
        qs = [
            FormQuestion("Student Name", 0),
            FormQuestion("Email address", 0),
            FormQuestion("Class", 0),
            FormQuestion("7A Translations", 2),
            FormQuestion("7B Reflections", 2),
        ]
        kept = filter_section_questions(qs)
        assert [q.title for q in kept] == ["7A Translations", "7B Reflections"]

    def test_drops_unsupported_types(self):
        qs = [
            FormQuestion("7A Translations", 2),
            FormQuestion("Page break", 8),
            FormQuestion("Section title", 6),
            FormQuestion("Grid", 7),
        ]
        kept = filter_section_questions(qs)
        assert [q.title for q in kept] == ["7A Translations"]

    def test_drops_blank_titles(self):
        kept = filter_section_questions([FormQuestion("", 2)])
        assert kept == []


# --------------------------------------------------------------------------- #
# _build_task
# --------------------------------------------------------------------------- #


def test_build_task_uses_classroom_source_with_workplan_prefix():
    cfg = WorkplanChildConfig(
        enabled=True,
        course_id="829769119654",
        topic="Student Workplan Tracker",
        subject="Mathematics Methods",
    )
    material = MaterialPost(
        stream_item_id="863360111553",
        title="Chapter 7 Workplan",
        form_url="https://docs.google.com/forms/d/e/abc/viewform",
    )
    due = datetime(2026, 6, 26, 23, 59, tzinfo=UTC)
    task = _build_task(
        child="tahlia",
        cfg=cfg,
        material=material,
        question=FormQuestion("7A Translations", 2),
        due_at=due,
    )
    assert task.source == Source.CLASSROOM
    assert task.source_id == "workplan:829769119654:863360111553:7a-translations"
    assert task.child == "tahlia"
    assert task.subject == "Mathematics Methods"
    assert task.title == "7A Translations"
    assert task.description == "Chapter 7 Workplan"
    assert task.due_at == due
    assert task.url == material.form_url


# --------------------------------------------------------------------------- #
# WorkplanFetcher._persist — full-replace semantics
# --------------------------------------------------------------------------- #


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


@pytest.fixture
def silver(store: StateStore) -> SilverWriter:
    return SilverWriter(store)


def _make_fetcher(store: StateStore, silver: SilverWriter) -> WorkplanFetcher:
    calendar = SchoolCalendar(
        semesters_by_year={
            2026: [Semester(label="S1", start=date(2026, 1, 28), end=date(2026, 6, 26))]
        }
    )
    return WorkplanFetcher(
        store=store,
        silver=silver,
        calendar=calendar,
        per_child={},
    )


def _task(*, child: str, source_id: str, title: str) -> object:
    from homework_hub.models import Status, Task, TaskType

    return Task(
        source=Source.CLASSROOM,
        source_id=source_id,
        child=child,
        subject="Mathematics Methods",
        title=title,
        description="Chapter 7 Workplan",
        status_raw="workplan",
        status=Status.NOT_STARTED,
        task_type=TaskType.HOMEWORK,
        url="https://example.com",
    )


def _silver_workplan_ids(store: StateStore, child: str) -> list[str]:
    with sqlite3.connect(store.db_path) as conn:
        rows = conn.execute(
            "SELECT source_id FROM silver_tasks "
            "WHERE child = ? AND source = 'classroom' AND source_id LIKE 'workplan:%' "
            "ORDER BY source_id",
            (child,),
        ).fetchall()
    return [r[0] for r in rows]


class TestPersistFullReplace:
    def test_inserts_new_tasks(self, store: StateStore, silver: SilverWriter):
        fetcher = _make_fetcher(store, silver)
        tasks = [
            _task(child="tahlia", source_id="workplan:1:m1:7a-translations", title="7A"),
            _task(child="tahlia", source_id="workplan:1:m1:7b-reflections", title="7B"),
        ]
        fetcher._persist("tahlia", tasks)
        assert _silver_workplan_ids(store, "tahlia") == [
            "workplan:1:m1:7a-translations",
            "workplan:1:m1:7b-reflections",
        ]

    def test_deletes_stale_rows_not_in_batch(
        self, store: StateStore, silver: SilverWriter
    ):
        fetcher = _make_fetcher(store, silver)
        first = [
            _task(child="tahlia", source_id="workplan:1:m1:7a", title="7A"),
            _task(child="tahlia", source_id="workplan:1:m1:7b", title="7B"),
        ]
        fetcher._persist("tahlia", first)

        # Next run: 7b removed, 7c added.
        second = [
            _task(child="tahlia", source_id="workplan:1:m1:7a", title="7A"),
            _task(child="tahlia", source_id="workplan:1:m1:7c", title="7C"),
        ]
        fetcher._persist("tahlia", second)
        assert _silver_workplan_ids(store, "tahlia") == [
            "workplan:1:m1:7a",
            "workplan:1:m1:7c",
        ]

    def test_does_not_touch_other_classroom_rows(
        self, store: StateStore, silver: SilverWriter
    ):
        # Pre-seed a non-workplan classroom row.
        from homework_hub.models import Status, Task, TaskType

        regular = Task(
            source=Source.CLASSROOM,
            source_id="abc123",
            child="tahlia",
            subject="English",
            title="Essay",
            description="",
            status_raw="ASSIGNED",
            status=Status.NOT_STARTED,
            task_type=TaskType.HOMEWORK,
            url="",
        )
        silver.upsert_many([(regular, None)])

        fetcher = _make_fetcher(store, silver)
        # Empty workplan batch — should NOT delete the regular classroom row.
        fetcher._persist("tahlia", [])

        with sqlite3.connect(store.db_path) as conn:
            ids = [
                r[0]
                for r in conn.execute(
                    "SELECT source_id FROM silver_tasks "
                    "WHERE child = ? AND source = 'classroom' ORDER BY source_id",
                    ("tahlia",),
                ).fetchall()
            ]
        assert ids == ["abc123"]

    def test_does_not_touch_other_children(self, store: StateStore, silver: SilverWriter):
        fetcher = _make_fetcher(store, silver)
        fetcher._persist(
            "james",
            [_task(child="james", source_id="workplan:9:m9:1a", title="1A")],
        )
        # Tahlia's empty run must not delete James's rows.
        fetcher._persist("tahlia", [])
        assert _silver_workplan_ids(store, "james") == ["workplan:9:m9:1a"]

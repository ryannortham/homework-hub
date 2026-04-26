"""Tests for the Classroom source — DOM scrape pipeline.

The Playwright headless flow is not unit-tested (it requires real Chromium);
instead we cover:

- ``map_classroom_card_to_task``: pure card-dict → Task mapping
- ``parse_due_text``: every observed date format
- ``_resolve_status``: view-tab + card text → Status
- ``ClassroomStorageState``: cookie validation
- ``ClassroomSource.fetch``: orchestrated dedupe across the three views,
  using an injected fake scraper

Card payloads in ``CARDS`` are real samples captured from
classroom.google.com on 2026-04-26.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status
from homework_hub.sources.base import AuthExpiredError, SchemaBreakError
from homework_hub.sources.classroom import (
    DEFAULT_TZ,
    ClassroomSource,
    ClassroomStorageState,
    ScrapeResult,
    _resolve_status,
    map_classroom_card_to_task,
    parse_due_text,
)

# Reference "today" for date-parser tests. Wednesday, 2026-04-29 — chosen so
# weekday-only inputs ("Wednesday, 23:59") have a deterministic resolution.
TODAY = date(2026, 4, 29)


# Real cards captured live (with course IDs lightly tweaked for stability).
CARD_THIS_WEEK = {
    "href": "/u/0/c/831032257639/a/860942702794/details",
    "course_id": "831032257639",
    "stream_item_id": "860942702794",
    "stream_item_type": "1",
    "title": "Yr 9 Outdoor Ed Camp reflection Benchmark Task",
    "subject": "Yr 9 Outdoor Education",
    "due_or_status": "Wednesday, 23:59",
    "icon": "assignment",
}
CARD_FULL_DATE = {
    "href": "/u/0/c/830055141853/a/832041991611/details",
    "course_id": "830055141853",
    "stream_item_id": "832041991611",
    "stream_item_type": "1",
    "title": "Lesson 3 - Ethical Debates",
    "subject": "9MEXC",
    "due_or_status": "Tuesday, 2 Dec 2025",
    "icon": "assignment",
}
CARD_NO_YEAR = {
    "href": "/u/0/c/830700997846/a/861344997601/details",
    "course_id": "830700997846",
    "stream_item_id": "861344997601",
    "stream_item_type": "1",
    "title": "WW1 Benchmark",
    "subject": "2026 - 9HUM2",
    "due_or_status": "Wednesday 6 May",
    "icon": "assignment",
}
CARD_HANDED_IN = {
    "href": "/u/0/c/830055141853/a/831023747678/details",
    "course_id": "830055141853",
    "stream_item_id": "831023747678",
    "stream_item_type": "1",
    "title": "Front Cover for Folders",
    "subject": "9MEXC",
    "due_or_status": "Handed in Estigfend",
    "icon": "assignment",
}
CARD_DONE_LATE = {
    "href": "/u/0/c/831032257639/a/818676079820/details",
    "course_id": "831032257639",
    "stream_item_id": "818676079820",
    "stream_item_type": "1",
    "title": "Research Task on Australian Environments",
    "subject": "Yr 9 Outdoor Education",
    "due_or_status": "Handed in Done late Estigfend",
    "icon": "assignment",
}
CARD_NOT_HANDED_IN = {
    "href": "/u/0/c/747617946536/a/762251287436/details",
    "course_id": "747617946536",
    "stream_item_id": "762251287436",
    "stream_item_type": "1",
    "title": "Career Action Plan",
    "subject": "Year 9 2026",
    "due_or_status": "0 points out of a possible 100 Not handed in",
    "icon": "assignment",
}


# --------------------------------------------------------------------------- #
# parse_due_text
# --------------------------------------------------------------------------- #


class TestParseDueText:
    def test_returns_none_for_empty(self):
        assert parse_due_text("", today=TODAY) is None
        assert parse_due_text("   ", today=TODAY) is None

    def test_returns_none_for_status_string(self):
        assert parse_due_text("Handed in", today=TODAY) is None
        assert parse_due_text("Not handed in", today=TODAY) is None

    def test_time_only_resolves_to_this_week(self):
        # TODAY is Wednesday — "Wednesday, 23:59" means today 23:59 local.
        result = parse_due_text("Wednesday, 23:59", today=TODAY)
        assert result is not None
        # 23:59 Melbourne == 13:59 UTC during AEST, 12:59 UTC during AEDT.
        # April 29 is AEST (UTC+10).
        assert result == datetime(2026, 4, 29, 13, 59, tzinfo=UTC)

    def test_time_only_friday_resolves_forward(self):
        result = parse_due_text("Friday, 23:59", today=TODAY)
        assert result is not None
        assert result.date() == date(2026, 5, 1)  # Friday after Wednesday

    def test_time_only_monday_wraps_to_next_week(self):
        # Wed → next Mon (= 2026-05-04) at 09:00 Melbourne == 23:00 UTC Sunday.
        result = parse_due_text("Monday, 09:00", today=TODAY)
        assert result is not None
        assert result == datetime(2026, 5, 3, 23, 0, tzinfo=UTC)

    def test_full_date_with_year(self):
        result = parse_due_text("Tuesday, 2 Dec 2025", today=TODAY)
        assert result is not None
        # Default 23:59 local → 12:59 UTC (AEDT in December).
        assert result == datetime(2025, 12, 2, 12, 59, tzinfo=UTC)

    def test_full_date_with_explicit_time(self):
        result = parse_due_text("Tuesday, 2 Dec 2025, 09:00", today=TODAY)
        assert result is not None
        assert result == datetime(2025, 12, 1, 22, 0, tzinfo=UTC)

    def test_date_no_year_picks_next_occurrence(self):
        # 6 May is a week away — same year.
        result = parse_due_text("Wednesday 6 May", today=TODAY)
        assert result is not None
        assert result.date() == date(2026, 5, 6)

    def test_date_no_year_rolls_to_next_year_when_past(self):
        # Today 2026-04-29; "1 Mar" would have been March 2026 → past, so 2027.
        result = parse_due_text("Sunday 1 Mar", today=TODAY)
        assert result is not None
        assert result.date() == date(2027, 3, 1)

    def test_short_month_names(self):
        result = parse_due_text("Tuesday, 2 Dec 2025", today=TODAY)
        assert result is not None and result.year == 2025

    def test_garbage_returns_none(self):
        assert parse_due_text("totally not a date", today=TODAY) is None

    def test_strips_estigfend_artifact(self):
        # The translate-button label can leak in.
        result = parse_due_text("Tuesday, 2 Dec 2025 Estigfend", today=TODAY)
        assert result is not None and result.year == 2025


# --------------------------------------------------------------------------- #
# _resolve_status
# --------------------------------------------------------------------------- #


class TestResolveStatus:
    def test_handed_in_is_submitted(self):
        raw, status = _resolve_status("done", "Handed in", None)
        assert status is Status.SUBMITTED
        assert "Handed in" in raw

    def test_done_late_is_submitted(self):
        _, status = _resolve_status("done", "Handed in Done late", None)
        assert status is Status.SUBMITTED

    def test_not_handed_in_is_overdue(self):
        _, status = _resolve_status("done", "Not handed in", None)
        assert status is Status.OVERDUE

    def test_returned_is_graded(self):
        _, status = _resolve_status("done", "Returned 80/100", None)
        assert status is Status.GRADED

    def test_assigned_view_with_future_due(self):
        future = datetime(2099, 1, 1, tzinfo=UTC)
        _, status = _resolve_status("assigned", "Wednesday, 23:59", future)
        assert status is Status.NOT_STARTED

    def test_assigned_view_past_due_flips_overdue(self):
        past = datetime(2020, 1, 1, tzinfo=UTC)
        _, status = _resolve_status("assigned", "Wednesday, 23:59", past)
        assert status is Status.OVERDUE

    def test_missing_view_defaults_overdue(self):
        _, status = _resolve_status("missing", "Tuesday, 2 Dec 2025", None)
        assert status is Status.OVERDUE

    def test_done_view_defaults_submitted(self):
        _, status = _resolve_status("done", "", None)
        assert status is Status.SUBMITTED

    def test_strips_translation_artifact(self):
        raw, _ = _resolve_status("done", "Handed in Estigfend", None)
        assert "Estigfend" not in raw


# --------------------------------------------------------------------------- #
# map_classroom_card_to_task
# --------------------------------------------------------------------------- #


class TestMapping:
    def test_basic_assigned_card(self):
        t = map_classroom_card_to_task(
            child="james",
            view="assigned",
            card=CARD_FULL_DATE,
            today=TODAY,
        )
        assert t.source is SourceEnum.CLASSROOM
        assert t.child == "james"
        assert t.title == "Lesson 3 - Ethical Debates"
        assert t.subject == "9MEXC"
        assert t.url == "https://classroom.google.com/u/0/c/830055141853/a/832041991611/details"
        assert t.source_id == "830055141853:832041991611"
        # 2 Dec 2025 is past TODAY (2026-04-29) → status flips OVERDUE.
        assert t.status is Status.OVERDUE
        assert t.due_at is not None and t.due_at.year == 2025

    def test_done_card_marks_submitted(self):
        t = map_classroom_card_to_task(
            child="tahlia",
            view="done",
            card=CARD_HANDED_IN,
            today=TODAY,
        )
        assert t.status is Status.SUBMITTED
        assert t.due_at is None  # status string isn't a date

    def test_done_late_marks_submitted(self):
        t = map_classroom_card_to_task(
            child="tahlia",
            view="done",
            card=CARD_DONE_LATE,
            today=TODAY,
        )
        assert t.status is Status.SUBMITTED

    def test_not_handed_in_marks_overdue(self):
        t = map_classroom_card_to_task(
            child="tahlia",
            view="done",
            card=CARD_NOT_HANDED_IN,
            today=TODAY,
        )
        assert t.status is Status.OVERDUE

    def test_no_year_card_resolves_future(self):
        t = map_classroom_card_to_task(
            child="james",
            view="assigned",
            card=CARD_NO_YEAR,
            today=TODAY,
        )
        assert t.due_at is not None and t.due_at.year == 2026

    def test_missing_course_id_raises(self):
        bad = dict(CARD_FULL_DATE, course_id=None)
        with pytest.raises(SchemaBreakError):
            map_classroom_card_to_task(child="james", view="assigned", card=bad, today=TODAY)

    def test_missing_title_raises(self):
        bad = dict(CARD_FULL_DATE, title="")
        with pytest.raises(SchemaBreakError):
            map_classroom_card_to_task(child="james", view="assigned", card=bad, today=TODAY)

    def test_url_passes_through_when_already_absolute(self):
        absolute = dict(
            CARD_FULL_DATE,
            href="https://classroom.google.com/explicit/url",
        )
        t = map_classroom_card_to_task(child="james", view="assigned", card=absolute, today=TODAY)
        assert t.url == "https://classroom.google.com/explicit/url"

    def test_timezone_default_is_melbourne(self):
        # Sanity: parse a known date and verify the tz roundtrip lands UTC.
        t = map_classroom_card_to_task(
            child="james",
            view="assigned",
            card=CARD_FULL_DATE,
            tz=DEFAULT_TZ,
            today=TODAY,
        )
        assert t.due_at is not None and t.due_at.tzinfo == UTC


# --------------------------------------------------------------------------- #
# ClassroomStorageState
# --------------------------------------------------------------------------- #


def _state_with_cookies(*names: str) -> dict:
    return {
        "cookies": [{"name": n, "value": "x", "domain": ".google.com"} for n in names],
        "origins": [],
    }


class TestStorageState:
    def test_load_missing_path_raises_auth_expired(self, tmp_path: Path):
        with pytest.raises(AuthExpiredError):
            ClassroomStorageState.load(tmp_path / "missing.json")

    def test_load_invalid_json_raises_auth_expired(self, tmp_path: Path):
        path = tmp_path / "bad.json"
        path.write_text("{not json")
        with pytest.raises(AuthExpiredError):
            ClassroomStorageState.load(path)

    def test_validate_requires_sid(self, tmp_path: Path):
        path = tmp_path / "s.json"
        path.write_text(json.dumps(_state_with_cookies("SAPISID")))
        with pytest.raises(AuthExpiredError, match="SID"):
            ClassroomStorageState.load(path)

    def test_validate_requires_sapisid_family(self, tmp_path: Path):
        path = tmp_path / "s.json"
        path.write_text(json.dumps(_state_with_cookies("SID")))
        with pytest.raises(AuthExpiredError, match="SAPISID"):
            ClassroomStorageState.load(path)

    def test_validate_passes_with_minimum_cookies(self, tmp_path: Path):
        path = tmp_path / "s.json"
        path.write_text(json.dumps(_state_with_cookies("SID", "SAPISID")))
        state = ClassroomStorageState.load(path)
        assert state.cookies_for_domain("google.com")["SID"] == "x"

    def test_secure_variants_satisfy_sapisid_check(self, tmp_path: Path):
        path = tmp_path / "s.json"
        path.write_text(json.dumps(_state_with_cookies("SID", "__Secure-3PAPISID")))
        state = ClassroomStorageState.load(path)
        assert state.cookies_for_domain("google.com")

    def test_save_round_trip(self, tmp_path: Path):
        path = tmp_path / "out.json"
        raw = _state_with_cookies("SID", "SAPISID")
        ClassroomStorageState(raw).save(path)
        reloaded = ClassroomStorageState.load(path)
        assert reloaded.raw == raw


# --------------------------------------------------------------------------- #
# ClassroomSource.fetch (integration with fake scraper)
# --------------------------------------------------------------------------- #


class FakeScraper:
    """Returns canned ScrapeResults per view; supports context-manager."""

    def __init__(self, results_by_view: dict[str, list[dict]]):
        self.results_by_view = results_by_view
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *_exc):
        self.exited = True

    def fetch_view(self, view: str) -> ScrapeResult:
        return ScrapeResult(view=view, cards=self.results_by_view.get(view, []))


@pytest.fixture
def storage_path(tmp_path: Path) -> Path:
    path = tmp_path / "james-classroom.json"
    path.write_text(json.dumps(_state_with_cookies("SID", "SAPISID")))
    return path


class TestClassroomSource:
    def test_fetch_aggregates_three_views(self, storage_path: Path):
        canned = {
            "assigned": [CARD_FULL_DATE],
            "missing": [CARD_NO_YEAR],
            "done": [CARD_HANDED_IN],
        }
        scraper = FakeScraper(canned)
        src = ClassroomSource(
            {"james": storage_path},
            scraper_factory=lambda _s: scraper,
        )
        tasks = src.fetch("james")
        assert len(tasks) == 3
        assert {t.title for t in tasks} == {
            CARD_FULL_DATE["title"],
            CARD_NO_YEAR["title"],
            CARD_HANDED_IN["title"],
        }
        assert scraper.entered and scraper.exited

    def test_fetch_dedupes_card_appearing_in_two_views(self, storage_path: Path):
        # Same card shows up in both /assigned and /missing — we should
        # report it once, with the /missing-flavoured status.
        canned = {
            "assigned": [CARD_FULL_DATE],
            "missing": [CARD_FULL_DATE],
            "done": [],
        }
        src = ClassroomSource(
            {"james": storage_path},
            scraper_factory=lambda _s: FakeScraper(canned),
        )
        tasks = src.fetch("james")
        assert len(tasks) == 1
        assert tasks[0].status is Status.OVERDUE  # from /missing winning

    def test_fetch_unknown_child_raises(self, storage_path: Path):
        src = ClassroomSource({"james": storage_path})
        with pytest.raises(SchemaBreakError):
            src.fetch("tahlia")

    def test_fetch_missing_storage_raises_auth_expired(self, tmp_path: Path):
        src = ClassroomSource({"james": tmp_path / "nope.json"})
        with pytest.raises(AuthExpiredError):
            src.fetch("james")

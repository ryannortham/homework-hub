"""Tests for ``dashboard_layout`` (v5.0 publish-time Dashboard lists)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from homework_hub.dashboard_layout import (
    DashboardTask,
    build_protect_dashboard_request,
    build_requests,
    filter_done,
    filter_no_due_date,
    filter_overdue,
    filter_upcoming,
    filter_week,
    task_rows_to_dashboard_tasks,
)
from homework_hub.pipeline.auth_status import SourceAuthRow
from homework_hub.schema import (
    DASHBOARD_DONE_TABLE_ID,
    DASHBOARD_DONE_TABLE_NAME,
    DASHBOARD_NO_DUE_DATE_TABLE_ID,
    DASHBOARD_NO_DUE_DATE_TABLE_NAME,
    DASHBOARD_OVERDUE_TABLE_ID,
    DASHBOARD_OVERDUE_TABLE_NAME,
    DASHBOARD_UPCOMING_TABLE_ID,
    DASHBOARD_UPCOMING_TABLE_NAME,
    DASHBOARD_WEEK_TABLE_ID,
    DASHBOARD_WEEK_TABLE_NAME,
    STATUS_VALUES,
)

TODAY = date(2026, 5, 28)
DASH_SID = 0


def _by_kind(requests: list[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    return [r for r in requests if kind in r]


def _tasks() -> list[DashboardTask]:
    return [
        DashboardTask("Maths", "Algebra worksheet", date(2026, 5, 20), "Overdue", "https://x/1"),
        DashboardTask("English", "Essay draft", date(2026, 5, 30), "Not started", "https://x/2"),
        DashboardTask("Science", "Lab report", date(2026, 6, 4), "In progress", ""),
        DashboardTask("Art", "Sketchbook", date(2026, 7, 1), "Not started", "https://x/4"),
        DashboardTask(
            "Music",
            "Choose recital piece",
            None,
            "Not started",
            "https://x/5",
            assigned=date(2026, 5, 27),
        ),
        # Overdue-by-status but due in the future — must still land in Overdue.
        DashboardTask("History", "Late essay", date(2026, 6, 1), "Overdue", ""),
    ]


# --------------------------------------------------------------------------- #
# Filters
# --------------------------------------------------------------------------- #


class TestFilters:
    def test_overdue_uses_status_not_days(self):
        tasks = _tasks()
        overdue = filter_overdue(tasks)
        assert [t.title for t in overdue] == ["Algebra worksheet", "Late essay"]

    def test_week_excludes_overdue_status(self):
        # A task due today but flipped to Overdue must NOT appear in Week.
        tasks = [
            DashboardTask("S", "today-overdue", TODAY, "Overdue", ""),
            DashboardTask("S", "today-ok", TODAY, "Not started", ""),
        ]
        week = filter_week(tasks, TODAY)
        assert [t.title for t in week] == ["today-ok"]

    def test_week_excludes_done_statuses(self):
        """Submitted / Graded / Archived must not appear in the Week
        section even if due within the 0..7 day window — matches the
        KPI tile formula so tile count == table row count."""
        tasks = [
            DashboardTask("S", "submitted", TODAY, "Submitted", ""),
            DashboardTask("S", "graded", TODAY, "Graded", ""),
            DashboardTask("S", "archived", TODAY, "Archived", ""),
            DashboardTask("S", "pending", TODAY, "Not started", ""),
        ]
        week = filter_week(tasks, TODAY)
        assert [t.title for t in week] == ["pending"]

    def test_week_boundary_inclusive_seven_days(self):
        tasks = [
            DashboardTask("S", "d7", date(2026, 6, 4), "Not started", ""),  # today+7
            DashboardTask("S", "d8", date(2026, 6, 5), "Not started", ""),  # today+8
        ]
        week = filter_week(tasks, TODAY)
        upcoming = filter_upcoming(tasks, TODAY)
        assert [t.title for t in week] == ["d7"]
        assert [t.title for t in upcoming] == ["d8"]

    def test_upcoming_excludes_overdue_status(self):
        tasks = [
            DashboardTask("S", "future-overdue", date(2026, 8, 1), "Overdue", ""),
            DashboardTask("S", "future-ok", date(2026, 8, 1), "Not started", ""),
        ]
        upcoming = filter_upcoming(tasks, TODAY)
        assert [t.title for t in upcoming] == ["future-ok"]

    def test_upcoming_excludes_done_statuses(self):
        tasks = [
            DashboardTask("S", "submitted", date(2026, 8, 1), "Submitted", ""),
            DashboardTask("S", "graded", date(2026, 8, 1), "Graded", ""),
            DashboardTask("S", "archived", date(2026, 8, 1), "Archived", ""),
            DashboardTask("S", "pending", date(2026, 8, 1), "Not started", ""),
        ]
        upcoming = filter_upcoming(tasks, TODAY)
        assert [t.title for t in upcoming] == ["pending"]

    def test_no_due_date_includes_only_pending_undated_tasks(self):
        tasks = [
            DashboardTask("S", "pending", None, "Not started", ""),
            DashboardTask("S", "in-progress", None, "In progress", ""),
            DashboardTask("S", "overdue", None, "Overdue", ""),
            DashboardTask("S", "submitted", None, "Submitted", ""),
            DashboardTask("S", "graded", None, "Graded", ""),
            DashboardTask("S", "archived", None, "Archived", ""),
            DashboardTask("S", "dated", TODAY, "Not started", ""),
        ]

        result = filter_no_due_date(tasks)

        assert [t.title for t in result] == ["in-progress", "pending"]

    def test_no_due_date_sorts_newest_assignments_first(self):
        tasks = [
            DashboardTask(
                "S",
                "older",
                None,
                "Not started",
                "",
                assigned=date(2026, 5, 20),
            ),
            DashboardTask(
                "S",
                "newest",
                None,
                "Not started",
                "",
                assigned=date(2026, 5, 27),
            ),
            DashboardTask("S", "unknown", None, "Not started", ""),
        ]

        result = filter_no_due_date(tasks)

        assert [t.title for t in result] == ["newest", "older", "unknown"]

    def test_every_pending_task_appears_in_exactly_one_active_section(self):
        tasks = [
            DashboardTask("S", "overdue", date(2026, 5, 20), "Overdue", ""),
            DashboardTask("S", "week", TODAY, "Not started", ""),
            DashboardTask("S", "upcoming", date(2026, 6, 5), "In progress", ""),
            DashboardTask("S", "undated", None, "Not started", ""),
        ]
        sections = (
            filter_overdue(tasks),
            filter_week(tasks, TODAY),
            filter_upcoming(tasks, TODAY),
            filter_no_due_date(tasks),
        )
        memberships = {task.title: sum(task in section for section in sections) for task in tasks}

        assert memberships == {
            "overdue": 1,
            "week": 1,
            "upcoming": 1,
            "undated": 1,
        }


class TestFilterDone:
    """Done(7d): status IN {Submitted, Graded} AND completed within last 7 days.

    "Completed" prefers ``submitted`` (silver ``submitted_at`` projected
    to a Melbourne local date) and falls back to ``due`` for legacy
    rows that pre-date transition-time stamping.
    """

    def test_includes_submitted_and_graded_within_window_by_due_fallback(self):
        # No `submitted` set → falls back to `due`. Covers legacy rows.
        tasks = [
            DashboardTask("S", "sub-today", TODAY, "Submitted", ""),
            DashboardTask("S", "grad-yesterday", date(2026, 5, 27), "Graded", ""),
            DashboardTask("S", "sub-7d-ago", date(2026, 5, 21), "Submitted", ""),
        ]
        done = filter_done(tasks, TODAY)
        assert {t.title for t in done} == {"sub-today", "grad-yesterday", "sub-7d-ago"}

    def test_uses_submitted_when_present_over_due(self):
        # Future due (Sunday) but submitted today — must surface.
        # Mirrors james's Japanese EP tasks (due 2026-05-31, status=Submitted).
        tasks = [
            DashboardTask(
                "Japanese",
                "Chapter 1 Task 1 Verb meanings",
                date(2026, 5, 31),
                "Submitted",
                "",
                submitted=TODAY,
            ),
        ]
        done = filter_done(tasks, TODAY)
        assert [t.title for t in done] == ["Chapter 1 Task 1 Verb meanings"]

    def test_excludes_outside_window_by_due_fallback(self):
        tasks = [
            DashboardTask("S", "too-old", date(2026, 5, 20), "Submitted", ""),  # 8d ago
            DashboardTask("S", "future", date(2026, 5, 30), "Submitted", ""),  # tomorrow
        ]
        done = filter_done(tasks, TODAY)
        assert done == []

    def test_excludes_when_submitted_outside_window(self):
        # `submitted` outranks `due`, so an old `submitted` excludes a row
        # that would otherwise qualify by `due`.
        tasks = [
            DashboardTask(
                "S",
                "old-completion",
                TODAY,
                "Submitted",
                "",
                submitted=date(2026, 5, 19),
            ),
        ]
        assert filter_done(tasks, TODAY) == []

    def test_excludes_non_done_statuses(self):
        tasks = [
            DashboardTask("S", "pending", TODAY, "Not started", ""),
            DashboardTask("S", "in-prog", TODAY, "In progress", ""),
            DashboardTask("S", "overdue", TODAY, "Overdue", ""),
            DashboardTask("S", "archived", TODAY, "Archived", ""),
        ]
        assert filter_done(tasks, TODAY) == []

    def test_sorted_by_completed_descending(self):
        tasks = [
            DashboardTask("S", "older", date(2026, 5, 23), "Submitted", ""),
            DashboardTask("S", "newest", date(2026, 5, 28), "Graded", ""),
            DashboardTask("S", "middle", date(2026, 5, 25), "Submitted", ""),
        ]
        done = filter_done(tasks, TODAY)
        assert [t.title for t in done] == ["newest", "middle", "older"]

    def test_sort_prefers_submitted_over_due(self):
        # A row with submitted=today should beat a row with due=today
        # but submitted=yesterday.
        tasks = [
            DashboardTask(
                "S", "due-today-sub-yest", TODAY, "Submitted", "", submitted=date(2026, 5, 27)
            ),
            DashboardTask(
                "S", "due-yest-sub-today", date(2026, 5, 27), "Submitted", "", submitted=TODAY
            ),
        ]
        done = filter_done(tasks, TODAY)
        assert [t.title for t in done] == ["due-yest-sub-today", "due-today-sub-yest"]

    def test_skips_rows_with_no_due_or_submitted(self):
        tasks = [DashboardTask("S", "no-dates", None, "Submitted", "")]
        assert filter_done(tasks, TODAY) == []


# --------------------------------------------------------------------------- #
# Teardown
# --------------------------------------------------------------------------- #


class TestTeardown:
    def test_emits_delete_table_per_existing(self):
        reqs = build_requests(
            dash_sheet_id=DASH_SID,
            tasks=[],
            today=TODAY,
            existing_table_ids=["t1", "t2", "t3"],
        )
        deletes = _by_kind(reqs, "deleteTable")
        assert [d["deleteTable"]["tableId"] for d in deletes] == ["t1", "t2", "t3"]

    def test_emits_delete_banding_per_existing(self):
        reqs = build_requests(
            dash_sheet_id=DASH_SID,
            tasks=[],
            today=TODAY,
            existing_banded_range_ids=[11, 22],
        )
        deletes = _by_kind(reqs, "deleteBanding")
        assert {d["deleteBanding"]["bandedRangeId"] for d in deletes} == {11, 22}

    def test_emits_drain_delete_conditional_format_rules(self):
        reqs = build_requests(
            dash_sheet_id=DASH_SID,
            tasks=[],
            today=TODAY,
            existing_conditional_format_rule_count=5,
        )
        deletes = _by_kind(reqs, "deleteConditionalFormatRule")
        assert len(deletes) == 5
        for d in deletes:
            assert d["deleteConditionalFormatRule"]["index"] == 0
            assert d["deleteConditionalFormatRule"]["sheetId"] == DASH_SID

    def test_teardown_unmerges_lists_region(self):
        """Leftover footer merges from previous publishes block
        ``addTable`` — Sheets returns an opaque 500. Teardown MUST emit
        an ``unmergeCells`` covering the full lists region (from the
        first section header row to the bottom of the pre-sized grid)
        so the new addTables have a clean slate."""
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        unmerges = _by_kind(reqs, "unmergeCells")
        assert len(unmerges) == 1
        rng = unmerges[0]["unmergeCells"]["range"]
        assert rng["sheetId"] == DASH_SID
        assert rng["startRowIndex"] == 3  # _LISTS_START_ROW
        assert rng["endRowIndex"] >= 100  # covers any plausible leftover
        assert rng["startColumnIndex"] == 0
        assert rng["endColumnIndex"] == 6  # full canvas width


# --------------------------------------------------------------------------- #
# Section sizing + Tables
# --------------------------------------------------------------------------- #


class TestSectionSizing:
    def test_five_tables_with_canonical_names_and_ids(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        tables = _by_kind(reqs, "addTable")
        assert len(tables) == 5
        names = [t["addTable"]["table"]["name"] for t in tables]
        ids = [t["addTable"]["table"]["tableId"] for t in tables]
        assert names == [
            DASHBOARD_OVERDUE_TABLE_NAME,
            DASHBOARD_WEEK_TABLE_NAME,
            DASHBOARD_NO_DUE_DATE_TABLE_NAME,
            DASHBOARD_UPCOMING_TABLE_NAME,
            DASHBOARD_DONE_TABLE_NAME,
        ]
        assert ids == [
            DASHBOARD_OVERDUE_TABLE_ID,
            DASHBOARD_WEEK_TABLE_ID,
            DASHBOARD_NO_DUE_DATE_TABLE_ID,
            DASHBOARD_UPCOMING_TABLE_ID,
            DASHBOARD_DONE_TABLE_ID,
        ]

    def test_table_range_sizes_match_data(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        tables = _by_kind(reqs, "addTable")
        # Overdue: 2, Week: 2, No due date: 1, Upcoming: 1,
        # Done: empty → 1 fallback row.
        spans = [
            t["addTable"]["table"]["range"]["endRowIndex"]
            - t["addTable"]["table"]["range"]["startRowIndex"]
            - 1  # subtract header row
            for t in tables
        ]
        assert spans == [2, 2, 1, 1, 1]

    def test_empty_section_emits_one_row_fallback(self):
        # No tasks at all → all five sections have 1-row fallback.
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=[], today=TODAY)
        tables = _by_kind(reqs, "addTable")
        for t in tables:
            rng = t["addTable"]["table"]["range"]
            assert rng["endRowIndex"] - rng["startRowIndex"] == 2  # header + 1 fallback

        # Each section's data row should contain a fallback string in the Title col.
        writes = _by_kind(reqs, "updateCells")
        title_fallbacks = []
        for w in writes:
            uc = w["updateCells"]
            if "start" not in uc:
                continue
            for row in uc.get("rows", []):
                if len(row["values"]) == 4:
                    title = row["values"][1].get("userEnteredValue", {}).get("stringValue", "")
                    if title and not title.startswith("Title") and title not in ("Subject",):
                        title_fallbacks.append(title)
        assert any("caught up" in s for s in title_fallbacks)
        assert any("Nothing due" in s for s in title_fallbacks)
        assert any("due date" in s.lower() for s in title_fallbacks)
        assert any("upcoming" in s.lower() for s in title_fallbacks)
        assert any("completed" in s.lower() for s in title_fallbacks)


# --------------------------------------------------------------------------- #
# Status dropdown + CF
# --------------------------------------------------------------------------- #


class TestStatusColumn:
    def test_addtable_has_no_column_properties(self):
        """Sheets ``addTable`` with any ``columnProperties`` field
        reliably 500s when the Dashboard tab hosts the KPI scorecard /
        donut charts. We emit a bare ``addTable`` (no column metadata)
        — Sheets infers TEXT for every column — and layer the Status
        dropdown via a separate ``setDataValidation`` request."""
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        for t in _by_kind(reqs, "addTable"):
            assert "columnProperties" not in t["addTable"]["table"]

    def test_status_dropdown_applied_per_section(self):
        """One ``updateTable`` per section, setting ``columnProperties``
        with ``columnType=DROPDOWN`` on the Status column. Column types
        come via ``updateTable`` (not inline on ``addTable``) because
        inline ``columnProperties`` 500s on a Dashboard tab that hosts
        floating charts — ``updateTable`` against an already-registered
        table sidesteps the poison."""
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        # Filter to the section ``updateTable`` requests (the ones that
        # set columnProperties). The header-only repaint ``updateTable``s
        # for the non-Dashboard tabs are tested separately.
        updates = [
            u
            for u in _by_kind(reqs, "updateTable")
            if "columnProperties" in u["updateTable"]["table"]
        ]
        assert len(updates) == 5  # one per section
        for u in updates:
            cols = u["updateTable"]["table"]["columnProperties"]
            # All four content columns are described.
            assert [c["columnIndex"] for c in cols] == [0, 1, 2, 3]
            assert [c["columnType"] for c in cols] == [
                "TEXT",
                "TEXT",
                "DATE",
                "DROPDOWN",
            ]
            status = cols[3]
            cond = status["dataValidationRule"]["condition"]
            assert cond["type"] == "ONE_OF_LIST"
            vals = [v["userEnteredValue"] for v in cond["values"]]
            assert vals == list(STATUS_VALUES)
        # The standalone setDataValidation request is no longer needed —
        # the table's DROPDOWN column type carries the validation.
        assert _by_kind(reqs, "setDataValidation") == []

    def test_no_separate_add_banding_layered_over_table(self):
        """Tables provide implicit banding — layering ``addBanding`` on
        the same range triggers a Sheets 500."""
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        assert _by_kind(reqs, "addBanding") == []

    def test_no_status_pill_conditional_format_rules(self):
        """Status column uses the default dropdown-chip rendering for
        visual consistency with the Tasks tab — no per-status CF pill
        styling layered on top."""
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        assert _by_kind(reqs, "addConditionalFormatRule") == []


class TestTableHeaderColour:
    """Every Sheets-Table header chip — Dashboard's five sections AND
    the non-Dashboard tabs (Tasks/History/UserEdits/Settings) — is
    painted with the resolved theme accent so the whole sheet tracks
    the kid's chosen ``Format → Theme``. When no theme is supplied
    publish falls back to a sage default."""

    def test_all_section_tables_use_default_header_color_without_theme(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        section_updates = [
            u
            for u in _by_kind(reqs, "updateTable")
            if "columnProperties" in u["updateTable"]["table"]
        ]
        assert len(section_updates) == 5
        target = {"red": 111 / 255, "green": 164 / 255, "blue": 140 / 255}
        for u in section_updates:
            rows_props = u["updateTable"]["table"]["rowsProperties"]
            rgb = rows_props["headerColorStyle"]["rgbColor"]
            for chan, expected in target.items():
                assert rgb[chan] == pytest.approx(expected, rel=1e-6)

    def test_section_tables_use_theme_accent_when_supplied(self):
        accent = {"red": 0.2, "green": 0.4, "blue": 0.8}
        reqs = build_requests(
            dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY, theme_accent=accent
        )
        section_updates = [
            u
            for u in _by_kind(reqs, "updateTable")
            if "columnProperties" in u["updateTable"]["table"]
        ]
        assert len(section_updates) == 5
        for u in section_updates:
            rgb = u["updateTable"]["table"]["rowsProperties"]["headerColorStyle"]["rgbColor"]
            assert rgb == accent

    def test_update_table_fields_mask_covers_columns_and_header_colour(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        for u in _by_kind(reqs, "updateTable"):
            mask = u["updateTable"]["fields"]
            # Every updateTable touches headerColorStyle; the section
            # ones additionally carry columnProperties.
            assert "rowsProperties.headerColorStyle" in mask
            if "columnProperties" in u["updateTable"]["table"]:
                assert "columnProperties" in mask


# --------------------------------------------------------------------------- #
# Footer + grid resize
# --------------------------------------------------------------------------- #


class TestFooterAndGrid:
    def test_no_grid_resize_emitted(self):
        """The Dashboard grid is pre-sized at template bootstrap to
        ``_DASH_GRID_ROW_COUNT`` rows so ``addTable`` ranges always fit
        within existing bounds. ``build_requests`` MUST NOT emit a grid
        resize — doing so in the same batch as ``addTable`` triggers a
        Sheets server-side 500."""
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        resizes = [
            r
            for r in reqs
            if "updateSheetProperties" in r
            and "gridProperties" in r["updateSheetProperties"]["properties"]
        ]
        assert resizes == []

    def test_addtables_emitted_for_every_section(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        assert sum(1 for r in reqs if "addTable" in r) == 5

    def test_footer_emits_lastsync_formula_merged(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        # Find the formula write for the lastsync footer.
        found = False
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            if "start" not in uc:
                continue
            for row in uc.get("rows", []):
                for cell in row["values"]:
                    f = cell.get("userEnteredValue", {}).get("formulaValue", "")
                    if 'VLOOKUP("Last full sync"' in f:
                        found = True
        assert found, "lastsync footer formula missing"
        # Footer merge is the only mergeCells in the lists region.
        merges = _by_kind(reqs, "mergeCells")
        assert len(merges) == 1


# --------------------------------------------------------------------------- #
# Cell rendering
# --------------------------------------------------------------------------- #


class TestCellRendering:
    def test_source_freshness_line_is_neutral(self):
        source_rows = [
            SourceAuthRow(
                source="classroom",
                display_name="Classroom",
                last_success_at=datetime(2026, 5, 1, 14, 0, tzinfo=UTC),
                last_failure_at=None,
                last_failure_kind=None,
                token_expires_at=None,
                token_present=True,
                status="ok",
            ),
            SourceAuthRow(
                source="eduperfect",
                display_name="EduPerfect",
                last_success_at=datetime(2026, 4, 30, 14, 0, tzinfo=UTC),
                last_failure_at=datetime(2026, 5, 1, 15, 0, tzinfo=UTC),
                last_failure_kind="auth_expired",
                token_expires_at=datetime(2026, 5, 1, 13, 0, tzinfo=UTC),
                token_present=True,
                status="expired",
            ),
        ]

        requests = build_requests(
            dash_sheet_id=DASH_SID,
            tasks=[],
            today=TODAY,
            source_auth_rows=source_rows,
        )

        values = [
            cell["userEnteredValue"]["stringValue"]
            for request in _by_kind(requests, "updateCells")
            for row in request["updateCells"].get("rows", [])
            for cell in row.get("values", [])
            if "stringValue" in cell.get("userEnteredValue", {})
        ]
        freshness = next(value for value in values if value.startswith("Sources:"))
        assert "Classroom updated 02/05 00:00" in freshness
        assert "EP updated 01/05 00:00" in freshness
        assert "expired" not in freshness.lower()
        assert "⚠" not in freshness

    def test_source_freshness_line_uses_own_row_before_tables(self):
        source_rows = [
            SourceAuthRow(
                source="classroom",
                display_name="Classroom",
                last_success_at=None,
                last_failure_at=None,
                last_failure_kind=None,
                token_expires_at=None,
                token_present=True,
                status="never_synced",
            )
        ]

        requests = build_requests(
            dash_sheet_id=DASH_SID,
            tasks=[],
            today=TODAY,
            source_auth_rows=source_rows,
        )

        source_merge = next(
            request["mergeCells"]["range"]
            for request in requests
            if request.get("mergeCells", {}).get("range", {}).get("startRowIndex") == 3
        )
        assert source_merge["startColumnIndex"] == 1
        assert source_merge["endColumnIndex"] == 5
        first_table = next(
            request["addTable"]["table"] for request in requests if "addTable" in request
        )
        assert first_table["range"]["startRowIndex"] == 5

    def test_title_uses_hyperlink_when_link_present(self):
        tasks = [DashboardTask("S", 'Re"port', date(2026, 5, 30), "Not started", "https://a/b")]
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=tasks, today=TODAY)
        # Week section will contain this row.
        formulas = []
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            if "start" not in uc:
                continue
            for row in uc.get("rows", []):
                if len(row["values"]) == 4:
                    f = row["values"][1].get("userEnteredValue", {}).get("formulaValue", "")
                    if f:
                        formulas.append(f)
        assert any('HYPERLINK("https://a/b","Re""port")' in f for f in formulas)

    def test_due_cell_uses_date_serial(self):
        tasks = [DashboardTask("S", "x", date(2026, 5, 30), "Not started", "")]
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=tasks, today=TODAY)
        # Look for the data-row write that has a numberValue in the Due column.
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            if "start" not in uc:
                continue
            for row in uc.get("rows", []):
                if len(row["values"]) == 4:
                    due = row["values"][2]
                    nv = due.get("userEnteredValue", {}).get("numberValue")
                    if nv is not None:
                        # 2026-05-30 minus 1899-12-30 epoch.
                        assert nv == (date(2026, 5, 30) - date(1899, 12, 30)).days
                        fmt = due["userEnteredFormat"]["numberFormat"]
                        assert fmt["type"] == "DATE"
                        assert fmt["pattern"] == "dd/MM/yyyy"
                        return
        raise AssertionError("no date-serial Due cell emitted")


# --------------------------------------------------------------------------- #
# TaskRow projection
# --------------------------------------------------------------------------- #


class TestTaskRowProjection:
    def test_projects_minimum_fields(self):
        class _Row:
            def __init__(self, cells: list[Any]) -> None:
                self.cells = cells

        # TASKS_TAB columns: Subject, Type, Title, Due, Days, Status, Notes, Source, Link, task_uid.
        row = _Row(
            [
                "Maths",
                "Homework",
                "Algebra",
                date(2026, 5, 30),
                2,
                "Not started",
                "",
                "manual",
                "https://x",
                "uid-1",
            ]
        )
        out = task_rows_to_dashboard_tasks([row])
        assert out == [
            DashboardTask("Maths", "Algebra", date(2026, 5, 30), "Not started", "https://x")
        ]

    def test_overlays_submitted_from_silver_tasks(self):
        """When the silver Task list is passed, submitted_at is converted
        to a Melbourne-local date and joined onto the projected row by
        task_uid. This is the bridge that lets ``filter_done`` use the
        completion date instead of the due date."""
        from datetime import UTC, datetime

        from homework_hub.models import Source, Status, Task

        class _Row:
            def __init__(self, cells: list[Any]) -> None:
                self.cells = cells

        row = _Row(
            [
                "Japanese",
                "Homework",
                "Chapter 1 Task 1",
                date(2026, 5, 31),
                3,
                "Submitted",
                "",
                "manual",
                "https://x",
                "eduperfect:12215183",
            ]
        )
        # 2026-05-28 22:30 UTC == 2026-05-29 08:30 AEST → Melbourne date 29 May.
        submitted_utc = datetime(2026, 5, 28, 22, 30, tzinfo=UTC)
        silver_task = Task(
            source=Source.EDUPERFECT,
            source_id="12215183",
            child="james",
            subject="Japanese",
            title="Chapter 1 Task 1",
            status=Status.SUBMITTED,
            submitted_at=submitted_utc,
        )
        out = task_rows_to_dashboard_tasks([row], tasks=[silver_task])
        assert len(out) == 1
        assert out[0].submitted == date(2026, 5, 29)

    def test_submitted_none_when_silver_task_missing(self):
        class _Row:
            def __init__(self, cells: list[Any]) -> None:
                self.cells = cells

        row = _Row(
            [
                "Maths",
                "Homework",
                "Orphan",
                date(2026, 5, 30),
                2,
                "Submitted",
                "",
                "manual",
                "https://x",
                "compass:GHOST",
            ]
        )
        out = task_rows_to_dashboard_tasks([row], tasks=[])
        assert out[0].submitted is None


class TestProtectDashboardRequest:
    """The whole-sheet hard-lock that prevents kids editing the Dashboard."""

    def test_targets_dashboard_sheet_id_only(self):
        req = build_protect_dashboard_request(
            dashboard_sheet_id=42,
            service_account_email="bot@svc.iam.gserviceaccount.com",
        )
        pr = req["addProtectedRange"]["protectedRange"]
        # Range body carries ONLY ``sheetId`` — no row/column bounds —
        # which the Sheets API interprets as "the whole sheet".
        assert pr["range"] == {"sheetId": 42}

    def test_hard_lock_not_warning_only(self):
        req = build_protect_dashboard_request(
            dashboard_sheet_id=42,
            service_account_email="bot@svc.iam.gserviceaccount.com",
        )
        pr = req["addProtectedRange"]["protectedRange"]
        assert pr["warningOnly"] is False

    def test_service_account_sole_editor(self):
        req = build_protect_dashboard_request(
            dashboard_sheet_id=42,
            service_account_email="bot@svc.iam.gserviceaccount.com",
        )
        pr = req["addProtectedRange"]["protectedRange"]
        assert pr["editors"] == {"users": ["bot@svc.iam.gserviceaccount.com"]}

    def test_description_is_concise_user_facing_string(self):
        req = build_protect_dashboard_request(
            dashboard_sheet_id=42,
            service_account_email="bot@svc.iam.gserviceaccount.com",
        )
        pr = req["addProtectedRange"]["protectedRange"]
        # The description surfaces in Sheets' lock toast. Lock it so
        # accidental wording drift gets caught.
        assert pr["description"] == "Auto-generated — edit on Tasks tab"


# --------------------------------------------------------------------------- #
# Theme-aware colour helpers + non-Dashboard table repaint
# --------------------------------------------------------------------------- #


class TestResolveAndTint:
    """``_resolve_header_color`` falls back when no theme is supplied;
    ``_tint`` mixes ``white_mix`` fraction of pure white with the accent."""

    def test_resolve_returns_default_when_no_theme(self):
        from homework_hub.dashboard_layout import _DEFAULT_HEADER_COLOR, _resolve_header_color

        assert _resolve_header_color(None) == _DEFAULT_HEADER_COLOR

    def test_resolve_returns_input_when_theme_present(self):
        from homework_hub.dashboard_layout import _resolve_header_color

        accent = {"red": 0.1, "green": 0.5, "blue": 0.9}
        assert _resolve_header_color(accent) == accent

    def test_tint_known_red_accent(self):
        from homework_hub.dashboard_layout import _tint

        # Pure red, 92% white → red stays 1.0, green/blue rise to 0.92.
        out = _tint({"red": 1.0, "green": 0.0, "blue": 0.0}, white_mix=0.92)
        assert out["red"] == pytest.approx(1.0)
        assert out["green"] == pytest.approx(0.92)
        assert out["blue"] == pytest.approx(0.92)

    def test_tint_missing_channels_default_to_zero(self):
        from homework_hub.dashboard_layout import _tint

        # All channels omitted → output is pure white_mix everywhere.
        out = _tint({}, white_mix=0.92)
        assert out == {"red": 0.92, "green": 0.92, "blue": 0.92}


class TestRepaintNonDashboardTableHeaders:
    """``build_requests`` appends one ``updateTable`` per non-Dashboard
    Sheets-Table (Tasks/History/UserEdits/Settings) so theme changes
    propagate to every tab on every publish."""

    def _repaint_requests(self, reqs: list[dict]) -> list[dict]:
        return [
            u
            for u in reqs
            if "updateTable" in u and "columnProperties" not in u["updateTable"]["table"]
        ]

    def test_emits_one_per_non_dashboard_table(self):
        from homework_hub.schema import DASHBOARD_TAB, SCHEMA

        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        repaints = self._repaint_requests(reqs)
        expected_table_ids = {
            tab.table_id for tab in SCHEMA.tabs if tab.table_id and tab.name != DASHBOARD_TAB.name
        }
        seen = {u["updateTable"]["table"]["tableId"] for u in repaints}
        assert seen == expected_table_ids

    def test_fields_mask_only_header_colour(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        for u in self._repaint_requests(reqs):
            assert u["updateTable"]["fields"] == "rowsProperties.headerColorStyle"

    def test_uses_theme_accent_when_supplied(self):
        accent = {"red": 0.2, "green": 0.4, "blue": 0.8}
        reqs = build_requests(
            dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY, theme_accent=accent
        )
        for u in self._repaint_requests(reqs):
            rgb = u["updateTable"]["table"]["rowsProperties"]["headerColorStyle"]["rgbColor"]
            assert rgb == accent

    def test_falls_back_to_default_without_theme(self):
        from homework_hub.dashboard_layout import _DEFAULT_HEADER_COLOR

        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        repaints = self._repaint_requests(reqs)
        assert repaints, "expected at least one repaint request"
        for u in repaints:
            rgb = u["updateTable"]["table"]["rowsProperties"]["headerColorStyle"]["rgbColor"]
            assert rgb == _DEFAULT_HEADER_COLOR

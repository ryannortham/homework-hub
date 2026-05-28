"""Tests for ``dashboard_layout`` (v5.0 publish-time Dashboard lists)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from homework_hub.dashboard_layout import (
    DashboardTask,
    build_protect_dashboard_request,
    build_requests,
    filter_done,
    filter_overdue,
    filter_upcoming,
    filter_week,
    task_rows_to_dashboard_tasks,
)
from homework_hub.schema import (
    DASHBOARD_DONE_TABLE_ID,
    DASHBOARD_DONE_TABLE_NAME,
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


class TestFilterDone:
    """Done(7d): status IN {Submitted, Graded} AND due within last 7 days."""

    def test_includes_submitted_and_graded_within_window(self):
        tasks = [
            DashboardTask("S", "sub-today", TODAY, "Submitted", ""),
            DashboardTask("S", "grad-yesterday", date(2026, 5, 27), "Graded", ""),
            DashboardTask("S", "sub-7d-ago", date(2026, 5, 21), "Submitted", ""),
        ]
        done = filter_done(tasks, TODAY)
        assert {t.title for t in done} == {"sub-today", "grad-yesterday", "sub-7d-ago"}

    def test_excludes_outside_window(self):
        tasks = [
            DashboardTask("S", "too-old", date(2026, 5, 20), "Submitted", ""),  # 8d ago
            DashboardTask("S", "future", date(2026, 5, 30), "Submitted", ""),  # tomorrow
        ]
        done = filter_done(tasks, TODAY)
        assert done == []

    def test_excludes_non_done_statuses(self):
        tasks = [
            DashboardTask("S", "pending", TODAY, "Not started", ""),
            DashboardTask("S", "in-prog", TODAY, "In progress", ""),
            DashboardTask("S", "overdue", TODAY, "Overdue", ""),
            DashboardTask("S", "archived", TODAY, "Archived", ""),
        ]
        assert filter_done(tasks, TODAY) == []

    def test_sorted_by_due_descending(self):
        tasks = [
            DashboardTask("S", "older", date(2026, 5, 23), "Submitted", ""),
            DashboardTask("S", "newest", date(2026, 5, 28), "Graded", ""),
            DashboardTask("S", "middle", date(2026, 5, 25), "Submitted", ""),
        ]
        done = filter_done(tasks, TODAY)
        assert [t.title for t in done] == ["newest", "middle", "older"]

    def test_skips_rows_with_no_due_date(self):
        tasks = [DashboardTask("S", "no-due", None, "Submitted", "")]
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
    def test_four_tables_with_canonical_names_and_ids(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        tables = _by_kind(reqs, "addTable")
        assert len(tables) == 4
        names = [t["addTable"]["table"]["name"] for t in tables]
        ids = [t["addTable"]["table"]["tableId"] for t in tables]
        assert names == [
            DASHBOARD_OVERDUE_TABLE_NAME,
            DASHBOARD_WEEK_TABLE_NAME,
            DASHBOARD_UPCOMING_TABLE_NAME,
            DASHBOARD_DONE_TABLE_NAME,
        ]
        assert ids == [
            DASHBOARD_OVERDUE_TABLE_ID,
            DASHBOARD_WEEK_TABLE_ID,
            DASHBOARD_UPCOMING_TABLE_ID,
            DASHBOARD_DONE_TABLE_ID,
        ]

    def test_table_range_sizes_match_data(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        tables = _by_kind(reqs, "addTable")
        # Overdue: 2 (Algebra + Late essay), Week: 2 (Essay draft today+2, Lab today+7),
        # Upcoming: 1 (Sketchbook today+34), Done: empty → 1 fallback row.
        spans = [
            t["addTable"]["table"]["range"]["endRowIndex"]
            - t["addTable"]["table"]["range"]["startRowIndex"]
            - 1  # subtract header row
            for t in tables
        ]
        assert spans == [2, 2, 1, 1]

    def test_empty_section_emits_one_row_fallback(self):
        # No tasks at all → all four sections have 1-row fallback.
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
        updates = _by_kind(reqs, "updateTable")
        assert len(updates) == 4  # one per section
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
    """All four Dashboard Tables share a sage-green header chip —
    RGB(111, 164, 140) — applied via the same ``updateTable`` request
    that sets ``columnProperties``. The colour rides on
    ``Table.rowsProperties.headerColorStyle`` with a ``fields`` mask
    extension; band colours are left at Sheets defaults."""

    def test_all_tables_use_target_header_color(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        updates = _by_kind(reqs, "updateTable")
        assert len(updates) == 4
        target = {"red": 111 / 255, "green": 164 / 255, "blue": 140 / 255}
        for u in updates:
            rows_props = u["updateTable"]["table"]["rowsProperties"]
            rgb = rows_props["headerColorStyle"]["rgbColor"]
            for chan, expected in target.items():
                assert rgb[chan] == pytest.approx(expected, rel=1e-6)

    def test_update_table_fields_mask_covers_columns_and_header_colour(self):
        reqs = build_requests(dash_sheet_id=DASH_SID, tasks=_tasks(), today=TODAY)
        for u in _by_kind(reqs, "updateTable"):
            mask = u["updateTable"]["fields"]
            assert "columnProperties" in mask
            assert "rowsProperties.headerColorStyle" in mask


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
        assert sum(1 for r in reqs if "addTable" in r) == 4

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

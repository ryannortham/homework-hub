"""Tests for the medallion-aware bootstrap-sheet template (M5c).

Verifies that ``bootstrap_requests()`` emits a structurally-correct
``spreadsheets.batchUpdate`` body for the SCHEMA spec — the right tabs,
the right Tables, dropdowns, formats and hidden-tab flag. No live API
calls; pure dict assertions.
"""

from __future__ import annotations

from typing import Any

from homework_hub.schema import (
    DASHBOARD_DATA_TAB,
    DASHBOARD_TAB,
    HISTORY_TAB,
    SCHEMA,
    SETTINGS_TAB,
    TASKS_TAB,
    USER_EDITS_TAB,
)
from homework_hub.sheet_template import bootstrap_requests, refresh_layout_requests


def _by_kind(reqs: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return [r for r in reqs if key in r]


def _addsheet_titles(reqs: list[dict[str, Any]]) -> list[str]:
    return [r["addSheet"]["properties"]["title"] for r in _by_kind(reqs, "addSheet")]


def _addtable_names(reqs: list[dict[str, Any]]) -> list[str]:
    return [r["addTable"]["table"]["name"] for r in _by_kind(reqs, "addTable")]


class TestTabCreation:
    def test_locale_set_to_en_au(self):
        """Spreadsheet locale must be en_AU so dd/mm/yyyy patterns render day-first."""
        reqs = bootstrap_requests()
        locale_reqs = _by_kind(reqs, "updateSpreadsheetProperties")
        assert len(locale_reqs) == 1
        props = locale_reqs[0]["updateSpreadsheetProperties"]["properties"]
        assert props["locale"] == "en_AU"

    def test_default_tab_renamed_to_first_schema_tab(self):
        reqs = bootstrap_requests()
        first_rename = _by_kind(reqs, "updateSheetProperties")[0]
        assert first_rename["updateSheetProperties"]["properties"]["sheetId"] == 0
        assert first_rename["updateSheetProperties"]["properties"]["title"] == DASHBOARD_TAB.name
        assert first_rename["updateSheetProperties"]["properties"]["title"] == "Dashboard"

    def test_other_tabs_are_added(self):
        reqs = bootstrap_requests()
        titles = _addsheet_titles(reqs)
        # 5 extra tabs beyond the renamed first
        assert titles == [
            TASKS_TAB.name,
            HISTORY_TAB.name,
            SETTINGS_TAB.name,
            USER_EDITS_TAB.name,
            DASHBOARD_DATA_TAB.name,
        ]

    def test_extra_tabs_get_distinct_sheet_ids(self):
        reqs = bootstrap_requests()
        ids = [r["addSheet"]["properties"]["sheetId"] for r in _by_kind(reqs, "addSheet")]
        assert len(set(ids)) == len(ids)
        assert 0 not in ids  # 0 reserved for the renamed first tab


class TestHeaders:
    def test_each_tab_with_headers_writes_a_header_row(self):
        reqs = bootstrap_requests()
        update_cells = _by_kind(reqs, "updateCells")
        # Today is pure formula — it writes a formula at A1, not headers.
        # The other 4 tabs write a string-valued header row at row 0.
        header_writes = [
            r
            for r in update_cells
            if r["updateCells"]["start"]["rowIndex"] == 0
            and "stringValue"
            in r["updateCells"]["rows"][0]["values"][0].get("userEnteredValue", {})
        ]
        assert len(header_writes) == 4

    def test_tasks_header_matches_schema(self):
        reqs = bootstrap_requests()
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            if uc["start"]["rowIndex"] != 0:
                continue
            row0_values = [v["userEnteredValue"]["stringValue"] for v in uc["rows"][0]["values"]]
            if row0_values[: len(TASKS_TAB.columns)] == list(TASKS_TAB.header_row):
                return
        raise AssertionError("Tasks header row not found in updateCells requests")


class TestDashboardLayout:
    """The Dashboard tab is written entirely by formula via ``updateCells`` —
    greeting, last-sync, KPI labels/values, three list sections, by-subject."""

    def _dash_writes(self, reqs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            r for r in _by_kind(reqs, "updateCells") if r["updateCells"]["start"]["sheetId"] == 0
        ]

    def _formulas_at(self, reqs: list[dict[str, Any]], row_index: int) -> list[str]:
        for r in self._dash_writes(reqs):
            uc = r["updateCells"]
            if uc["start"]["rowIndex"] != row_index:
                continue
            return [
                v.get("userEnteredValue", {}).get("formulaValue", "")
                for v in uc["rows"][0]["values"]
            ]
        return []

    def _strings_at(self, reqs: list[dict[str, Any]], row_index: int) -> list[str]:
        for r in self._dash_writes(reqs):
            uc = r["updateCells"]
            if uc["start"]["rowIndex"] != row_index:
                continue
            return [
                v.get("userEnteredValue", {}).get("stringValue", "")
                for v in uc["rows"][0]["values"]
            ]
        return []

    def test_greeting_formula_in_a1_references_settings(self):
        reqs = bootstrap_requests()
        # v4.2: greeting moved to row 2 (0-based 1) inside the top border row.
        formulas = self._formulas_at(reqs, 1)
        assert formulas, "no formula written to Dashboard greeting row"
        assert 'VLOOKUP("Child", Settings!A:B' in formulas[0]

    def test_kpi_tiles_are_scorecard_charts(self):
        """v4.2: KPIs render as 4 floating ``scorecardChart`` objects
        anchored at B3 with pure pixel offsets, not as cells inside
        the Dashboard grid."""
        reqs = bootstrap_requests()
        scorecards = [
            r
            for r in _by_kind(reqs, "addChart")
            if "scorecardChart" in r["addChart"]["chart"]["spec"]
        ]
        assert len(scorecards) == 4
        titles = {r["addChart"]["chart"]["spec"]["title"] for r in scorecards}
        assert titles == {"Overdue", "Due this week", "Upcoming", "Done this week"}
        # All anchored to Dashboard (sheetId 0) at cell B3 (row 2, col 1).
        # Position differentiation is by pixel offset, not by cell anchor.
        for r in scorecards:
            anchor = r["addChart"]["chart"]["position"]["overlayPosition"]["anchorCell"]
            assert anchor["sheetId"] == 0
            assert anchor["rowIndex"] == 2
            assert anchor["columnIndex"] == 1

    def test_scorecard_chart_sources_reference_dashboard_data_tab(self):
        """Scorecard values must pull from the hidden DashboardData tab,
        not from cells on the visible Dashboard grid."""
        reqs = bootstrap_requests()
        # Find DashboardData's allocated sheetId (last extra tab).
        add_sheets = _by_kind(reqs, "addSheet")
        data_sid = next(
            r["addSheet"]["properties"]["sheetId"]
            for r in add_sheets
            if r["addSheet"]["properties"]["title"] == "DashboardData"
        )
        scorecards = [
            r
            for r in _by_kind(reqs, "addChart")
            if "scorecardChart" in r["addChart"]["chart"]["spec"]
        ]
        for r in scorecards:
            src = r["addChart"]["chart"]["spec"]["scorecardChart"]["keyValueData"]["sourceRange"][
                "sources"
            ][0]
            assert src["sheetId"] == data_sid


class TestDashboardDataTab:
    """The hidden DashboardData helper tab seeds the 4 scorecard +
    donut chart sources with the KPI label/value pairs."""

    def test_dashboard_data_tab_seeded_with_kpi_formulas(self):
        reqs = bootstrap_requests()
        # Find DashboardData's sheetId.
        add_sheets = _by_kind(reqs, "addSheet")
        data_sid = next(
            r["addSheet"]["properties"]["sheetId"]
            for r in add_sheets
            if r["addSheet"]["properties"]["title"] == "DashboardData"
        )
        # The seed write puts the labels in col A and COUNTIF formulas
        # in col B, starting at row 2 (0-based 1).
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            start = uc.get("start", {})
            if start.get("sheetId") != data_sid or start.get("rowIndex") != 1:
                continue
            rows = uc["rows"]
            assert len(rows) == 4
            labels = [row["values"][0]["userEnteredValue"]["stringValue"] for row in rows]
            assert labels == ["Overdue", "Due this week", "Upcoming", "Done this week"]
            formulas = [row["values"][1]["userEnteredValue"]["formulaValue"] for row in rows]
            for f in formulas:
                assert f.startswith("=COUNTIF") and "Tasks!" in f
            return
        raise AssertionError("DashboardData seed write not emitted")

    def test_dashboard_data_tab_seeded_with_subjects_query(self):
        """The Subjects donut sources from a spilled QUERY at row 7
        (0-based 6) col A of DashboardData. The seed must include a
        single ``updateCells`` placing that formula."""
        reqs = bootstrap_requests()
        add_sheets = _by_kind(reqs, "addSheet")
        data_sid = next(
            r["addSheet"]["properties"]["sheetId"]
            for r in add_sheets
            if r["addSheet"]["properties"]["title"] == "DashboardData"
        )
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            start = uc.get("start", {})
            if (
                start.get("sheetId") != data_sid
                or start.get("rowIndex") != 6
                or start.get("columnIndex") != 0
            ):
                continue
            rows = uc["rows"]
            assert len(rows) == 1
            assert len(rows[0]["values"]) == 1
            formula = rows[0]["values"][0]["userEnteredValue"]["formulaValue"]
            assert formula.startswith("=QUERY(Tasks!A2:F")
            assert "group by A" in formula
            # Filter must mirror the dashboard tile semantics: always
            # exclude Archived; only count Submitted/Graded if Due is
            # within the last 7 days (matches Done-this-week section).
            assert "Archived" in formula
            assert "Submitted" in formula
            assert "Graded" in formula
            assert "TODAY()-7" in formula
            return
        raise AssertionError("DashboardData Subjects QUERY seed not emitted")


class TestDashboardFormats:
    def _dash_format_reqs(self, reqs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # Dashboard is sheetId=0; column-format ``repeatCell`` requests use
        # ``startRowIndex=1`` (skip header), but Dashboard formats target
        # explicit single rows (0..47). Filter by sheetId 0.
        return [r for r in _by_kind(reqs, "repeatCell") if r["repeatCell"]["range"]["sheetId"] == 0]

    def test_greeting_row_has_large_bold_font(self):
        reqs = bootstrap_requests()
        for r in self._dash_format_reqs(reqs):
            rng = r["repeatCell"]["range"]
            # v4.2: greeting at row 2 (0-based 1) inside top border row.
            if rng["startRowIndex"] != 1:
                continue
            fmt = r["repeatCell"]["cell"]["userEnteredFormat"]["textFormat"]
            assert fmt.get("bold") is True
            assert fmt.get("fontSize", 0) >= 18
            return
        raise AssertionError("greeting row format not emitted")

    def test_kpi_scorecards_use_theme_color_foreground(self):
        """v4: KPI value foreground colours use ``themeColor`` so the
        spreadsheet theme drives the urgency palette."""
        reqs = bootstrap_requests()
        scorecards = [
            r
            for r in _by_kind(reqs, "addChart")
            if "scorecardChart" in r["addChart"]["chart"]["spec"]
        ]
        assert len(scorecards) == 4
        for r in scorecards:
            fg = r["addChart"]["chart"]["spec"]["scorecardChart"]["keyValueFormat"]["textFormat"][
                "foregroundColorStyle"
            ]
            assert "themeColor" in fg

    def test_theme_colors_used_for_dashboard_colors(self):
        """v2 colour comes from spreadsheet theme — every backgroundColor /
        foreground colour field on the Dashboard must use ``themeColor``
        rather than literal RGB so the kid can repaint via theme switch.
        Conditional-format rules + banding are excluded — those API surfaces
        don't accept colorStyle and must use plain RGB."""
        reqs = bootstrap_requests()
        for r in self._dash_format_reqs(reqs):
            uf = r["repeatCell"]["cell"]["userEnteredFormat"]
            bg_style = uf.get("backgroundColorStyle")
            if bg_style is not None:
                assert "themeColor" in bg_style, bg_style
            fg_style = uf.get("textFormat", {}).get("foregroundColorStyle")
            if fg_style is not None:
                assert "themeColor" in fg_style, fg_style
            # Plain colour fields must NOT be used on Dashboard repeatCell.
            assert "backgroundColor" not in uf
            assert "foregroundColor" not in uf.get("textFormat", {})


class TestRefreshLayoutRequests:
    def test_renames_first_tab_to_dashboard(self):
        reqs = refresh_layout_requests()
        rename = next(r for r in reqs if "updateSheetProperties" in r)
        props = rename["updateSheetProperties"]["properties"]
        assert props["sheetId"] == 0
        assert props["title"] == "Dashboard"

    def test_grows_grid_to_fit_layout(self):
        """Legacy Today tabs are single-cell (rowCount=1, columnCount=1).
        The refresh must size the grid for the v5.0 4-row frame; publish
        later grows it to fit the lists region."""
        reqs = refresh_layout_requests()
        grid_resizes = [
            r
            for r in reqs
            if "updateSheetProperties" in r
            and "rowCount" in r["updateSheetProperties"]["properties"].get("gridProperties", {})
        ]
        assert len(grid_resizes) == 1
        gp = grid_resizes[0]["updateSheetProperties"]["properties"]["gridProperties"]
        # v5.0: 4-row visible frame (top border + greeting + graphics + bottom
        # border) — but the grid is pre-sized to 1000 rows so per-publish
        # ``addTable`` calls always fit within existing bounds. 6 cols wide
        # (A and F are 8px border columns).
        assert gp["rowCount"] == 1000
        assert gp["columnCount"] == 6

    def test_clears_dashboard_layout_area(self):
        reqs = refresh_layout_requests()
        # v5.0 clear covers A1:F4 (the frame only).
        clears = [
            r
            for r in _by_kind(reqs, "updateCells")
            if "range" in r["updateCells"]
            and r["updateCells"]["range"]["sheetId"] == 0
            and r["updateCells"]["range"]["endRowIndex"] == 4
            and r["updateCells"]["range"]["endColumnIndex"] == 6
        ]
        assert len(clears) == 1

    def test_reemits_dashboard_layout(self):
        reqs = refresh_layout_requests()
        # Greeting formula must be present (now at row 1, not row 0).
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            if "start" not in uc:
                continue
            if uc["start"].get("sheetId") != 0 or uc["start"].get("rowIndex") != 1:
                continue
            cell = uc["rows"][0]["values"][0]
            if 'VLOOKUP("Child"' in cell.get("userEnteredValue", {}).get("formulaValue", ""):
                return
        raise AssertionError("greeting formula not re-emitted by refresh_layout_requests")

    def test_refreshes_dropdowns_on_table_tabs(self):
        reqs = refresh_layout_requests()
        update_tables = [r for r in reqs if "updateTable" in r]
        # Tasks, History, UserEdits — three table-backed tabs.
        assert len(update_tables) == 3
        for r in update_tables:
            # Fields mask is either "columnProperties" alone (no sheetId
            # override available) or "columnProperties,range" (range widened
            # to cover the current schema column count — needed when the
            # schema grows a column relative to the live table).
            assert r["updateTable"]["fields"] in (
                "columnProperties",
                "columnProperties,range",
            )

    def test_adds_missing_schema_tabs(self):
        """If the live spreadsheet pre-dates a schema tab (e.g.
        ``DashboardData`` was added later), the refresh must emit an
        ``addSheet`` for the missing tab so the new layout's chart data
        source materialises."""
        # Pretend the live spreadsheet has Dashboard + the 4 legacy tabs
        # but is missing DashboardData.
        existing = ["Dashboard", "Tasks", "History", "Settings", "UserEdits"]
        reqs = refresh_layout_requests(existing_tab_names=existing)
        add_sheets = [r["addSheet"]["properties"]["title"] for r in _by_kind(reqs, "addSheet")]
        assert add_sheets == ["DashboardData"]

    def test_no_addsheet_when_all_tabs_present(self):
        """If every schema tab is already on the live spreadsheet, the
        refresh must not emit any ``addSheet`` request."""
        existing = [
            "Dashboard",
            "Tasks",
            "History",
            "Settings",
            "UserEdits",
            "DashboardData",
        ]
        reqs = refresh_layout_requests(existing_tab_names=existing)
        assert _by_kind(reqs, "addSheet") == []


class TestNativeTables:
    def test_one_addtable_per_table_tab(self):
        reqs = bootstrap_requests()
        names = _addtable_names(reqs)
        assert sorted(names) == sorted(["tbl_tasks", "tbl_history", "tbl_user_edits"])

    def test_table_range_includes_seed_row(self):
        reqs = bootstrap_requests()
        for r in _by_kind(reqs, "addTable"):
            rng = r["addTable"]["table"]["range"]
            assert rng["startRowIndex"] == 0
            assert rng["endRowIndex"] == 2  # header + 1 seed row

    def test_table_column_properties_typed(self):
        reqs = bootstrap_requests()
        tasks_table = next(
            r for r in _by_kind(reqs, "addTable") if r["addTable"]["table"]["name"] == "tbl_tasks"
        )
        cols = tasks_table["addTable"]["table"]["columnProperties"]
        by_name = {c["columnName"]: c for c in cols}
        assert by_name["Due"]["columnType"] == "DATE"
        assert by_name["Type"]["columnType"] == "DROPDOWN"
        assert "dataValidationRule" in by_name["Type"]
        assert by_name["Status"]["columnType"] == "DROPDOWN"
        assert "dataValidationRule" in by_name["Status"]


class TestSeedRow:
    def test_seed_row_written_for_each_table_tab(self):
        reqs = bootstrap_requests()
        # Seed rows are row-2 writes on table-backed tabs only (sheetId != 0;
        # Dashboard is sheetId 0 and emits its own row-2 last-sync write).
        # NOTE: DashboardData's helper data write also lands at rowIndex=1
        # (4 rows starting at row 2) — that's not a Table seed, it's the
        # KPI helper cells. Filter to writes whose sheet has a Table.
        table_sids = {
            r["addTable"]["table"]["range"]["sheetId"] for r in _by_kind(reqs, "addTable")
        }
        seeds = [
            r
            for r in _by_kind(reqs, "updateCells")
            if r["updateCells"]["start"]["rowIndex"] == 1
            and r["updateCells"]["start"]["sheetId"] in table_sids
        ]
        # 3 table tabs (Tasks, History, UserEdits)
        assert len(seeds) == 3

    def test_tasks_seed_includes_days_formula(self):
        reqs = bootstrap_requests()
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            if uc["start"]["rowIndex"] != 1:
                continue
            for cell in uc["rows"][0]["values"]:
                f = cell.get("userEnteredValue", {}).get("formulaValue", "")
                if "TODAY()" in f and "D2" in f:
                    return
        raise AssertionError("Days formula not seeded on Tasks row 2")


class TestDropdowns:
    def test_dropdowns_enforced_via_table_column_properties(self):
        """All dropdowns live in table-backed tabs, so they're enforced
        via ``addTable.columnProperties[].dataValidationRule`` rather
        than standalone ``setDataValidation`` requests (which the API
        rejects on cells inside typed table columns)."""
        reqs = bootstrap_requests()
        # No standalone setDataValidation — every dropdown column is in a Table.
        assert _by_kind(reqs, "setDataValidation") == []
        # And the Tasks addTable carries 4 DROPDOWN column properties with
        # ONE_OF_LIST validation rules (Type, Status, Source).
        tasks_table = next(
            r["addTable"]["table"]
            for r in _by_kind(reqs, "addTable")
            if r["addTable"]["table"]["name"] == "tbl_tasks"
        )
        dropdown_cols = [
            c for c in tasks_table["columnProperties"] if c.get("columnType") == "DROPDOWN"
        ]
        assert len(dropdown_cols) == 3
        for c in dropdown_cols:
            cond = c["dataValidationRule"]["condition"]
            assert cond["type"] == "ONE_OF_LIST"
            assert all("userEnteredValue" in v for v in cond["values"])

    def test_no_setdatavalidation_inside_tables(self):
        """Sanity check: ``_apply_dropdowns`` must skip table tabs."""
        reqs = bootstrap_requests()
        assert _by_kind(reqs, "setDataValidation") == []


class TestColumnFormats:
    def test_date_columns_get_date_format(self):
        reqs = bootstrap_requests()
        date_formats = [
            r
            for r in _by_kind(reqs, "repeatCell")
            if r["repeatCell"]["cell"]
            .get("userEnteredFormat", {})
            .get("numberFormat", {})
            .get("type")
            == "DATE"
        ]
        # Tasks.Due + History.Due = 2 date columns
        assert len(date_formats) == 2
        # All use dd/MM/yyyy (uppercase MM = months; lowercase mm = minutes)
        for r in date_formats:
            pattern = r["repeatCell"]["cell"]["userEnteredFormat"]["numberFormat"]["pattern"]
            assert pattern == "dd/MM/yyyy", f"Expected dd/MM/yyyy, got {pattern!r}"

    def test_checkbox_columns_get_boolean_validation(self):
        reqs = bootstrap_requests()
        checkboxes = [
            r
            for r in _by_kind(reqs, "repeatCell")
            if r["repeatCell"]["cell"].get("dataValidation", {}).get("condition", {}).get("type")
            == "BOOLEAN"
        ]
        # No checkbox columns in the new schema
        assert len(checkboxes) == 0


class TestColumnWidths:
    def test_widths_emitted_only_for_columns_with_width_px(self):
        reqs = bootstrap_requests()
        # Filter to COLUMN dim-prop updates that set ``pixelSize``; the
        # Dashboard also emits ROW dim-prop updates (custom row heights)
        # and COLUMN ``hiddenByUser`` updates (hide chart helper / gulf).
        width_reqs = [
            r
            for r in _by_kind(reqs, "updateDimensionProperties")
            if r["updateDimensionProperties"]["range"]["dimension"] == "COLUMNS"
            and "pixelSize" in r["updateDimensionProperties"]["fields"]
        ]
        expected = sum(1 for tab in SCHEMA.tabs for c in tab.columns if c.width_px is not None)
        assert len(width_reqs) == expected


class TestTabProperties:
    def test_user_edits_tab_hidden(self):
        reqs = bootstrap_requests()
        hidden = [
            r
            for r in _by_kind(reqs, "updateSheetProperties")
            if r["updateSheetProperties"]["properties"].get("hidden") is True
        ]
        # UserEdits + DashboardData are both hidden helper tabs.
        assert len(hidden) == 2

    def test_frozen_row_set_for_tabs_with_frozen_rows(self):
        reqs = bootstrap_requests()
        frozen = [
            r
            for r in _by_kind(reqs, "updateSheetProperties")
            if "frozenRowCount"
            in r["updateSheetProperties"]["properties"].get("gridProperties", {})
        ]
        # Tabs with frozen_rows != 0 emit a frozenRowCount update.
        # Dashboard and DashboardData both have frozen_rows=0; the rest have 1.
        expected = sum(1 for t in SCHEMA.tabs if t.frozen_rows)
        assert len(frozen) == expected


class TestRequestOrdering:
    def test_addsheet_precedes_anything_targeting_it(self):
        reqs = bootstrap_requests()
        first_seen: dict[int, int] = {}
        for i, r in enumerate(reqs):
            key = next(iter(r.keys()))
            target_id: int | None = None
            if key == "addSheet":
                target_id = r["addSheet"]["properties"]["sheetId"]
            elif key in {"updateCells", "repeatCell", "updateDimensionProperties"}:
                rng = r[key].get("range") or r[key].get("start")
                if rng:
                    target_id = rng["sheetId"]
            elif key == "setDataValidation":
                target_id = r["setDataValidation"]["range"]["sheetId"]
            elif key == "addTable":
                target_id = r["addTable"]["table"]["range"]["sheetId"]
            elif key == "updateSheetProperties":
                target_id = r["updateSheetProperties"]["properties"]["sheetId"]
            if target_id is None or target_id == 0:
                continue
            if key == "addSheet":
                first_seen[target_id] = i
            else:
                # Any non-addSheet request targeting this id must come after addSheet
                assert (
                    target_id in first_seen
                ), f"Request {i} ({key}) targets sheetId {target_id} before its addSheet"

    def test_addtable_after_seed_row_write(self):
        reqs = bootstrap_requests()
        for r_idx, r in enumerate(reqs):
            if "addTable" not in r:
                continue
            sid = r["addTable"]["table"]["range"]["sheetId"]
            # Find the seed write (row 2) for this sheet
            for prev_idx in range(r_idx):
                prev = reqs[prev_idx]
                if "updateCells" not in prev:
                    continue
                start = prev["updateCells"]["start"]
                if start["sheetId"] == sid and start["rowIndex"] == 1:
                    break
            else:
                raise AssertionError(
                    f"addTable for sheetId {sid} emitted without preceding seed row"
                )


class TestDashboardV2Visuals:
    """Dashboard v2 adds: hidden gridlines, merged greeting banner,
    section-header bands, a donut chart, alternating-row banding on list
    slabs, and conditional-format status pills."""

    def test_dashboard_hides_gridlines(self):
        reqs = bootstrap_requests()
        hide = [
            r
            for r in _by_kind(reqs, "updateSheetProperties")
            if r["updateSheetProperties"]["properties"]
            .get("gridProperties", {})
            .get("hideGridlines")
            is True
        ]
        assert len(hide) == 1
        assert hide[0]["updateSheetProperties"]["properties"]["sheetId"] == 0

    def test_greeting_row_merged(self):
        reqs = bootstrap_requests()
        merges = [
            r for r in _by_kind(reqs, "mergeCells") if r["mergeCells"]["range"]["sheetId"] == 0
        ]
        # v5.0: only the greeting merge is template-owned; section headers
        # + footer merges live in dashboard_layout and are emitted at publish.
        assert len(merges) == 1
        # Greeting is at row 1 (0-based) and spans cols B:E (1..5) within
        # the bordered canvas; the graphics band sits on row 2 below
        # (charts floating with pixel offsets, not depending on cell ranges).
        greeting = merges[0]["mergeCells"]["range"]
        assert greeting["startRowIndex"] == 1
        assert greeting["startColumnIndex"] == 1
        assert greeting["endColumnIndex"] == 5
        assert greeting["endRowIndex"] == 2

    def test_donut_chart_added(self):
        reqs = bootstrap_requests()
        # v5.1: 4 scorecard charts + 2 donut charts (Status, Subjects) = 6
        # total addChart requests.
        charts = _by_kind(reqs, "addChart")
        assert len(charts) == 6
        donuts = [r for r in charts if "pieChart" in r["addChart"]["chart"]["spec"]]
        assert len(donuts) == 2
        # Both donuts require a non-trivial pieHole.
        for d in donuts:
            assert d["addChart"]["chart"]["spec"]["pieChart"]["pieHole"] >= 0.5
            # Anchored to the Dashboard tab.
            anchor = d["addChart"]["chart"]["position"]["overlayPosition"]["anchorCell"]
            assert anchor["sheetId"] == 0
        titles = {d["addChart"]["chart"]["spec"]["title"] for d in donuts}
        assert titles == {"Status", "Subjects"}

    def test_donut_chart_sources_reference_dashboard_data_tab(self):
        """Both donut charts source their data from the hidden
        DashboardData tab, not from the Dashboard grid."""
        reqs = bootstrap_requests()
        add_sheets = _by_kind(reqs, "addSheet")
        data_sid = next(
            r["addSheet"]["properties"]["sheetId"]
            for r in add_sheets
            if r["addSheet"]["properties"]["title"] == "DashboardData"
        )
        donuts = [
            r for r in _by_kind(reqs, "addChart") if "pieChart" in r["addChart"]["chart"]["spec"]
        ]
        for donut in donuts:
            pie = donut["addChart"]["chart"]["spec"]["pieChart"]
            domain_src = pie["domain"]["sourceRange"]["sources"][0]
            series_src = pie["series"]["sourceRange"]["sources"][0]
            assert domain_src["sheetId"] == data_sid
            assert series_src["sheetId"] == data_sid

    def test_all_dashboard_charts_anchored_at_a2(self):
        """v5.1: every floating chart on the Dashboard tab (4 scorecards
        + 2 donuts) anchors at cell B3 (rowIndex=2, columnIndex=1), which
        is the first inner cell of the bordered graphics band. All
        positioning is via explicit pixel offsets — never via cell ranges."""
        reqs = bootstrap_requests()
        dash_charts = [
            r
            for r in _by_kind(reqs, "addChart")
            if r["addChart"]["chart"]["position"]["overlayPosition"]["anchorCell"]["sheetId"] == 0
        ]
        assert len(dash_charts) == 6
        for r in dash_charts:
            pos = r["addChart"]["chart"]["position"]["overlayPosition"]
            anchor = pos["anchorCell"]
            assert anchor["rowIndex"] == 2
            assert anchor["columnIndex"] == 1
            # Pixel layout must be fully explicit: w/h + offsets all present.
            assert "widthPixels" in pos
            assert "heightPixels" in pos
            assert "offsetXPixels" in pos
            assert "offsetYPixels" in pos

    def test_chart_pixel_layout_has_consistent_padding(self):
        """v4.3 layout invariant: charts have 8px internal padding on
        the top + bottom of the graphics band (band height 188 =
        8 + 82 + 8 + 82 + 8). Adjacent charts have at least an 8px gap
        on at least one axis."""
        reqs = bootstrap_requests()
        rects: list[tuple[int, int, int, int]] = []
        for r in _by_kind(reqs, "addChart"):
            pos = r["addChart"]["chart"]["position"]["overlayPosition"]
            if pos["anchorCell"]["sheetId"] != 0:
                continue
            x = pos["offsetXPixels"]
            y = pos["offsetYPixels"]
            w = pos["widthPixels"]
            h = pos["heightPixels"]
            rects.append((x, y, w, h))
        assert len(rects) == 6
        # All charts at or below the 8px top pad; never above origin.
        for x, y, _w, _h in rects:
            assert x >= 0, f"chart at x={x} crosses left edge"
            assert y >= 8, f"chart at y={y} missing top padding"
        # No two charts overlap, and the smallest gap between any pair is
        # at least 8px on at least one axis.
        for i, (x1, y1, w1, h1) in enumerate(rects):
            for j, (x2, y2, w2, h2) in enumerate(rects):
                if i >= j:
                    continue
                # Compute horizontal/vertical gaps (negative = overlap).
                gap_x = max(x1, x2) - min(x1 + w1, x2 + w2)
                gap_y = max(y1, y2) - min(y1 + h1, y2 + h2)
                assert (
                    gap_x >= 8 or gap_y >= 8
                ), f"charts {i} and {j} have insufficient padding: gap_x={gap_x} gap_y={gap_y}"

    def test_dashboard_row_heights_are_purposeful(self):
        """v5.0 frame invariant: exactly four row-height writes on the
        Dashboard tab — 8px top border, 56px greeting, 188px graphics
        band, 8px bottom border. The lists region's content-row heights
        are owned by dashboard_layout and emitted at publish time."""
        reqs = bootstrap_requests()
        row_height_writes = [
            r
            for r in _by_kind(reqs, "updateDimensionProperties")
            if r["updateDimensionProperties"]["range"]["sheetId"] == 0
            and r["updateDimensionProperties"]["range"]["dimension"] == "ROWS"
        ]
        assert (
            len(row_height_writes) == 4
        ), f"expected 4 row-height writes on Dashboard, got {len(row_height_writes)}"
        sizes = sorted(
            r["updateDimensionProperties"]["properties"]["pixelSize"] for r in row_height_writes
        )
        # Two 8px borders, 56px greeting, 188px graphics.
        assert sizes == [8, 8, 56, 188]


class TestRefreshLayoutTearsDownArtefacts:
    """``refresh_layout_requests`` must delete pre-existing charts,
    bandings, merges and conditional rules before re-adding fresh ones,
    otherwise reapply stacks duplicates onto every existing sheet."""

    def test_emits_delete_embedded_object_per_chart(self):
        reqs = refresh_layout_requests(existing_chart_ids=[10, 20])
        deletes = _by_kind(reqs, "deleteEmbeddedObject")
        assert {d["deleteEmbeddedObject"]["objectId"] for d in deletes} == {10, 20}

    def test_emits_delete_banding_per_banded_range(self):
        reqs = refresh_layout_requests(existing_banded_range_ids=[7, 8, 9, 10])
        deletes = _by_kind(reqs, "deleteBanding")
        assert {d["deleteBanding"]["bandedRangeId"] for d in deletes} == {7, 8, 9, 10}

    def test_emits_unmerge_per_existing_merge(self):
        ranges = [
            {
                "sheetId": 0,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": 5,
            },
        ]
        reqs = refresh_layout_requests(existing_merge_ranges=ranges)
        deletes = _by_kind(reqs, "unmergeCells")
        assert len(deletes) == 1
        assert deletes[0]["unmergeCells"]["range"] == ranges[0]

    def test_emits_delete_conditional_format_rule_per_existing(self):
        reqs = refresh_layout_requests(existing_conditional_format_rule_count=4)
        deletes = _by_kind(reqs, "deleteConditionalFormatRule")
        # All four deletions target index 0 (drain semantics).
        assert len(deletes) == 4
        for d in deletes:
            assert d["deleteConditionalFormatRule"]["index"] == 0
            assert d["deleteConditionalFormatRule"]["sheetId"] == 0


# --------------------------------------------------------------------------- #
# v5.1 graphics-bar thirds layout
# --------------------------------------------------------------------------- #


class TestGraphicsBarThirds:
    """v5.1 split the 1024px graphics band into three equal 336px thirds
    (8px gap between each): left = 2x2 tile grid (tiles 164x82), middle
    = Status donut (336x172), right = Subjects donut (336x172)."""

    def test_tiles_resized_to_one_sixth_width(self):
        reqs = bootstrap_requests()
        scorecards = [
            r
            for r in _by_kind(reqs, "addChart")
            if "scorecardChart" in r["addChart"]["chart"]["spec"]
        ]
        for r in scorecards:
            pos = r["addChart"]["chart"]["position"]["overlayPosition"]
            assert pos["widthPixels"] == 164
            assert pos["heightPixels"] == 82
        # Two distinct X offsets (left + right columns in the 2x2 grid).
        xs = sorted(
            {
                r["addChart"]["chart"]["position"]["overlayPosition"]["offsetXPixels"]
                for r in scorecards
            }
        )
        assert xs == [0, 172]
        # Two distinct Y offsets (top + bottom rows).
        ys = sorted(
            {
                r["addChart"]["chart"]["position"]["overlayPosition"]["offsetYPixels"]
                for r in scorecards
            }
        )
        assert ys == [8, 98]

    def test_status_donut_in_middle_third(self):
        reqs = bootstrap_requests()
        donut = next(
            r
            for r in _by_kind(reqs, "addChart")
            if "pieChart" in r["addChart"]["chart"]["spec"]
            and r["addChart"]["chart"]["spec"]["title"] == "Status"
        )
        pos = donut["addChart"]["chart"]["position"]["overlayPosition"]
        assert pos["offsetXPixels"] == 344
        assert pos["offsetYPixels"] == 8
        assert pos["widthPixels"] == 336
        assert pos["heightPixels"] == 172

    def test_subjects_donut_in_right_third(self):
        reqs = bootstrap_requests()
        donut = next(
            r
            for r in _by_kind(reqs, "addChart")
            if "pieChart" in r["addChart"]["chart"]["spec"]
            and r["addChart"]["chart"]["spec"]["title"] == "Subjects"
        )
        pos = donut["addChart"]["chart"]["position"]["overlayPosition"]
        assert pos["offsetXPixels"] == 688
        assert pos["offsetYPixels"] == 8
        assert pos["widthPixels"] == 336
        assert pos["heightPixels"] == 172

    def test_subjects_donut_sources_query_block(self):
        """The Subjects donut domain/series point at rows 6..25 (0-based,
        exclusive end) of DashboardData — the QUERY spill block."""
        reqs = bootstrap_requests()
        add_sheets = _by_kind(reqs, "addSheet")
        data_sid = next(
            r["addSheet"]["properties"]["sheetId"]
            for r in add_sheets
            if r["addSheet"]["properties"]["title"] == "DashboardData"
        )
        donut = next(
            r
            for r in _by_kind(reqs, "addChart")
            if "pieChart" in r["addChart"]["chart"]["spec"]
            and r["addChart"]["chart"]["spec"]["title"] == "Subjects"
        )
        pie = donut["addChart"]["chart"]["spec"]["pieChart"]
        assert pie["pieHole"] == 0.6
        assert pie["legendPosition"] == "RIGHT_LEGEND"
        domain = pie["domain"]["sourceRange"]["sources"][0]
        series = pie["series"]["sourceRange"]["sources"][0]
        assert domain["sheetId"] == data_sid
        assert domain["startRowIndex"] == 6
        assert domain["endRowIndex"] == 25
        assert domain["startColumnIndex"] == 0
        assert domain["endColumnIndex"] == 1
        assert series["sheetId"] == data_sid
        assert series["startRowIndex"] == 6
        assert series["endRowIndex"] == 25
        assert series["startColumnIndex"] == 1
        assert series["endColumnIndex"] == 2

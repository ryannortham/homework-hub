"""Tests for the sheet bootstrap template (request payload builder)."""

from __future__ import annotations

from homework_hub.sheet_template import (
    BY_SUBJECT_SHEET_ID,
    RAW_SHEET_ID,
    SETTINGS_SHEET_ID,
    TASKS_HEADERS,
    TASKS_SHEET_ID,
    TODAY_SHEET_ID,
    bootstrap_requests,
)
from homework_hub.sinks.sheets import RAW_HEADERS


class TestBootstrapStructure:
    def setup_method(self):
        self.requests = bootstrap_requests()

    def test_returns_non_empty_list_of_dicts(self):
        assert isinstance(self.requests, list)
        assert len(self.requests) > 0
        assert all(isinstance(r, dict) for r in self.requests)

    def test_renames_default_tab_to_today(self):
        renames = [
            r
            for r in self.requests
            if "updateSheetProperties" in r
            and r["updateSheetProperties"]["properties"].get("title") == "Today"
        ]
        assert len(renames) == 1
        assert renames[0]["updateSheetProperties"]["properties"]["sheetId"] == TODAY_SHEET_ID

    def test_creates_all_extra_tabs(self):
        added_titles = {
            r["addSheet"]["properties"]["title"] for r in self.requests if "addSheet" in r
        }
        assert added_titles == {"Tasks", "By Subject", "Raw", "Settings"}

    def test_extra_tabs_have_stable_ids(self):
        sheet_ids = {
            r["addSheet"]["properties"]["title"]: r["addSheet"]["properties"]["sheetId"]
            for r in self.requests
            if "addSheet" in r
        }
        assert sheet_ids["Tasks"] == TASKS_SHEET_ID
        assert sheet_ids["By Subject"] == BY_SUBJECT_SHEET_ID
        assert sheet_ids["Raw"] == RAW_SHEET_ID
        assert sheet_ids["Settings"] == SETTINGS_SHEET_ID

    def test_hides_raw_tab(self):
        hides = [
            r
            for r in self.requests
            if "updateSheetProperties" in r
            and r["updateSheetProperties"]["properties"].get("hidden") is True
        ]
        assert len(hides) == 1
        assert hides[0]["updateSheetProperties"]["properties"]["sheetId"] == RAW_SHEET_ID


class TestRawHeaders:
    def setup_method(self):
        self.requests = bootstrap_requests()

    def test_raw_tab_header_row_written(self):
        raw_writes = [
            r
            for r in self.requests
            if "updateCells" in r
            and r["updateCells"]["start"]["sheetId"] == RAW_SHEET_ID
            and r["updateCells"]["start"]["rowIndex"] == 0
        ]
        assert len(raw_writes) == 1
        cells = raw_writes[0]["updateCells"]["rows"][0]["values"]
        header_strings = [c["userEnteredValue"]["stringValue"] for c in cells]
        assert header_strings == RAW_HEADERS


class TestTasksFormulas:
    def setup_method(self):
        self.requests = bootstrap_requests()

    def test_tasks_header_row_includes_extras(self):
        header_writes = [
            r
            for r in self.requests
            if "updateCells" in r
            and r["updateCells"]["start"]["sheetId"] == TASKS_SHEET_ID
            and r["updateCells"]["start"]["rowIndex"] == 0
        ]
        assert len(header_writes) == 1
        cells = header_writes[0]["updateCells"]["rows"][0]["values"]
        names = [c["userEnteredValue"]["stringValue"] for c in cells]
        assert names == TASKS_HEADERS
        assert "manual_status" in names
        assert "effective_status" in names
        assert "days_left" in names

    def test_tasks_row2_pulls_from_raw_via_arrayformula(self):
        arr_writes = [
            r
            for r in self.requests
            if "updateCells" in r
            and r["updateCells"]["start"]["sheetId"] == TASKS_SHEET_ID
            and r["updateCells"]["start"]["rowIndex"] == 1
            and r["updateCells"]["start"]["columnIndex"] == 0
        ]
        assert len(arr_writes) == 1
        formula = arr_writes[0]["updateCells"]["rows"][0]["values"][0]["userEnteredValue"][
            "formulaValue"
        ]
        assert formula.startswith("=ARRAYFORMULA")
        assert "Raw!A2:A" in formula
        assert "Raw!L2:L" in formula

    def test_days_left_formula_exists(self):
        # Column R (index 17) on row 2
        writes = [
            r
            for r in self.requests
            if "updateCells" in r
            and r["updateCells"]["start"]["sheetId"] == TASKS_SHEET_ID
            and r["updateCells"]["start"]["rowIndex"] == 1
            and r["updateCells"]["start"]["columnIndex"] == 17
        ]
        assert len(writes) == 1
        formula = writes[0]["updateCells"]["rows"][0]["values"][0]["userEnteredValue"][
            "formulaValue"
        ]
        assert "TODAY()" in formula
        assert "DATEVALUE" in formula


class TestTodayDashboard:
    def setup_method(self):
        self.requests = bootstrap_requests()

    def test_today_tab_has_query_formulas_for_overdue_today_week(self):
        today_writes = [
            r
            for r in self.requests
            if "updateCells" in r and r["updateCells"]["start"]["sheetId"] == TODAY_SHEET_ID
        ]
        assert today_writes, "Expected at least one updateCells for Today tab"
        # Flatten all formulas on the Today tab
        formulas: list[str] = []
        for w in today_writes:
            for row in w["updateCells"]["rows"]:
                for cell in row.get("values", []):
                    val = cell.get("userEnteredValue", {})
                    if "formulaValue" in val:
                        formulas.append(val["formulaValue"])
        joined = " | ".join(formulas)
        assert "QUERY(Tasks" in joined
        # Overdue, today, this week and next week sections all referenced
        assert "R<0" in joined
        assert "R=0" in joined
        assert "R<=7" in joined
        assert "R<=14" in joined


class TestConditionalFormatting:
    def setup_method(self):
        self.requests = bootstrap_requests()
        self.rules = [
            r["addConditionalFormatRule"] for r in self.requests if "addConditionalFormatRule" in r
        ]

    def test_at_least_four_rules_added_on_tasks(self):
        assert len(self.rules) >= 4
        for r in self.rules:
            assert r["rule"]["ranges"][0]["sheetId"] == TASKS_SHEET_ID

    def test_overdue_rule_uses_red_background(self):
        overdue = [
            r
            for r in self.rules
            if r["rule"]["booleanRule"]["condition"]["values"][0]["userEnteredValue"] == "=$R2<0"
        ]
        assert len(overdue) == 1
        bg = overdue[0]["rule"]["booleanRule"]["format"]["backgroundColor"]
        # red dominant
        assert bg["red"] > bg["green"] and bg["red"] > bg["blue"]

    def test_submitted_rule_strikes_through(self):
        striked = [
            r
            for r in self.rules
            if r["rule"]["booleanRule"]["format"].get("textFormat", {}).get("strikethrough") is True
        ]
        assert len(striked) >= 1


class TestSettingsSeed:
    def setup_method(self):
        self.requests = bootstrap_requests()

    def test_settings_tab_has_three_source_rows(self):
        writes = [
            r
            for r in self.requests
            if "updateCells" in r and r["updateCells"]["start"]["sheetId"] == SETTINGS_SHEET_ID
        ]
        assert len(writes) == 1
        rows = writes[0]["updateCells"]["rows"]
        assert len(rows) == 4  # header + 3 sources
        sources = [r["values"][0]["userEnteredValue"]["stringValue"] for r in rows[1:]]
        assert sources == ["classroom", "compass", "edrolo"]

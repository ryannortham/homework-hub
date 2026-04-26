"""Tests for the medallion-aware bootstrap-sheet template (M5c).

Verifies that ``bootstrap_requests()`` emits a structurally-correct
``spreadsheets.batchUpdate`` body for the SCHEMA spec — the right tabs,
the right Tables, dropdowns, formats and hidden-tab flag. No live API
calls; pure dict assertions.
"""

from __future__ import annotations

from typing import Any

from homework_hub.schema import (
    DUPLICATES_TAB,
    SCHEMA,
    SETTINGS_TAB,
    TASKS_TAB,
    TODAY_TAB,
    USER_EDITS_TAB,
)
from homework_hub.sheet_template import bootstrap_requests


def _by_kind(reqs: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return [r for r in reqs if key in r]


def _addsheet_titles(reqs: list[dict[str, Any]]) -> list[str]:
    return [r["addSheet"]["properties"]["title"] for r in _by_kind(reqs, "addSheet")]


def _addtable_names(reqs: list[dict[str, Any]]) -> list[str]:
    return [r["addTable"]["table"]["name"] for r in _by_kind(reqs, "addTable")]


class TestTabCreation:
    def test_default_tab_renamed_to_first_schema_tab(self):
        reqs = bootstrap_requests()
        first_rename = _by_kind(reqs, "updateSheetProperties")[0]
        assert first_rename["updateSheetProperties"]["properties"]["sheetId"] == 0
        assert first_rename["updateSheetProperties"]["properties"]["title"] == TODAY_TAB.name

    def test_other_tabs_are_added(self):
        reqs = bootstrap_requests()
        titles = _addsheet_titles(reqs)
        # 4 extra tabs beyond the renamed first
        assert titles == [
            TASKS_TAB.name,
            DUPLICATES_TAB.name,
            SETTINGS_TAB.name,
            USER_EDITS_TAB.name,
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


class TestTodayFormula:
    def test_today_writes_query_formula_in_a1(self):
        reqs = bootstrap_requests()
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            if uc["start"]["sheetId"] != 0 or uc["start"]["rowIndex"] != 0:
                continue
            cell = uc["rows"][0]["values"][0]
            if "formulaValue" in cell.get("userEnteredValue", {}):
                assert "QUERY(tbl_tasks" in cell["userEnteredValue"]["formulaValue"]
                return
        raise AssertionError("Today QUERY formula not written to A1")


class TestNativeTables:
    def test_one_addtable_per_table_tab(self):
        reqs = bootstrap_requests()
        names = _addtable_names(reqs)
        assert sorted(names) == sorted(["tbl_tasks", "tbl_duplicates", "tbl_user_edits"])

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
        assert by_name["Done"]["columnType"] == "BOOLEAN"
        assert by_name["Priority"]["columnType"] == "DROPDOWN"
        assert "dataValidationRule" in by_name["Priority"]


class TestSeedRow:
    def test_seed_row_written_for_each_table_tab(self):
        reqs = bootstrap_requests()
        seeds = [
            r for r in _by_kind(reqs, "updateCells") if r["updateCells"]["start"]["rowIndex"] == 1
        ]
        # 3 table tabs (Tasks, Possible Duplicates, UserEdits)
        assert len(seeds) == 3

    def test_tasks_seed_includes_days_formula(self):
        reqs = bootstrap_requests()
        for r in _by_kind(reqs, "updateCells"):
            uc = r["updateCells"]
            if uc["start"]["rowIndex"] != 1:
                continue
            for cell in uc["rows"][0]["values"]:
                f = cell.get("userEnteredValue", {}).get("formulaValue", "")
                if "[@Due]" in f:
                    return
        raise AssertionError("Days formula not seeded on Tasks row 2")


class TestDropdowns:
    def test_dropdowns_set_for_dropdown_columns(self):
        reqs = bootstrap_requests()
        # 3 dropdown columns on Tasks: Status, Priority, Source
        dropdown_reqs = _by_kind(reqs, "setDataValidation")
        assert len(dropdown_reqs) == 3
        for r in dropdown_reqs:
            cond = r["setDataValidation"]["rule"]["condition"]
            assert cond["type"] == "ONE_OF_LIST"
            assert all("userEnteredValue" in v for v in cond["values"])

    def test_dropdown_starts_at_row_2(self):
        reqs = bootstrap_requests()
        for r in _by_kind(reqs, "setDataValidation"):
            assert r["setDataValidation"]["range"]["startRowIndex"] == 1


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
        # Tasks.Due + Possible Duplicates.compass_due + classroom_due
        assert len(date_formats) == 3

    def test_checkbox_columns_get_boolean_validation(self):
        reqs = bootstrap_requests()
        checkboxes = [
            r
            for r in _by_kind(reqs, "repeatCell")
            if r["repeatCell"]["cell"].get("dataValidation", {}).get("condition", {}).get("type")
            == "BOOLEAN"
        ]
        # Tasks.Done + Duplicates.Confirm + Duplicates.Dismiss
        assert len(checkboxes) == 3


class TestColumnWidths:
    def test_widths_emitted_only_for_columns_with_width_px(self):
        reqs = bootstrap_requests()
        width_reqs = _by_kind(reqs, "updateDimensionProperties")
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
        assert len(hidden) == 1

    def test_frozen_row_set_for_tabs_with_frozen_rows(self):
        reqs = bootstrap_requests()
        frozen = [
            r
            for r in _by_kind(reqs, "updateSheetProperties")
            if "gridProperties" in r["updateSheetProperties"]["properties"]
        ]
        # Today has frozen_rows=0; everything else defaults to 1
        assert len(frozen) == len(SCHEMA.tabs) - 1


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

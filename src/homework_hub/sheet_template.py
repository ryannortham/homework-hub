"""Bootstrap-sheet batchUpdate builder (M5c).

Generates the full ``spreadsheets.batchUpdate`` request list to turn a
freshly-``spreadsheets.create``d sheet into the medallion-shaped, native-
Sheets-Tables, kid-facing layout described by
:data:`homework_hub.schema.SCHEMA`.

Designed to be **pure**: this module imports nothing Google-API-related,
emits plain dicts, and is exhaustively unit-tested against the SCHEMA
spec. The live API call lives in :mod:`homework_hub.sinks.gold_sink`.

Order of operations is non-trivial because Sheets imposes constraints:

1. Rename the default sheet (id=0) to the first SCHEMA tab.
2. ``addSheet`` for every other tab with a deterministic sheetId.
3. ``updateCells`` write the header row of every tab.
4. Today tab: write the single QUERY formula into A1.
5. Tasks tab: seed row 2 with formula-column templates and blank cells
   for the other columns so the new Table will absorb them.
6. ``addTable`` for every tab whose ``TabSpec.table_id`` is non-empty.
   (Done after the seed row so the Table picks up at least one data row,
   which Sheets requires; the kid-facing UI hides the seed once real
   tasks land.)
7. Apply per-column metadata: type formats (DATE / NUMBER / CHECKBOX),
   dropdown DataValidation, column widths, frozen rows, hidden flag.
"""

from __future__ import annotations

from typing import Any

from homework_hub.schema import SCHEMA, ColumnKind, ColumnSpec, SheetSchema

# --------------------------------------------------------------------------- #
# sheetId allocation
# --------------------------------------------------------------------------- #
#
# The first sheet in a fresh spreadsheet always has sheetId 0; we rename it
# to the schema's first tab. Other tabs get deterministic ids starting at
# 1001 so the batchUpdate body can self-reference them without name lookups.

_FIRST_TAB_SHEET_ID = 0
_BASE_EXTRA_SHEET_ID = 1001


def _allocate_sheet_ids(schema: SheetSchema) -> dict[str, int]:
    """Map TabSpec.name → sheetId. First tab gets 0; rest get 1001+."""
    ids: dict[str, int] = {}
    for i, tab in enumerate(schema.tabs):
        ids[tab.name] = _FIRST_TAB_SHEET_ID if i == 0 else _BASE_EXTRA_SHEET_ID + (i - 1)
    return ids


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #


def bootstrap_requests(schema: SheetSchema = SCHEMA) -> list[dict[str, Any]]:
    """Return the full list of batchUpdate requests for a fresh sheet.

    The sole entry point for :class:`SheetsClient.create_sheet`. All
    helpers below are private.
    """
    sheet_ids = _allocate_sheet_ids(schema)
    requests: list[dict[str, Any]] = []
    requests.extend(_rename_default_tab(schema, sheet_ids))
    requests.extend(_add_extra_tabs(schema, sheet_ids))
    requests.extend(_write_headers(schema, sheet_ids))
    requests.extend(_write_today_formula(schema, sheet_ids))
    requests.extend(_seed_table_data_rows(schema, sheet_ids))
    requests.extend(_add_tables(schema, sheet_ids))
    requests.extend(_apply_column_formats(schema, sheet_ids))
    requests.extend(_apply_dropdowns(schema, sheet_ids))
    requests.extend(_set_column_widths(schema, sheet_ids))
    requests.extend(_apply_tab_properties(schema, sheet_ids))
    return requests


# --------------------------------------------------------------------------- #
# 1. Rename + 2. Add tabs
# --------------------------------------------------------------------------- #


def _rename_default_tab(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    first = schema.tabs[0]
    return [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": _FIRST_TAB_SHEET_ID, "title": first.name},
                "fields": "title",
            }
        }
    ]


def _add_extra_tabs(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for tab in schema.tabs[1:]:
        out.append(
            {
                "addSheet": {
                    "properties": {
                        "sheetId": sheet_ids[tab.name],
                        "title": tab.name,
                    }
                }
            }
        )
    return out


# --------------------------------------------------------------------------- #
# 3. Header rows
# --------------------------------------------------------------------------- #


def _write_headers(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for tab in schema.tabs:
        if not tab.columns:
            continue
        # Today tab has a single empty-header column (it's pure formula).
        if all(c.header == "" for c in tab.columns):
            continue
        out.append(
            _update_cells(
                sheet_ids[tab.name],
                row_index=0,
                column_index=0,
                rows=[[_string_cell(c.header) for c in tab.columns]],
                fields="userEnteredValue,userEnteredFormat",
            )
        )
    return out


# --------------------------------------------------------------------------- #
# 4. Today tab QUERY formula
# --------------------------------------------------------------------------- #


def _write_today_formula(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    """Today tab is a single QUERY formula in A1; no data rows."""
    out: list[dict[str, Any]] = []
    for tab in schema.tabs:
        if tab.table_id:
            continue
        formula_cols = [c for c in tab.columns if c.kind is ColumnKind.FORMULA]
        if len(tab.columns) == 1 and formula_cols:
            spec = formula_cols[0]
            out.append(
                _update_cells(
                    sheet_ids[tab.name],
                    row_index=0,
                    column_index=0,
                    rows=[[_formula_cell(spec.formula_template)]],
                    fields="userEnteredValue",
                )
            )
    return out


# --------------------------------------------------------------------------- #
# 5. Seed row 2 for table tabs
# --------------------------------------------------------------------------- #


def _seed_table_data_rows(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    """Sheets Tables require ≥1 data row at creation. Write a placeholder
    row (formulas filled in, other columns blank) so ``addTable`` succeeds.
    The publish layer overwrites this row on first sync.
    """
    out: list[dict[str, Any]] = []
    for tab in schema.tabs:
        if not tab.table_id:
            continue
        cells: list[dict[str, Any]] = []
        for col in tab.columns:
            if col.kind is ColumnKind.FORMULA:
                cells.append(_formula_cell(col.formula_template))
            elif col.kind is ColumnKind.CHECKBOX:
                cells.append(_bool_cell(False))
            else:
                cells.append(_string_cell(""))
        out.append(
            _update_cells(
                sheet_ids[tab.name],
                row_index=1,
                column_index=0,
                rows=[cells],
                fields="userEnteredValue",
            )
        )
    return out


# --------------------------------------------------------------------------- #
# 6. Native Sheets Tables (addTable)
# --------------------------------------------------------------------------- #


def _add_tables(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    """One ``addTable`` per tab with a non-empty ``table_id``.

    The Table covers headers + 1 seed row; Sheets auto-extends as rows are
    appended below. Column types are conveyed via ``columnProperties`` so
    the Table widget shows the right filter/sort affordances.
    """
    out: list[dict[str, Any]] = []
    for tab in schema.tabs:
        if not tab.table_id:
            continue
        out.append(
            {
                "addTable": {
                    "table": {
                        "name": tab.table_id,
                        "tableId": tab.table_id,
                        "range": {
                            "sheetId": sheet_ids[tab.name],
                            "startRowIndex": 0,
                            "endRowIndex": 2,
                            "startColumnIndex": 0,
                            "endColumnIndex": len(tab.columns),
                        },
                        "columnProperties": [
                            _table_column_properties(i, c) for i, c in enumerate(tab.columns)
                        ],
                    }
                }
            }
        )
    return out


def _table_column_properties(index: int, col: ColumnSpec) -> dict[str, Any]:
    """Map a ColumnSpec to a Sheets Table ``columnProperties`` entry.

    Sheets recognises a fixed set of ``columnType`` values:
    DOUBLE / TEXT / DATE / TIME / DATE_TIME / BOOLEAN / DROPDOWN / TAGS.
    """
    type_for_kind: dict[ColumnKind, str] = {
        ColumnKind.TEXT: "TEXT",
        ColumnKind.DATE: "DATE",
        ColumnKind.NUMBER: "DOUBLE",
        ColumnKind.CHECKBOX: "BOOLEAN",
        ColumnKind.DROPDOWN: "DROPDOWN",
        ColumnKind.FORMULA: "DOUBLE",  # Days = numeric; safe default
    }
    props: dict[str, Any] = {
        "columnIndex": index,
        "columnName": col.header,
        "columnType": type_for_kind[col.kind],
    }
    if col.kind is ColumnKind.DROPDOWN:
        props["dataValidationRule"] = {
            "condition": {
                "type": "ONE_OF_LIST",
                "values": [{"userEnteredValue": v} for v in col.dropdown_values],
            },
            "strict": True,
            "showCustomUi": True,
        }
    return props


# --------------------------------------------------------------------------- #
# 7. Per-column formats (DATE / NUMBER / CHECKBOX)
# --------------------------------------------------------------------------- #


def _apply_column_formats(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    """Set ``numberFormat`` (DATE / NUMBER) + ``dataValidation`` (CHECKBOX)
    on whole columns starting at row 2 so the header row keeps its plain
    text style.
    """
    out: list[dict[str, Any]] = []
    for tab in schema.tabs:
        for i, col in enumerate(tab.columns):
            if col.kind is ColumnKind.DATE:
                out.append(
                    _repeat_cell(
                        sheet_ids[tab.name],
                        column_index=i,
                        cell={
                            "userEnteredFormat": {
                                "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
                            }
                        },
                        fields="userEnteredFormat.numberFormat",
                    )
                )
            elif col.kind is ColumnKind.NUMBER or (
                col.kind is ColumnKind.FORMULA and col.key == "days"
            ):
                out.append(
                    _repeat_cell(
                        sheet_ids[tab.name],
                        column_index=i,
                        cell={
                            "userEnteredFormat": {
                                "numberFormat": {"type": "NUMBER", "pattern": "0"}
                            }
                        },
                        fields="userEnteredFormat.numberFormat",
                    )
                )
            elif col.kind is ColumnKind.CHECKBOX:
                out.append(
                    _repeat_cell(
                        sheet_ids[tab.name],
                        column_index=i,
                        cell={
                            "dataValidation": {
                                "condition": {"type": "BOOLEAN"},
                                "strict": True,
                            }
                        },
                        fields="dataValidation",
                    )
                )
    return out


# --------------------------------------------------------------------------- #
# 8. Dropdowns
# --------------------------------------------------------------------------- #


def _apply_dropdowns(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    """ONE_OF_LIST DataValidation per DROPDOWN column on row 2 onwards."""
    out: list[dict[str, Any]] = []
    for tab in schema.tabs:
        for i, col in enumerate(tab.columns):
            if col.kind is not ColumnKind.DROPDOWN:
                continue
            out.append(
                {
                    "setDataValidation": {
                        "range": {
                            "sheetId": sheet_ids[tab.name],
                            "startRowIndex": 1,
                            "startColumnIndex": i,
                            "endColumnIndex": i + 1,
                        },
                        "rule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [{"userEnteredValue": v} for v in col.dropdown_values],
                            },
                            "strict": True,
                            "showCustomUi": True,
                        },
                    }
                }
            )
    return out


# --------------------------------------------------------------------------- #
# 9. Column widths
# --------------------------------------------------------------------------- #


def _set_column_widths(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for tab in schema.tabs:
        for i, col in enumerate(tab.columns):
            if col.width_px is None:
                continue
            out.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_ids[tab.name],
                            "dimension": "COLUMNS",
                            "startIndex": i,
                            "endIndex": i + 1,
                        },
                        "properties": {"pixelSize": col.width_px},
                        "fields": "pixelSize",
                    }
                }
            )
    return out


# --------------------------------------------------------------------------- #
# 10. Tab-level properties (frozen rows, hidden)
# --------------------------------------------------------------------------- #


def _apply_tab_properties(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for tab in schema.tabs:
        props: dict[str, Any] = {"sheetId": sheet_ids[tab.name]}
        fields: list[str] = []
        if tab.frozen_rows:
            props["gridProperties"] = {"frozenRowCount": tab.frozen_rows}
            fields.append("gridProperties.frozenRowCount")
        if tab.hidden:
            props["hidden"] = True
            fields.append("hidden")
        if not fields:
            continue
        out.append(
            {
                "updateSheetProperties": {
                    "properties": props,
                    "fields": ",".join(fields),
                }
            }
        )
    return out


# --------------------------------------------------------------------------- #
# Cell helpers
# --------------------------------------------------------------------------- #


def _string_cell(value: str) -> dict[str, Any]:
    return {"userEnteredValue": {"stringValue": value}}


def _formula_cell(formula: str) -> dict[str, Any]:
    return {"userEnteredValue": {"formulaValue": formula}}


def _bool_cell(value: bool) -> dict[str, Any]:
    return {"userEnteredValue": {"boolValue": value}}


def _update_cells(
    sheet_id: int,
    *,
    row_index: int,
    column_index: int,
    rows: list[list[dict[str, Any]]],
    fields: str,
) -> dict[str, Any]:
    return {
        "updateCells": {
            "rows": [{"values": row} for row in rows],
            "fields": fields,
            "start": {
                "sheetId": sheet_id,
                "rowIndex": row_index,
                "columnIndex": column_index,
            },
        }
    }


def _repeat_cell(
    sheet_id: int,
    *,
    column_index: int,
    cell: dict[str, Any],
    fields: str,
) -> dict[str, Any]:
    """Apply ``cell`` formatting to every row (from row 2 down) of a column."""
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "startColumnIndex": column_index,
                "endColumnIndex": column_index + 1,
            },
            "cell": cell,
            "fields": fields,
        }
    }


__all__ = ["bootstrap_requests"]

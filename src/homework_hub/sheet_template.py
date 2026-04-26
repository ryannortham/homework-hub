"""Builds the batchUpdate requests for bootstrapping a new child's Google Sheet.

A fresh sheet (created via Sheets API `presentations.create`) starts with one
unnamed tab. We:

1. Rename it to `Today` (so the kid sees the dashboard when they open the sheet).
2. Add `Tasks`, `By Subject`, `Raw` (hidden) and `Settings` tabs.
3. Populate `Raw` headers (the only tab the script writes data into).
4. Populate `Tasks` with formulas that pull from `Raw` plus user-editable columns.
5. Populate `Today` with `QUERY()` formulas grouping by Overdue / Today / Week.
6. Apply conditional formatting to `Tasks` (red overdue, orange due-soon, etc.)
   and `Today`.
7. Hide the `Raw` tab.
8. Populate `Settings` with last-sync placeholders.

All requests are returned as plain dicts — easy to assert against in tests and
fed directly into `spreadsheets.batchUpdate`.
"""

from __future__ import annotations

from typing import Any

from homework_hub.sinks.sheets import RAW_HEADERS

# Stable sheetIds we assign so subsequent batchUpdate requests can target tabs
# by ID instead of fragile name lookups. The first tab in a new spreadsheet
# always has sheetId 0; we rename it rather than delete + recreate to avoid
# the "spreadsheets must have at least one sheet" race.
TODAY_SHEET_ID = 0
TASKS_SHEET_ID = 1001
BY_SUBJECT_SHEET_ID = 1002
RAW_SHEET_ID = 1003
SETTINGS_SHEET_ID = 1004

# Columns on the Tasks tab — formulas pulling from Raw plus editable extras.
# Column letters: A child, B source, C source_id, D subject, E title,
#                 F description, G assigned_at, H due_at, I status, J status_raw,
#                 K url, L last_synced, then editable: M manual_status,
#                 N priority, O time_estimate, P notes, Q effective_status,
#                 R days_left.
TASKS_HEADERS: list[str] = [
    *RAW_HEADERS,
    "manual_status",
    "priority",
    "time_estimate",
    "notes",
    "effective_status",
    "days_left",
]


def bootstrap_requests() -> list[dict[str, Any]]:
    """Return the full list of batchUpdate requests for a fresh sheet."""
    requests: list[dict[str, Any]] = []
    requests.extend(_rename_default_tab())
    requests.extend(_create_extra_tabs())
    requests.extend(_write_headers())
    requests.extend(_write_tasks_formulas())
    requests.extend(_write_today_formulas())
    requests.extend(_write_settings_seed())
    requests.extend(_apply_conditional_formatting())
    requests.extend(_hide_raw_tab())
    return requests


# --------------------------------------------------------------------------- #
# Tab creation
# --------------------------------------------------------------------------- #


def _rename_default_tab() -> list[dict[str, Any]]:
    return [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": TODAY_SHEET_ID, "title": "Today"},
                "fields": "title",
            }
        }
    ]


def _create_extra_tabs() -> list[dict[str, Any]]:
    return [
        {"addSheet": {"properties": {"sheetId": TASKS_SHEET_ID, "title": "Tasks"}}},
        {"addSheet": {"properties": {"sheetId": BY_SUBJECT_SHEET_ID, "title": "By Subject"}}},
        {"addSheet": {"properties": {"sheetId": RAW_SHEET_ID, "title": "Raw"}}},
        {"addSheet": {"properties": {"sheetId": SETTINGS_SHEET_ID, "title": "Settings"}}},
    ]


# --------------------------------------------------------------------------- #
# Headers + formulas
# --------------------------------------------------------------------------- #


def _write_headers() -> list[dict[str, Any]]:
    """Header row for Raw and Tasks. Today is dashboard-formatted separately."""
    return [
        _values_request(RAW_SHEET_ID, 0, 0, [_string_row(RAW_HEADERS)]),
        _values_request(TASKS_SHEET_ID, 0, 0, [_string_row(TASKS_HEADERS)]),
    ]


def _write_tasks_formulas() -> list[dict[str, Any]]:
    """Tasks tab: row 2 holds array formulas that mirror Raw + compute extras.

    A1-L1 already populated by _write_headers. Row 2 onward is filled by:
    - A2:L : ARRAYFORMULA pulling Raw!A2:L (so new Raw rows auto-appear)
    - M:P : left blank for kids to fill in
    - Q2 : ARRAYFORMULA effective_status (manual_status if set, else status,
           with overdue override when due_at < today and status not submitted)
    - R2 : ARRAYFORMULA days_left = INT(due_at - today)
    """
    pull = (
        '=ARRAYFORMULA(IF(Raw!A2:A="",,{'
        "Raw!A2:A,Raw!B2:B,Raw!C2:C,Raw!D2:D,Raw!E2:E,Raw!F2:F,"
        "Raw!G2:G,Raw!H2:H,Raw!I2:I,Raw!J2:J,Raw!K2:K,Raw!L2:L"
        "}))"
    )
    effective = (
        '=ARRAYFORMULA(IF(A2:A="",,IF(M2:M<>"",M2:M,'
        'IF(AND(H2:H<>"",DATEVALUE(LEFT(H2:H,10))<TODAY(),'
        'NOT(REGEXMATCH(I2:I,"submitted|graded"))),"overdue",I2:I))))'
    )
    days_left = '=ARRAYFORMULA(IF(H2:H="",,DATEVALUE(LEFT(H2:H,10))-TODAY()))'
    return [
        _formula_request(TASKS_SHEET_ID, 1, 0, [[pull]]),  # A2
        _formula_request(TASKS_SHEET_ID, 1, 16, [[effective]]),  # Q2
        _formula_request(TASKS_SHEET_ID, 1, 17, [[days_left]]),  # R2
    ]


def _write_today_formulas() -> list[dict[str, Any]]:
    """Today tab: KPI strip + four QUERY blocks (Overdue/Today/Week/Next)."""
    rows: list[list[dict[str, Any]]] = [
        [_string_cell("Today")],
        [],
        [_string_cell("Overdue"), _string_cell("Due Today"), _string_cell("Due This Week")],
        [
            _formula_cell(
                "=IFERROR(COUNTA(QUERY(Tasks!A2:R,\"select A where R<0 and Q!='submitted' "
                "and Q!='graded'\",0)),0)"
            ),
            _formula_cell(
                "=IFERROR(COUNTA(QUERY(Tasks!A2:R,\"select A where R=0 and Q!='submitted' "
                "and Q!='graded'\",0)),0)"
            ),
            _formula_cell(
                '=IFERROR(COUNTA(QUERY(Tasks!A2:R,"select A where R>=0 and R<=7 '
                "and Q!='submitted' and Q!='graded'\",0)),0)"
            ),
        ],
        [],
        [_string_cell("Overdue")],
        [
            _formula_cell(
                "=IFERROR(QUERY(Tasks!A2:R,"
                "\"select D,E,B,H,Q,R where R<0 and Q!='submitted' and Q!='graded' "
                "order by R asc label D 'Subject', E 'Task', B 'Source', H 'Due', "
                "Q 'Status', R 'Days'\",0),\"None — nice work!\")"
            )
        ],
        [],
        [_string_cell("Due Today")],
        [
            _formula_cell(
                "=IFERROR(QUERY(Tasks!A2:R,"
                "\"select D,E,B,H,Q where R=0 and Q!='submitted' and Q!='graded' "
                "order by D asc label D 'Subject', E 'Task', B 'Source', H 'Due', "
                'Q \'Status\'",0),"Nothing due today")'
            )
        ],
        [],
        [_string_cell("This Week")],
        [
            _formula_cell(
                "=IFERROR(QUERY(Tasks!A2:R,"
                "\"select D,E,B,H,Q,R where R>=1 and R<=7 and Q!='submitted' "
                "and Q!='graded' order by R asc, D asc label D 'Subject', E 'Task', "
                "B 'Source', H 'Due', Q 'Status', R 'Days'\",0),\"Nothing this week\")"
            )
        ],
        [],
        [_string_cell("Next Week")],
        [
            _formula_cell(
                "=IFERROR(QUERY(Tasks!A2:R,"
                "\"select D,E,B,H,Q,R where R>=8 and R<=14 and Q!='submitted' "
                "and Q!='graded' order by R asc, D asc label D 'Subject', E 'Task', "
                "B 'Source', H 'Due', Q 'Status', R 'Days'\",0),\"Nothing scheduled\")"
            )
        ],
    ]
    return [
        {
            "updateCells": {
                "rows": [{"values": row} for row in rows],
                "fields": "userEnteredValue,userEnteredFormat",
                "start": {"sheetId": TODAY_SHEET_ID, "rowIndex": 0, "columnIndex": 0},
            }
        }
    ]


def _write_settings_seed() -> list[dict[str, Any]]:
    rows = [
        ["Source", "Last sync", "Last error"],
        ["classroom", "", ""],
        ["compass", "", ""],
        ["edrolo", "", ""],
    ]
    return [_values_request(SETTINGS_SHEET_ID, 0, 0, [[_string_cell(c) for c in r] for r in rows])]


# --------------------------------------------------------------------------- #
# Conditional formatting
# --------------------------------------------------------------------------- #


def _apply_conditional_formatting() -> list[dict[str, Any]]:
    """Colour the Tasks tab by days_left (column R, index 17)."""
    rng = {
        "sheetId": TASKS_SHEET_ID,
        "startRowIndex": 1,
        "startColumnIndex": 0,
        "endColumnIndex": 18,
    }
    rules: list[dict[str, Any]] = [
        # Status submitted/graded → grey strike-through (highest priority)
        _format_rule(
            rng, '=REGEXMATCH($Q2,"submitted|graded")', bg=(0.93, 0.93, 0.93), strike=True, index=0
        ),
        # Days left < 0 → red
        _format_rule(rng, "=$R2<0", bg=(1.0, 0.85, 0.85), index=1),
        # Days left = 0 → orange
        _format_rule(rng, "=$R2=0", bg=(1.0, 0.92, 0.80), index=2),
        # Days left 1-2 → yellow
        _format_rule(rng, "=AND($R2>=1,$R2<=2)", bg=(1.0, 0.97, 0.80), index=3),
    ]
    return rules


def _format_rule(
    rng: dict[str, Any],
    formula: str,
    *,
    bg: tuple[float, float, float],
    strike: bool = False,
    index: int = 0,
) -> dict[str, Any]:
    text_format: dict[str, Any] = {}
    if strike:
        text_format["strikethrough"] = True
    fmt: dict[str, Any] = {
        "backgroundColor": {"red": bg[0], "green": bg[1], "blue": bg[2]},
    }
    if text_format:
        fmt["textFormat"] = text_format
    return {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [rng],
                "booleanRule": {
                    "condition": {
                        "type": "CUSTOM_FORMULA",
                        "values": [{"userEnteredValue": formula}],
                    },
                    "format": fmt,
                },
            },
            "index": index,
        }
    }


def _hide_raw_tab() -> list[dict[str, Any]]:
    return [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": RAW_SHEET_ID, "hidden": True},
                "fields": "hidden",
            }
        }
    ]


# --------------------------------------------------------------------------- #
# Cell helpers
# --------------------------------------------------------------------------- #


def _string_cell(value: str) -> dict[str, Any]:
    return {"userEnteredValue": {"stringValue": value}}


def _formula_cell(formula: str) -> dict[str, Any]:
    return {"userEnteredValue": {"formulaValue": formula}}


def _string_row(values: list[str]) -> list[dict[str, Any]]:
    return [_string_cell(v) for v in values]


def _values_request(
    sheet_id: int,
    row_index: int,
    column_index: int,
    rows: list[list[dict[str, Any]]],
) -> dict[str, Any]:
    return {
        "updateCells": {
            "rows": [{"values": row} for row in rows],
            "fields": "userEnteredValue",
            "start": {"sheetId": sheet_id, "rowIndex": row_index, "columnIndex": column_index},
        }
    }


def _formula_request(
    sheet_id: int,
    row_index: int,
    column_index: int,
    formulas: list[list[str]],
) -> dict[str, Any]:
    rows = [[_formula_cell(f) for f in row] for row in formulas]
    return _values_request(sheet_id, row_index, column_index, rows)

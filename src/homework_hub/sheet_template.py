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
4. Dashboard tab: write the full formula layout (greeting, KPI row,
   per-section list slabs, by-subject block) — every cell is its own
   formula expression.
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

# Dashboard tab is always the first tab; its sheetId is 0 (see
# ``_FIRST_TAB_SHEET_ID``). The constant below names it so callers don't
# repeat the magic string.
DASHBOARD_TAB_NAME = "Dashboard"

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

# Spreadsheet locale used for all bootstrapped sheets.  en_AU means date
# format patterns like "dd/mm/yyyy" are interpreted as day/month/year.
_LOCALE = "en_AU"


def _set_spreadsheet_locale() -> list[dict[str, Any]]:
    """Set the spreadsheet locale so date patterns render day-first (d/m/yyyy)."""
    return [
        {
            "updateSpreadsheetProperties": {
                "properties": {"locale": _LOCALE},
                "fields": "locale",
            }
        }
    ]


def bootstrap_requests(schema: SheetSchema = SCHEMA) -> list[dict[str, Any]]:
    """Return the full list of batchUpdate requests for a fresh sheet.

    The sole entry point for :class:`SheetsClient.create_sheet`. All
     helpers below are private.
    """
    sheet_ids = _allocate_sheet_ids(schema)
    requests: list[dict[str, Any]] = []
    requests.extend(_set_spreadsheet_locale())
    requests.extend(_rename_default_tab(schema, sheet_ids))
    requests.extend(_add_extra_tabs(schema, sheet_ids))
    requests.extend(_write_headers(schema, sheet_ids))
    requests.extend(_write_dashboard_data_rows(schema, sheet_ids))
    requests.extend(_write_dashboard_frame(schema, sheet_ids))
    requests.extend(_apply_dashboard_formats(schema, sheet_ids))
    requests.extend(_apply_dashboard_kpi_scorecards(schema, sheet_ids))
    requests.extend(_apply_dashboard_status_chart(schema, sheet_ids))
    requests.extend(_apply_dashboard_subjects_chart(schema, sheet_ids))
    requests.extend(_seed_table_data_rows(schema, sheet_ids))
    requests.extend(_add_tables(schema, sheet_ids))
    requests.extend(_apply_column_formats(schema, sheet_ids))
    requests.extend(_apply_dropdowns(schema, sheet_ids))
    requests.extend(_set_column_widths(schema, sheet_ids))
    requests.extend(_apply_tab_properties(schema, sheet_ids))
    return requests


def refresh_layout_requests(
    schema: SheetSchema = SCHEMA,
    *,
    sheet_id_overrides: dict[str, int] | None = None,
    existing_tab_names: list[str] | None = None,
    existing_chart_ids: list[int] | None = None,
    existing_banded_range_ids: list[int] | None = None,
    existing_merge_ranges: list[dict[str, Any]] | None = None,
    existing_conditional_format_rule_count: int = 0,
    existing_dashboard_table_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return batchUpdate requests to re-apply the Dashboard FRAME and
    refresh dropdown vocabularies on an EXISTING sheet without recreating it.

    Used by ``homework-hub reapply-template`` to push schema changes
    (renamed tabs, new formulas, new dropdown values, new helper tabs)
    to sheets that were bootstrapped before those changes existed.

    Since v5.0 the Dashboard's three task list Tables are emitted at
    publish time (see :mod:`homework_hub.dashboard_layout`), not at
    template time. This function therefore only re-emits the static
    frame: border rows/cols, greeting, graphics band with floating
    KPI scorecards + donut. The lists region and footer are left
    empty for the next ``publish`` to materialise.

    ``sheet_id_overrides`` maps tab title → live sheetId for sheets whose
    tabs no longer carry the IDs the schema-time allocator would have used
    (e.g. the legacy "Today" tab survives the rename to "Dashboard" but
    keeps whichever sheetId Sheets assigned it originally). The CLI looks
    these up via ``spreadsheets.get`` and passes them in. Tabs not in the
    override map fall back to the schema-time allocation.

    ``existing_tab_names`` is the list of tab titles currently present on
    the live spreadsheet. Any schema tab whose name isn't in that list
    triggers an ``addSheet`` request so new schema tabs (e.g.
    ``DashboardData``) materialise on existing sheets at refresh time.

    ``existing_dashboard_table_ids`` is the list of any pre-v5.0 Dashboard
    Tables that need tearing down before the frame is re-emitted.
    """
    sheet_ids = _allocate_sheet_ids(schema)
    if sheet_id_overrides:
        # The first tab's allocator-assigned id is 0; if the live sheet
        # carries a different id for that tab (because it pre-dates the
        # rename, or was deleted/recreated), the override takes precedence
        # so all downstream Dashboard requests target the right grid.
        # The override key may be either the legacy name ("Today") or the
        # new name ("Dashboard") — both map to the schema's first tab.
        first_tab_name = schema.tabs[0].name
        for legacy in ("Today", first_tab_name):
            if legacy in sheet_id_overrides:
                sheet_ids[first_tab_name] = sheet_id_overrides[legacy]
                break
    requests: list[dict[str, Any]] = []

    # 1. Rename first tab → schema.tabs[0].name (handles Today → Dashboard).
    requests.extend(_rename_default_tab(schema, sheet_ids))

    # 2. Add any schema tabs not yet present on the live spreadsheet.
    # First tab is always present (it's the renamed default sheet), so we
    # only check tabs[1:]. Headers + dropdowns + table-creation for new
    # tabs are emitted as part of the standard refresh path below.
    #
    # If the caller didn't supply ``existing_tab_names`` we assume every
    # schema tab is already present — callers without live metadata (e.g.
    # unit tests) shouldn't trigger spurious addSheet requests.
    if existing_tab_names is None:
        present: set[str] = {t.name for t in schema.tabs}
    else:
        present = set(existing_tab_names) | {schema.tabs[0].name}
    new_tabs: list[Any] = []
    for tab in schema.tabs[1:]:
        if tab.name in present:
            continue
        new_tabs.append(tab)
        requests.append(
            {
                "addSheet": {
                    "properties": {
                        "sheetId": sheet_ids[tab.name],
                        "title": tab.name,
                    }
                }
            }
        )

    # Newly-added tabs also need their headers, tab-level properties
    # (hidden flag, frozen rows), column widths, dropdowns and — if
    # table-backed — a seed row + addTable. We emit those here rather
    # than running the whole bootstrap path so an existing sheet's
    # already-correct tabs aren't disturbed.
    if new_tabs:
        new_schema = SheetSchema(tabs=tuple(new_tabs))
        requests.extend(_write_headers(new_schema, sheet_ids))
        requests.extend(_seed_table_data_rows(new_schema, sheet_ids))
        requests.extend(_add_tables(new_schema, sheet_ids))
        requests.extend(_apply_column_formats(new_schema, sheet_ids))
        requests.extend(_apply_dropdowns(new_schema, sheet_ids))
        requests.extend(_set_column_widths(new_schema, sheet_ids))
        requests.extend(_apply_tab_properties(new_schema, sheet_ids))

    dash = schema.tabs[0]

    # 3. Tear down stateful objects BEFORE resizing the grid. Shrinking
    # the grid auto-deletes any banded range / chart / merge that
    # referenced rows past the new bound, and a subsequent
    # ``deleteBanding`` referencing a now-vanished id 400s.
    # Order: CF rules FIRST (``deleteTable`` cascades and removes any
    # CF rule whose range targeted the table's cells, which would
    # invalidate our pre-fetched count), then dashboard tables, charts,
    # banded ranges, merges.
    for _ in range(existing_conditional_format_rule_count):
        requests.append(
            {
                "deleteConditionalFormatRule": {
                    "sheetId": sheet_ids[dash.name],
                    "index": 0,
                }
            }
        )
    for tid in existing_dashboard_table_ids or ():
        requests.append({"deleteTable": {"tableId": tid}})
    for cid in existing_chart_ids or ():
        requests.append({"deleteEmbeddedObject": {"objectId": cid}})
    for bid in existing_banded_range_ids or ():
        requests.append({"deleteBanding": {"bandedRangeId": bid}})
    for mrange in existing_merge_ranges or ():
        requests.append({"unmergeCells": {"range": mrange}})

    # 4. Resize the Dashboard grid to the frame minimum. The publish-time
    # dashboard_layout module re-grows the grid to fit the data Tables.
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_ids[dash.name],
                    "gridProperties": {
                        "rowCount": _DASH_GRID_ROW_COUNT,
                        "columnCount": _DASH_COL_COUNT,
                        "frozenRowCount": dash.frozen_rows,
                    },
                },
                "fields": (
                    "gridProperties.rowCount,gridProperties.columnCount,"
                    "gridProperties.frozenRowCount"
                ),
            }
        }
    )

    # 5. Clear the entire Dashboard grid so stale list-region content
    # from any previous template version is wiped. Publish re-grows the
    # grid and writes the lists region.
    requests.append(
        {
            "updateCells": {
                "range": {
                    "sheetId": sheet_ids[dash.name],
                    "startRowIndex": 0,
                    "endRowIndex": _DASH_FRAME_ROW_COUNT,
                    "startColumnIndex": 0,
                    "endColumnIndex": _DASH_COL_COUNT,
                },
                "fields": "userEnteredValue,userEnteredFormat",
            }
        }
    )

    # 6. Seed/refresh DashboardData helper cells, then re-emit the
    # Dashboard frame (greeting + graphics band charts). Lists region
    # + footer materialise on the next publish.
    requests.extend(_write_dashboard_data_rows(schema, sheet_ids))
    requests.extend(_write_dashboard_frame(schema, sheet_ids))
    requests.extend(_apply_dashboard_formats(schema, sheet_ids))
    requests.extend(_apply_dashboard_kpi_scorecards(schema, sheet_ids))
    requests.extend(_apply_dashboard_status_chart(schema, sheet_ids))
    requests.extend(_apply_dashboard_subjects_chart(schema, sheet_ids))

    # 7. Re-apply Dashboard column widths.
    for i, col in enumerate(dash.columns):
        if col.width_px is None:
            continue
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_ids[dash.name],
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1,
                    },
                    "properties": {"pixelSize": col.width_px},
                    "fields": "pixelSize",
                }
            }
        )

    # 8. Refresh dropdowns on Table-backed tabs (updateTable) and
    # standalone dropdowns on non-Table tabs (setDataValidation).
    # Skip tabs we just freshly added — addTable above already set the
    # dropdown column types and the standalone _apply_dropdowns pass
    # would re-emit setDataValidation rules that overlap.
    new_tab_names = {t.name for t in new_tabs}
    refresh_schema_tabs = tuple(t for t in schema.tabs if t.name not in new_tab_names)
    refresh_schema = SheetSchema(tabs=refresh_schema_tabs) if refresh_schema_tabs else schema
    for tab in refresh_schema.tabs:
        if not tab.table_id:
            continue
        sid = sheet_ids.get(tab.name)
        # When the schema column count grows (e.g. new ``original_value``
        # column on UserEdits), updateTable with columnProperties alone
        # fails: ``Too many column properties specified. There can only be
        # as many column properties as there are columns in the table.``
        # So we also widen the table's ``range`` to cover the new column
        # count. We only set ``startColumnIndex`` (=0) + ``endColumnIndex``
        # (=len(columns)); the row range is preserved by omitting row
        # indices, which the API treats as "keep current vertical extent".
        table_body: dict[str, object] = {
            "tableId": tab.table_id,
            "columnProperties": [_table_column_properties(i, c) for i, c in enumerate(tab.columns)],
        }
        fields = "columnProperties"
        if sid is not None:
            table_body["range"] = {
                "sheetId": sid,
                "startColumnIndex": 0,
                "endColumnIndex": len(tab.columns),
            }
            fields = "columnProperties,range"
        requests.append(
            {
                "updateTable": {
                    "table": table_body,
                    "fields": fields,
                }
            }
        )
    requests.extend(_apply_dropdowns(refresh_schema, sheet_ids))
    return requests


# --------------------------------------------------------------------------- #
# 1. Rename + 2. Add tabs
# --------------------------------------------------------------------------- #


def _rename_default_tab(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    first = schema.tabs[0]
    return [
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_ids[first.name], "title": first.name},
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
# 4. Dashboard tab layout (greeting, KPIs, lists)
# --------------------------------------------------------------------------- #
#
# v4.3 bordered-canvas layout (1-based row numbers; cols A..F).
#
# Column A and F are 8px border columns; cols B..E host the content.
# Row 1 and the final row are 8px border rows. The greeting/headers/
# footer are merged across B..E (not the full A..F width) so the border
# columns frame everything.
#
# Five deliberate row heights:
#
#   * Top border (row 1)         : 16 px
#   * Greeting (row 2)           : 56 px
#   * Graphics band (row 3)      : 180 px
#   * Content rows (rows 4..30)  : 24 px (uniform)
#   * Bottom border (row 31)     : 16 px
#
# Layout:
#
#   Row  1   8px border, empty (frames the top edge)
#   Row  2   B:E greeting banner (merged, 56px tall)
#   Row  3   B:E graphics band (180px tall). Five floating charts (4 KPI
#            scorecards in a 2x2 + 1 donut) anchored at B3 with pixel
#            offsets only — never via cell ranges.
#   Row  4   B:E "Overdue" section header (merged, ACCENT2 red)
#   Row  5   B..E column labels (Subject | Title | Due | Status)
#   Rows 6..10   B..E 5-row Overdue list slab
#   Row 11   B:E "Due this week" header (ACCENT3 amber)
#   Row 12   B..E column labels
#   Rows 13..17  B..E 5-row Due-this-week slab
#   Row 18   B:E "Upcoming" header (ACCENT1 blue)
#   Row 19   B..E column labels
#   Rows 20..29  B..E 10-row Upcoming slab
#   Row 30   B:E "Last sync: ..." footer (italic, muted, right-aligned)
#   Row 31   8px border, empty (frames the bottom edge)

# Section row offsets (0-based, used in updateCells start.rowIndex).
# Since v5.0 the task list region (rows 3..) is emitted at publish time
# by :mod:`homework_hub.dashboard_layout` (which owns Tables, banding,
# CF rules, and the last-sync footer). The template owns only the
# 4-row frame: top border, greeting, graphics band, bottom border.
_DASH_ROW_TOP_BORDER = 0
_DASH_ROW_GREETING = 1
_DASH_ROW_GRAPHICS = 2  # 188 px tall row hosting floating chart overlay
_DASH_ROW_BOTTOM_BORDER = 3
# Dashboard frame rows: 8px top border, 56px greeting, 188px graphics band,
# 8px bottom border. The lists region grows below and is owned by
# ``dashboard_layout``. We size the grid generously up-front (matching
# the Tasks tab's default of 1000 rows) rather than re-grow it on every
# publish — Sheets ``addTable`` validates its range against the
# pre-batch grid bounds and rejects with an opaque 500 if the grid was
# resized in the same batchUpdate call.
_DASH_FRAME_ROW_COUNT = 4
_DASH_GRID_ROW_COUNT = 1000

# Column index constants. Border columns are A (0) and F (5); content
# lives in B..E (indices 1..4 inclusive; end exclusive = 5).
_DASH_COL_BORDER_LEFT = 0
_DASH_COL_CONTENT_START = 1  # column B
_DASH_COL_CONTENT_END = 5  # one past column E (exclusive)
_DASH_COL_BORDER_RIGHT = 5
_DASH_COL_COUNT = 6

# Graphics band geometry — all charts anchored at B3
# (rowIndex=_DASH_ROW_GRAPHICS, columnIndex=_DASH_COL_CONTENT_START).
# Internal 8px padding on the top + bottom of the graphics band frames
# the charts away from the greeting banner above and Overdue header below.
#
# Inner canvas = columns B..E = 256 + 512 + 128 + 128 = 1024 px wide.
# Graphics band is split into three equal "thirds" with an 8px gap
# between each third:
#   third_w + gap + third_w + gap + third_w = 1024
#   = 336 + 8 + 336 + 8 + 336 = 1024  ✓
# Left third hosts the 2x2 KPI tile grid (4 tiles, 164x82 each, 8px
# gutters). Middle third hosts the "Status" donut (counts by task
# status). Right third hosts the "Subjects" donut (counts by subject).
# Tile maths within the left third:
#   2*tile_w + gap_x = 336 → tile_w = (336 - 8)/2 = 164
# Vertical: pad_y + 2*tile_h + gap_y + pad_y = 8 + 82 + 8 + 82 + 8 = 188  ✓
_DASH_GRAPHICS_HEIGHT_PX = 188
_DASH_GRAPHICS_PAD_Y = 8
_DASH_TILE_W = 164
_DASH_TILE_H = 82
_DASH_TILE_GAP_X = 8
_DASH_TILE_GAP_Y = 8
_DASH_SECTION_GAP = 8  # gap between the three graphics-bar thirds
_DASH_DONUT_W = 336
_DASH_DONUT_H = _DASH_GRAPHICS_HEIGHT_PX - 2 * _DASH_GRAPHICS_PAD_Y  # 172
_DASH_STATUS_DONUT_X = 344  # left third (336) + gap (8)
_DASH_SUBJECTS_DONUT_X = 688  # left third + gap + status donut + gap

# Settings tab lookup formulas. ``Settings!A:B`` keys "Child" and "Last
# full sync" are written by ``pipeline.publish.project_settings_rows``.
# Child name is wrapped in PROPER() to capitalise lower-case YAML keys.
_GREETING_FORMULA = (
    '=IFERROR("Hi " & PROPER(VLOOKUP("Child", Settings!A:B, 2, FALSE)) '
    "& \", here's what's on:\", \"Hi, here's what's on:\")"
)
# Last-sync footer formula moved to homework_hub.dashboard_layout in v5.0
# (the footer's row is dynamic and is owned by the publish-time layout).

# 4 KPI tiles in a 2x2 grid. Pending work takes priority over the
# completion metric: Done this week remains visible as a Dashboard table,
# while the fourth scorecard surfaces tasks that would otherwise disappear
# from every date-based section.
_KPI_LABELS = ("Overdue", "Due this week", "Upcoming", "No due date")
_KPI_FORMULAS = (
    # Overdue = Status column says so (matches dashboard section).
    '=COUNTIF(Tasks!F2:F,"Overdue")',
    # Due this week = 0..7 days AND status is a pending state.
    # Explicit status guards (rather than relying on the Days-column
    # blank trick) so the formula is self-documenting and matches the
    # ``filter_week`` predicate in dashboard_layout exactly.
    (
        '=COUNTIFS(Tasks!E2:E,">=0",Tasks!E2:E,"<=7",'
        'Tasks!F2:F,"<>Overdue",'
        'Tasks!F2:F,"<>Submitted",'
        'Tasks!F2:F,"<>Graded",'
        'Tasks!F2:F,"<>Archived")'
    ),
    # Upcoming = > 7 days AND status is a pending state.
    (
        '=COUNTIFS(Tasks!E2:E,">7",'
        'Tasks!F2:F,"<>Overdue",'
        'Tasks!F2:F,"<>Submitted",'
        'Tasks!F2:F,"<>Graded",'
        'Tasks!F2:F,"<>Archived")'
    ),
    # No due date = a real task row with blank Due and a pending status.
    # Subject must be non-blank so COUNTIFS does not count the unused tail
    # of the open-ended Tasks ranges.
    (
        '=COUNTIFS(Tasks!A2:A,"<>",Tasks!D2:D,"",'
        'Tasks!F2:F,"<>Overdue",'
        'Tasks!F2:F,"<>Submitted",'
        'Tasks!F2:F,"<>Graded",'
        'Tasks!F2:F,"<>Archived")'
    ),
)
# Accent assigned to each tile's value foreground, matching its section.
# Urgency mapping: Overdue=red, Due this week=amber, Upcoming=blue,
# No due date=green.
_KPI_ACCENTS = ("ACCENT2", "ACCENT3", "ACCENT1", "ACCENT4")


def _write_dashboard_frame(schema: SheetSchema, sheet_ids: dict[str, int]) -> list[dict[str, Any]]:
    """Emit the Dashboard frame — greeting cell only.

    Since v5.0 the task lists, banding, CF rules, section headers and
    last-sync footer are emitted at publish time by
    :mod:`homework_hub.dashboard_layout`. This function owns only the
    static greeting cell at B2; merge + format come from
    ``_apply_dashboard_formats``; the floating KPI scorecards + donuts
    come from ``_apply_dashboard_kpi_scorecards`` /
    ``_apply_dashboard_status_chart`` /
    ``_apply_dashboard_subjects_chart``.

    Skips silently if the schema's first tab isn't a dashboard-shaped
    (6-column, no headers, no table_id) tab — keeps the function safe
    when callers pass a synthetic test schema.
    """
    dash = schema.tabs[0]
    if dash.table_id or len(dash.columns) != _DASH_COL_COUNT or any(c.header for c in dash.columns):
        return []
    sid = sheet_ids[dash.name]
    return [
        _update_cells(
            sid,
            row_index=_DASH_ROW_GREETING,
            column_index=_DASH_COL_CONTENT_START,
            rows=[[_formula_cell(_GREETING_FORMULA)]],
            fields="userEnteredValue",
        )
    ]


def _apply_dashboard_formats(
    schema: SheetSchema, sheet_ids: dict[str, int]
) -> list[dict[str, Any]]:
    """Theme-driven formatting for the Dashboard tab (v4.3 bordered canvas).

    Uses ``ColorStyle.themeColorType`` (ACCENT1..6, TEXT, BACKGROUND, LINK)
    so the spreadsheet's theme drives every colour — the kid can repaint
    by switching theme in Sheets.

    Row heights are exactly five deliberate sizes (top-border=16,
    greeting=56, graphics=180, content=24, bottom-border=16). KPI tiles +
    donuts are floating ``scorecardChart``/``pieChart`` objects handled by
    ``_apply_dashboard_kpi_scorecards``, ``_apply_dashboard_status_chart``,
    and ``_apply_dashboard_subjects_chart``.
    """
    dash = schema.tabs[0]
    if dash.table_id or len(dash.columns) != _DASH_COL_COUNT or any(c.header for c in dash.columns):
        return []
    sid = sheet_ids[dash.name]
    out: list[dict[str, Any]] = []

    def _theme_color(name: str) -> dict[str, Any]:
        return {"themeColor": name}

    def _range(
        row_start: int,
        row_end: int,
        col_start: int = _DASH_COL_CONTENT_START,
        col_end: int | None = None,
    ) -> dict[str, Any]:
        return {
            "sheetId": sid,
            "startRowIndex": row_start,
            "endRowIndex": row_end,
            "startColumnIndex": col_start,
            "endColumnIndex": col_end if col_end is not None else _DASH_COL_CONTENT_END,
        }

    def _repeat(rng: dict[str, Any], fmt: dict[str, Any], fields: str) -> dict[str, Any]:
        return {"repeatCell": {"range": rng, "cell": {"userEnteredFormat": fmt}, "fields": fields}}

    # Hide gridlines on the Dashboard tab — biggest single visual win.
    out.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sid,
                    "gridProperties": {"hideGridlines": True},
                },
                "fields": "gridProperties.hideGridlines",
            }
        }
    )

    # Merge greeting B2:E2 (inside the border columns). The graphics band
    # sits on row 3 below, so the greeting owns row 2 cleanly within frame.
    out.append(
        {
            "mergeCells": {
                "range": _range(_DASH_ROW_GREETING, _DASH_ROW_GREETING + 1),
                "mergeType": "MERGE_ALL",
            }
        }
    )

    # Greeting banner: ACCENT1 background, BACKGROUND-colour text, bold.
    out.append(
        _repeat(
            _range(_DASH_ROW_GREETING, _DASH_ROW_GREETING + 1),
            {
                "backgroundColorStyle": _theme_color("ACCENT1"),
                "textFormat": {
                    "bold": True,
                    "fontSize": 18,
                    "foregroundColorStyle": _theme_color("BACKGROUND"),
                },
                "horizontalAlignment": "LEFT",
                "verticalAlignment": "MIDDLE",
                "padding": {"top": 6, "right": 12, "bottom": 6, "left": 12},
            },
            (
                "userEnteredFormat.backgroundColorStyle,"
                "userEnteredFormat.textFormat.bold,"
                "userEnteredFormat.textFormat.fontSize,"
                "userEnteredFormat.textFormat.foregroundColorStyle,"
                "userEnteredFormat.horizontalAlignment,"
                "userEnteredFormat.verticalAlignment,"
                "userEnteredFormat.padding"
            ),
        )
    )

    # Row heights — frame only. The publish-time dashboard_layout
    # sets dimensions on the lists region (rows 3..N-1) and the footer.
    #
    #   row 0           : 8 px   (top border)
    #   row 1           : 56 px  (greeting)
    #   row 2           : 188 px (graphics band hosting floating charts)
    #   row 3           : 8 px   (bottom border before first publish;
    #                             pushed down by publish-time inserts)
    row_dimension_specs = [
        (_DASH_ROW_TOP_BORDER, _DASH_ROW_TOP_BORDER + 1, 8),
        (_DASH_ROW_GREETING, _DASH_ROW_GREETING + 1, 56),
        (_DASH_ROW_GRAPHICS, _DASH_ROW_GRAPHICS + 1, _DASH_GRAPHICS_HEIGHT_PX),
        (_DASH_ROW_BOTTOM_BORDER, _DASH_ROW_BOTTOM_BORDER + 1, 8),
    ]
    for start, end, px in row_dimension_specs:
        out.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sid,
                        "dimension": "ROWS",
                        "startIndex": start,
                        "endIndex": end,
                    },
                    "properties": {"pixelSize": px},
                    "fields": "pixelSize",
                }
            }
        )

    return out


# --------------------------------------------------------------------------- #
# DashboardData helper tab + Dashboard floating charts
# --------------------------------------------------------------------------- #
#
# The Dashboard's 4 KPI scorecards and 2 donut charts all source their
# values from the hidden ``DashboardData`` tab. Layout:
#
#   A1:B1   (empty — reserved for future header / left blank for now)
#   A2      "Overdue"            B2  =COUNTIF(...)
#   A3      "Due this week"      B3  =COUNTIFS(...)
#   A4      "Upcoming"           B4  =COUNTIFS(...)
#   A5      "No due date"        B5  =COUNTIFS(...)
#   A6      (separator)
#   A7      =QUERY(Tasks!A2:A, "select A, count(A) ... group by A", 0)
#           ↳ spills down into A7:B(7+N-1) with one row per distinct
#             subject (col A = subject name, col B = task count).
#
# The whole tab is hidden so users never see this helper plumbing. Each
# scorecard chart points at a single value cell (B2..B5); the Status
# donut points at the label+value range (A2:B5); the Subjects donut
# points at the QUERY-spilled range (A7:B25 — generous bound, pie
# auto-omits empty rows; 15 subjects + headroom).
_DASH_DATA_TAB_NAME = "DashboardData"
_DASH_DATA_FIRST_ROW = 1  # 0-based; row 2 in 1-based notation.

# Subjects-donut data block — single QUERY cell spills into a 2-col block.
#
# Filter mirrors the 4 dashboard tiles exactly as a union of the four
# tile predicates:
#   - Overdue:        F = 'Overdue'
#   - Due this week:  D between today and today+7 AND F NOT IN {excluded}
#   - Upcoming:       D > today+7              AND F NOT IN {excluded}
#   - No due date:    D is blank                 AND F NOT IN {excluded}
# where {excluded} = {'Overdue','Submitted','Graded','Archived'}.
#
# This guarantees the sum of slice values == sum of tile values, since
# the four predicates are mutually exclusive (Overdue is its own status;
# pending statuses with future-dated D split into Due-week vs Upcoming;
# undated pending work is isolated in its own branch).
#
# QUERY needs date literals as `date 'yyyy-mm-dd'`; we build them via
# string concatenation with TEXT(...) on TODAY() and TODAY()+/-7.
_DASH_SUBJECTS_FIRST_ROW = 6  # 0-based; row 7 in 1-based notation.
_DASH_SUBJECTS_ROW_BOUND = 25  # 0-based exclusive; rows 7..24 = 18 slots.
_DASH_SUBJECTS_FORMULA = (
    "=QUERY(Tasks!A2:F, "
    '"select A, count(A) where A is not null and ('
    # Overdue tile
    "F = 'Overdue' "
    # Due this week tile
    'or (D >= date \'"&TEXT(TODAY(),"yyyy-mm-dd")&"\' '
    'and D <= date \'"&TEXT(TODAY()+7,"yyyy-mm-dd")&"\' '
    "and F != 'Overdue' and F != 'Submitted' "
    "and F != 'Graded' and F != 'Archived') "
    # Upcoming tile
    'or (D > date \'"&TEXT(TODAY()+7,"yyyy-mm-dd")&"\' '
    "and F != 'Overdue' and F != 'Submitted' "
    "and F != 'Graded' and F != 'Archived') "
    # No due date tile
    "or (D is null and F != 'Overdue' and F != 'Submitted' "
    "and F != 'Graded' and F != 'Archived')"
    ") group by A label count(A) ''\", 0)"
)


def _write_dashboard_data_rows(
    schema: SheetSchema, sheet_ids: dict[str, int]
) -> list[dict[str, Any]]:
    """Seed the hidden DashboardData tab with KPI label/value pairs plus
    the Subjects donut data source.

    Two writes:

    1. Rows 2..5: 4 KPI label/formula pairs (powering the scorecards and
       the Status donut).
    2. Row 7, col A: a single QUERY formula that spills into A7:B(7+N-1)
       — subject name + count per distinct subject in Tasks!A.

    Skips silently if the schema doesn't include a DashboardData tab
    (synthetic test schemas may omit it).
    """
    if not any(t.name == _DASH_DATA_TAB_NAME for t in schema.tabs):
        return []
    sid = sheet_ids[_DASH_DATA_TAB_NAME]
    kpi_rows: list[list[dict[str, Any]]] = []
    for label, formula in zip(_KPI_LABELS, _KPI_FORMULAS, strict=True):
        kpi_rows.append([_string_cell(label), _formula_cell(formula)])
    return [
        _update_cells(
            sid,
            row_index=_DASH_DATA_FIRST_ROW,
            column_index=0,
            rows=kpi_rows,
            fields="userEnteredValue",
        ),
        # Subjects donut data source — single QUERY cell at A7 that
        # spills down + right into A7:B(7+N-1).
        _update_cells(
            sid,
            row_index=_DASH_SUBJECTS_FIRST_ROW,
            column_index=0,
            rows=[[_formula_cell(_DASH_SUBJECTS_FORMULA)]],
            fields="userEnteredValue",
        ),
    ]


def _apply_dashboard_kpi_scorecards(
    schema: SheetSchema, sheet_ids: dict[str, int]
) -> list[dict[str, Any]]:
    """4 floating ``scorecardChart`` tiles in a 2x2 grid, occupying the
    left third (336 px) of the 1024 px graphics band. All anchored at
    cell B3 (the first inner content cell of the graphics row) with
    pure pixel offsets.

    Layout (offsets from B3 top-left, flush to the inner canvas):

        +-- left third (336) --+ 8 +-- Status donut (336) --+ 8 +-- Subjects donut (336) --+
        | tile1 (164x82) 8 tile2 |                         |                              |
        | Overdue        Due wk  |    [ Status donut ]     |    [ Subjects donut ]        |
        |                        |                         |                              |
        |       8 px row gap     |                         |                              |
        |                        |                         |                              |
        | tile3 (164x82) 8 tile4 |                         |                              |
        | Upcoming       No date |                         |                              |
        +--------------------------------------------------------------------------------+

    Each tile pulls its value from a single cell on DashboardData:

        Overdue        → DashboardData!B2  ACCENT2 red
        Due this week  → DashboardData!B3  ACCENT3 amber
        Upcoming       → DashboardData!B4  ACCENT1 blue
        No due date    → DashboardData!B5  ACCENT4 green

    Tile size: 164 x 82 px. Grid pitch: 172 x 90 (tile + 8 gutter).
    Two tile columns (164+8+164 = 336 px) exactly fill the left third.
    Two tile rows (82+8+82 = 172 px) exactly fill the inner vertical
    band (188 px graphics row minus 2*8 padY).
    """
    dash = schema.tabs[0]
    if dash.table_id or len(dash.columns) != _DASH_COL_COUNT or any(c.header for c in dash.columns):
        return []
    if not any(t.name == _DASH_DATA_TAB_NAME for t in schema.tabs):
        return []
    dash_sid = sheet_ids[dash.name]
    data_sid = sheet_ids[_DASH_DATA_TAB_NAME]

    tile_w, tile_h = _DASH_TILE_W, _DASH_TILE_H
    pitch_x = tile_w + _DASH_TILE_GAP_X
    pitch_y = tile_h + _DASH_TILE_GAP_Y

    # (label, accent, row_index_in_data_tab, grid_col, grid_row)
    tiles = (
        (_KPI_LABELS[0], _KPI_ACCENTS[0], _DASH_DATA_FIRST_ROW + 0, 0, 0),
        (_KPI_LABELS[1], _KPI_ACCENTS[1], _DASH_DATA_FIRST_ROW + 1, 1, 0),
        (_KPI_LABELS[2], _KPI_ACCENTS[2], _DASH_DATA_FIRST_ROW + 2, 0, 1),
        (_KPI_LABELS[3], _KPI_ACCENTS[3], _DASH_DATA_FIRST_ROW + 3, 1, 1),
    )

    out: list[dict[str, Any]] = []
    for label, accent, data_row, gx, gy in tiles:
        out.append(
            {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": label,
                            "titleTextFormat": {"bold": True, "fontSize": 11},
                            "backgroundColorStyle": {"themeColor": "BACKGROUND"},
                            "scorecardChart": {
                                "keyValueData": {
                                    "sourceRange": {
                                        "sources": [
                                            {
                                                "sheetId": data_sid,
                                                "startRowIndex": data_row,
                                                "endRowIndex": data_row + 1,
                                                "startColumnIndex": 1,
                                                "endColumnIndex": 2,
                                            }
                                        ]
                                    }
                                },
                                "keyValueFormat": {
                                    "textFormat": {
                                        "bold": True,
                                        "fontSize": 32,
                                        "foregroundColorStyle": {"themeColor": accent},
                                    }
                                },
                            },
                        },
                        "position": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": dash_sid,
                                    "rowIndex": _DASH_ROW_GRAPHICS,
                                    "columnIndex": _DASH_COL_CONTENT_START,
                                },
                                "offsetXPixels": gx * pitch_x,
                                "offsetYPixels": _DASH_GRAPHICS_PAD_Y + gy * pitch_y,
                                "widthPixels": tile_w,
                                "heightPixels": tile_h,
                            }
                        },
                    }
                }
            }
        )
    return out


def _apply_dashboard_status_chart(
    schema: SheetSchema, sheet_ids: dict[str, int]
) -> list[dict[str, Any]]:
    """ "Status" donut (pie with pieHole=0.6) anchored at B3, middle third
    of the graphics band (offsetX=344, width=336), flush to the inner
    canvas top/bottom.

    Data source is the hidden DashboardData tab's KPI block (A2:B5 —
    label, value pairs by task status). All positioning is in pixels
    from B3 — does not depend on cell ranges.
    """
    dash = schema.tabs[0]
    if dash.table_id or len(dash.columns) != _DASH_COL_COUNT or any(c.header for c in dash.columns):
        return []
    if not any(t.name == _DASH_DATA_TAB_NAME for t in schema.tabs):
        return []
    dash_sid = sheet_ids[dash.name]
    data_sid = sheet_ids[_DASH_DATA_TAB_NAME]
    nrows = len(_KPI_LABELS)

    return [
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Status",
                        "titleTextFormat": {"bold": True, "fontSize": 11},
                        "backgroundColorStyle": {"themeColor": "BACKGROUND"},
                        "pieChart": {
                            "legendPosition": "RIGHT_LEGEND",
                            "pieHole": 0.6,
                            "domain": {
                                "sourceRange": {
                                    "sources": [
                                        {
                                            "sheetId": data_sid,
                                            "startRowIndex": _DASH_DATA_FIRST_ROW,
                                            "endRowIndex": _DASH_DATA_FIRST_ROW + nrows,
                                            "startColumnIndex": 0,
                                            "endColumnIndex": 1,
                                        }
                                    ]
                                }
                            },
                            "series": {
                                "sourceRange": {
                                    "sources": [
                                        {
                                            "sheetId": data_sid,
                                            "startRowIndex": _DASH_DATA_FIRST_ROW,
                                            "endRowIndex": _DASH_DATA_FIRST_ROW + nrows,
                                            "startColumnIndex": 1,
                                            "endColumnIndex": 2,
                                        }
                                    ]
                                }
                            },
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": dash_sid,
                                "rowIndex": _DASH_ROW_GRAPHICS,
                                "columnIndex": _DASH_COL_CONTENT_START,
                            },
                            "offsetXPixels": _DASH_STATUS_DONUT_X,
                            "offsetYPixels": _DASH_GRAPHICS_PAD_Y,
                            "widthPixels": _DASH_DONUT_W,
                            "heightPixels": _DASH_DONUT_H,
                        }
                    },
                }
            }
        }
    ]


def _apply_dashboard_subjects_chart(
    schema: SheetSchema, sheet_ids: dict[str, int]
) -> list[dict[str, Any]]:
    """ "Subjects" donut (pie with pieHole=0.6) anchored at B3, right
    third of the graphics band (offsetX=688, width=336), flush to the
    inner canvas top/bottom.

    Data source is the QUERY-spilled block at DashboardData!A7:B25 —
    one row per distinct subject in Tasks!A, with count of tasks per
    subject. Generous row bound (18 slots; user expects ≤15 subjects).
    Pie auto-omits empty/zero rows.
    """
    dash = schema.tabs[0]
    if dash.table_id or len(dash.columns) != _DASH_COL_COUNT or any(c.header for c in dash.columns):
        return []
    if not any(t.name == _DASH_DATA_TAB_NAME for t in schema.tabs):
        return []
    dash_sid = sheet_ids[dash.name]
    data_sid = sheet_ids[_DASH_DATA_TAB_NAME]

    return [
        {
            "addChart": {
                "chart": {
                    "spec": {
                        "title": "Subjects",
                        "titleTextFormat": {"bold": True, "fontSize": 11},
                        "backgroundColorStyle": {"themeColor": "BACKGROUND"},
                        "pieChart": {
                            "legendPosition": "RIGHT_LEGEND",
                            "pieHole": 0.6,
                            "domain": {
                                "sourceRange": {
                                    "sources": [
                                        {
                                            "sheetId": data_sid,
                                            "startRowIndex": _DASH_SUBJECTS_FIRST_ROW,
                                            "endRowIndex": _DASH_SUBJECTS_ROW_BOUND,
                                            "startColumnIndex": 0,
                                            "endColumnIndex": 1,
                                        }
                                    ]
                                }
                            },
                            "series": {
                                "sourceRange": {
                                    "sources": [
                                        {
                                            "sheetId": data_sid,
                                            "startRowIndex": _DASH_SUBJECTS_FIRST_ROW,
                                            "endRowIndex": _DASH_SUBJECTS_ROW_BOUND,
                                            "startColumnIndex": 1,
                                            "endColumnIndex": 2,
                                        }
                                    ]
                                }
                            },
                        },
                    },
                    "position": {
                        "overlayPosition": {
                            "anchorCell": {
                                "sheetId": dash_sid,
                                "rowIndex": _DASH_ROW_GRAPHICS,
                                "columnIndex": _DASH_COL_CONTENT_START,
                            },
                            "offsetXPixels": _DASH_SUBJECTS_DONUT_X,
                            "offsetYPixels": _DASH_GRAPHICS_PAD_Y,
                            "widthPixels": _DASH_DONUT_W,
                            "heightPixels": _DASH_DONUT_H,
                        }
                    },
                }
            }
        }
    ]


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
                # Seed row is row 2 (1-based); substitute {row} in template.
                cells.append(_formula_cell(col.formula_template.format(row=2)))
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
        # NOTE: Inside ``addTable.table.columnProperties[].dataValidationRule``
        # the API only accepts ``condition`` — ``strict`` and ``showCustomUi``
        # are rejected here even though they're valid on a standalone
        # ``setDataValidation`` rule. We apply those flags via the separate
        # ``_apply_dropdowns`` pass below.
        props["dataValidationRule"] = {
            "condition": {
                "type": "ONE_OF_LIST",
                "values": [{"userEnteredValue": v} for v in col.dropdown_values],
            },
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
                                "numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}
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
    """ONE_OF_LIST DataValidation per DROPDOWN column on row 2 onwards.

    Skips tabs backed by a native Table — those columns already enforce
    dropdown semantics via ``columnType=DROPDOWN`` set in ``addTable``,
    and the API rejects ``setDataValidation`` on cells inside typed
    table columns.
    """
    out: list[dict[str, Any]] = []
    for tab in schema.tabs:
        if tab.table_id:
            continue
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


__all__ = ["DASHBOARD_TAB_NAME", "bootstrap_requests", "refresh_layout_requests"]

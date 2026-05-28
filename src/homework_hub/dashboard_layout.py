"""Dynamic Dashboard layout builder (v5.0).

The Dashboard's three task lists (Overdue / Due this week / Upcoming) are
emitted as **real Sheets Tables sized to the actual data** at publish
time. The template-time bootstrap only owns the static frame (border
rows/cols, greeting, graphics band with floating KPI scorecards + donut,
last-sync footer); this module owns everything between the graphics band
and the footer.

Why publish-time rather than template-time
------------------------------------------

Sheets Tables wrap a static cell range — there's no formula spill that
can grow a Table. To get true auto-sized sections (the only honest way
to render a long Upcoming list without scrolling past 5 blank rows),
the row counts must be computed when the data is known.

The trade-off: Dashboard list contents now only refresh on publish (cron
or on-demand `sync` / `publish` CLI), not when the kid edits a row's
Status on the Tasks tab. The KPI scorecards + donut continue to update
live because they read live ``COUNTIFS`` formulas on the hidden
``DashboardData`` tab.

Architecture
------------

* :func:`build_requests` is **pure**: takes a dashboard sheetId, the
  list of active :class:`~homework_hub.pipeline.publish.TaskRow` rows,
  the list of existing Dashboard table ids (so we can tear them down),
  and emits the full batchUpdate body for the lists region.
* Section identity is conveyed by the Table's ``name`` attribute
  (``Overdue`` / ``Due this week`` / ``Upcoming``). No separate header
  rows or coloured banners — the Table label IS the heading.
* Empty sections render a 1-row Table with a kid-friendly fallback
  message in the Title column.
* Status column is a dropdown matching the Tasks tab so the visual
  treatment is consistent — values come from
  :data:`homework_hub.schema.STATUS_VALUES`.

Layout
------

::

    Row 0     8px top border (frame, template-owned)
    Row 1     greeting          (frame, template-owned)
    Row 2     graphics band     (frame, template-owned, 188px)
    Row 3     Overdue Table header  ← dynamic starts here
    Rows 4..3+N_overdue    Overdue data rows (≥1)
    Row 4+N_overdue        spacer (8px)
    Row 5+N_overdue        Week Table header
    ...
    Row M     last-sync footer  (frame, template-owned)
    Row M+1   8px bottom border (frame, template-owned)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

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
    TASKS_TAB,
)

# Statuses that count as "done" for the purposes of the Done(7d) section
# and that should be excluded from the Week / Upcoming sections so the
# section row counts match the KPI tile counts above them.
_DONE_STATUSES: frozenset[str] = frozenset({"Submitted", "Graded"})
_EXCLUDE_FROM_PENDING: frozenset[str] = frozenset({"Submitted", "Graded", "Archived"})

# Done(7d) window — must mirror the tile formula in sheet_template.
_DONE_WINDOW_DAYS = 7

# Header colour for every Dashboard Table — a sage green that matches the
# user's chosen palette. Applied via ``Table.rowsProperties.headerColorStyle``
# in the same ``updateTable`` request that sets ``columnProperties``.
_TABLE_HEADER_COLOR: dict[str, float] = {
    "red": 111 / 255,
    "green": 164 / 255,
    "blue": 140 / 255,
}

# --------------------------------------------------------------------------- #
# Layout constants (shared with sheet_template's frame)
# --------------------------------------------------------------------------- #

# Frame rows the template owns; the lists region starts on the row
# immediately after the graphics band.
_FRAME_ROW_TOP_BORDER = 0
_FRAME_ROW_GREETING = 1
_FRAME_ROW_GRAPHICS = 2
_LISTS_START_ROW = 3  # 0-based first row of the lists region (leading spacer)
# Generous upper bound for the lists region — must match the grid size
# pre-set by ``sheet_template._DASH_GRID_ROW_COUNT`` so the teardown
# ``unmergeCells`` request covers any leftover footer merge a previous
# publish may have left behind further down the sheet.
_LISTS_END_ROW_BOUND = 1000

# Content sits in cols B..E (indices 1..4 inclusive; end exclusive = 5).
# Border cols A (0) and F (5) frame the canvas.
_COL_BORDER_LEFT = 0
_COL_CONTENT_START = 1  # Subject column inside each Table
_COL_CONTENT_END = 5  # one past Status column (exclusive)
_COL_BORDER_RIGHT = 5
_COL_COUNT = 6

# Inside each Table the columns are: Subject (0) | Title (1) | Due (2) | Status (3).
# These are 0-based indices relative to the Table's startColumnIndex
# (i.e. relative to _COL_CONTENT_START in the sheet).
_TBL_COL_SUBJECT = 0
_TBL_COL_TITLE = 1
_TBL_COL_DUE = 2
_TBL_COL_STATUS = 3
_TBL_NUM_COLUMNS = 4

# Row heights. The data rows + Table-header rows + spacers all share the
# same content-row height so the lists region renders as a clean grid.
_ROW_HEIGHT_BORDER = 8
_ROW_HEIGHT_CONTENT = 24
_ROW_HEIGHT_GREETING = 56
_ROW_HEIGHT_GRAPHICS = 188
_ROW_HEIGHT_FOOTER = 24
# Spacer row between consecutive Tables. The Sheets-rendered
# table-name chip floats above each header row and visually consumes
# ~40px of the spacer. To leave ~8px of clear padding above the
# chip, the spacer row is sized at 48px.
_ROW_HEIGHT_SPACER = 48

# Spacer between consecutive Tables: a single 8px row keeps the sections
# visually distinct without wasting a full content-row of whitespace.
_SPACER_ROWS = 1

# Column letter map for the Tasks tab (1-based) — used when building
# ``HYPERLINK`` formulas that reference the underlying data row.
# Unused right now (we write literal values), but kept here for
# completeness in case we later move to formula-backed cells.

# Fallback messages — shown in the Title column of a 1-row empty Table.
_FALLBACKS: dict[str, str] = {
    DASHBOARD_OVERDUE_TABLE_NAME: "All caught up — nothing overdue!",
    DASHBOARD_WEEK_TABLE_NAME: "Nothing due this week.",
    DASHBOARD_UPCOMING_TABLE_NAME: "No upcoming work — nice.",
    DASHBOARD_DONE_TABLE_NAME: "No work completed in the last 7 days yet.",
}

# Sheets date-serial epoch — copy of the constant in publish.py to avoid
# importing publish.py here (would create an import cycle).
_SHEETS_EPOCH = date(1899, 12, 30)


# --------------------------------------------------------------------------- #
# Inputs
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DashboardTask:
    """A single task as it should appear on the Dashboard.

    Distinct from :class:`~homework_hub.pipeline.publish.TaskRow` (which
    is keyed to the 10-column Tasks tab layout) so this module can be
    tested without dragging in the publish projection logic.
    """

    subject: str
    title: str
    due: date | None
    status: str
    link: str


@dataclass(frozen=True)
class SectionLayout:
    """Computed row offsets for one section after sizing."""

    name: str
    table_id: str
    header_row: int  # 0-based row index of the Table's header row
    data_start_row: int  # always header_row + 1
    data_end_row: int  # exclusive; equals data_start_row + n_data_rows
    fallback: bool  # True when the section is empty and we wrote a fallback row


# --------------------------------------------------------------------------- #
# Filtering / sorting
# --------------------------------------------------------------------------- #


def _days_until(due: date | None, today: date) -> int | None:
    if due is None:
        return None
    return (due - today).days


def filter_overdue(tasks: list[DashboardTask]) -> list[DashboardTask]:
    """Overdue = ``status == "Overdue"``.

    Sorted by due ascending; tasks with no due date last.
    """
    return _sort_by_due([t for t in tasks if t.status == "Overdue"])


def filter_week(tasks: list[DashboardTask], today: date) -> list[DashboardTask]:
    """Due this week = ``0 <= days <= 7`` AND status is a pending state.

    Excludes Overdue (which has its own section) and Submitted / Graded /
    Archived (which represent finished or shelved work). This matches the
    KPI tile formula's semantics so the tile count equals the table row
    count.
    """
    out: list[DashboardTask] = []
    for t in tasks:
        if t.status == "Overdue" or t.status in _EXCLUDE_FROM_PENDING:
            continue
        days = _days_until(t.due, today)
        if days is None:
            continue
        if 0 <= days <= 7:
            out.append(t)
    return _sort_by_due(out)


def filter_upcoming(tasks: list[DashboardTask], today: date) -> list[DashboardTask]:
    """Upcoming = ``days > 7`` AND status is a pending state.

    Same exclusion set as :func:`filter_week` so the KPI tile count and
    the table row count agree.
    """
    out: list[DashboardTask] = []
    for t in tasks:
        if t.status == "Overdue" or t.status in _EXCLUDE_FROM_PENDING:
            continue
        days = _days_until(t.due, today)
        if days is None:
            continue
        if days > 7:
            out.append(t)
    return _sort_by_due(out)


def filter_done(tasks: list[DashboardTask], today: date) -> list[DashboardTask]:
    """Done in the last 7 days = ``status IN {Submitted, Graded}`` AND
    ``today - 7 <= due <= today``.

    Sorted by due **descending** so the most recently completed work
    appears first.
    """
    cutoff = today - timedelta(days=_DONE_WINDOW_DAYS)
    out = [
        t
        for t in tasks
        if t.status in _DONE_STATUSES and t.due is not None and cutoff <= t.due <= today
    ]
    return _sort_by_due(out, descending=True)


def _sort_by_due(tasks: list[DashboardTask], *, descending: bool = False) -> list[DashboardTask]:
    # ``date.max`` sentinels tasks with no due date to the tail in the
    # ascending case; ``date.min`` does the same in the descending case.
    sentinel = date.min if descending else date.max
    return sorted(
        tasks,
        key=lambda t: (t.due or sentinel, t.title.lower()),
        reverse=descending,
    )


# --------------------------------------------------------------------------- #
# Conversion from TaskRow → DashboardTask
# --------------------------------------------------------------------------- #


def task_rows_to_dashboard_tasks(rows: list[Any]) -> list[DashboardTask]:
    """Project ``TaskRow``s (from publish) into the minimal Dashboard shape.

    ``rows`` is a list of :class:`~homework_hub.pipeline.publish.TaskRow`
    instances; typed as ``Any`` here to avoid the import cycle.
    """
    subj_idx = TASKS_TAB.column_index("subject")
    title_idx = TASKS_TAB.column_index("title")
    due_idx = TASKS_TAB.column_index("due")
    status_idx = TASKS_TAB.column_index("status")
    link_idx = TASKS_TAB.column_index("link")

    out: list[DashboardTask] = []
    for r in rows:
        cells = r.cells
        due_cell = cells[due_idx]
        due = due_cell if isinstance(due_cell, date) else None
        out.append(
            DashboardTask(
                subject=str(cells[subj_idx] or ""),
                title=str(cells[title_idx] or ""),
                due=due,
                status=str(cells[status_idx] or ""),
                link=str(cells[link_idx] or ""),
            )
        )
    return out


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #


def build_requests(
    *,
    dash_sheet_id: int,
    tasks: list[DashboardTask],
    today: date,
    existing_table_ids: list[str] | None = None,
    existing_banded_range_ids: list[int] | None = None,
    existing_conditional_format_rule_count: int = 0,
) -> list[dict[str, Any]]:
    """Build the per-publish Dashboard lists region as a single batchUpdate body.

    The Dashboard grid is sized once at template-bootstrap to 1000 rows
    (see ``sheet_template._DASH_GRID_ROW_COUNT``) so that ``addTable``
    can address any row in the lists region without needing a same-batch
    grid resize — Sheets validates ``addTable`` ranges against the
    pre-batch grid size and rejects with an opaque HTTP 500 if the grid
    was grown in the same call.

    Order of emitted requests:

    1. ``deleteTable`` per id in ``existing_table_ids``.
    2. ``deleteBanding`` per id in ``existing_banded_range_ids``.
    3. ``deleteConditionalFormatRule`` x N (drain semantics — always index 0).
    4. ``updateCells`` clearing the lists region.
    5. ``updateDimensionProperties`` setting content-row heights.
    6. Per section: header write, data write, ``addTable``, per-status CF rule,
       header cell formatting.
    7. Footer: formula write + merge + format.
    """
    sections = _compute_section_layouts(tasks, today)
    last_data_row = sections[-1].data_end_row
    footer_row = last_data_row + _SPACER_ROWS

    requests: list[dict[str, Any]] = []
    requests.extend(
        _teardown_requests(
            dash_sheet_id=dash_sheet_id,
            existing_table_ids=existing_table_ids or [],
            existing_banded_range_ids=existing_banded_range_ids or [],
            existing_conditional_format_rule_count=existing_conditional_format_rule_count,
        )
    )
    requests.append(_clear_lists_region_request(dash_sheet_id, footer_row + 1))
    requests.extend(
        _row_height_requests(
            dash_sheet_id=dash_sheet_id,
            sections=sections,
            footer_row=footer_row,
        )
    )
    for section, section_tasks in zip(sections, _sectioned_tasks(tasks, today), strict=True):
        requests.extend(_section_requests(dash_sheet_id, section, section_tasks))
    requests.extend(_footer_requests(dash_sheet_id, footer_row))
    return requests


# --------------------------------------------------------------------------- #
# Section sizing
# --------------------------------------------------------------------------- #


def _sectioned_tasks(tasks: list[DashboardTask], today: date) -> tuple[
    list[DashboardTask],
    list[DashboardTask],
    list[DashboardTask],
    list[DashboardTask],
]:
    return (
        filter_overdue(tasks),
        filter_week(tasks, today),
        filter_upcoming(tasks, today),
        filter_done(tasks, today),
    )


def _compute_section_layouts(tasks: list[DashboardTask], today: date) -> tuple[SectionLayout, ...]:
    """Walk the sections in order, allocating rows as we go.

    Each section consumes 1 header row + ``max(1, len(data))`` data rows
    + 1 spacer row (except the last section which is followed by the
    footer, not a spacer — the footer's preceding row is itself the
    spacer between Tables).
    """
    sec_meta = (
        (DASHBOARD_OVERDUE_TABLE_NAME, DASHBOARD_OVERDUE_TABLE_ID, filter_overdue(tasks)),
        (DASHBOARD_WEEK_TABLE_NAME, DASHBOARD_WEEK_TABLE_ID, filter_week(tasks, today)),
        (
            DASHBOARD_UPCOMING_TABLE_NAME,
            DASHBOARD_UPCOMING_TABLE_ID,
            filter_upcoming(tasks, today),
        ),
        (DASHBOARD_DONE_TABLE_NAME, DASHBOARD_DONE_TABLE_ID, filter_done(tasks, today)),
    )
    # Leading spacer row so the first table's Sheets-rendered name chip
    # gets the same visual breathing room above it as the second and
    # third tables get from inter-section spacers.
    cursor = _LISTS_START_ROW + _SPACER_ROWS
    out: list[SectionLayout] = []
    for i, (name, tid, section_tasks) in enumerate(sec_meta):
        header_row = cursor
        data_rows = max(1, len(section_tasks))
        data_start = header_row + 1
        data_end = data_start + data_rows
        out.append(
            SectionLayout(
                name=name,
                table_id=tid,
                header_row=header_row,
                data_start_row=data_start,
                data_end_row=data_end,
                fallback=len(section_tasks) == 0,
            )
        )
        cursor = data_end
        if i < len(sec_meta) - 1:
            cursor += _SPACER_ROWS
    return tuple(out)


# --------------------------------------------------------------------------- #
# Teardown / frame
# --------------------------------------------------------------------------- #


def _teardown_requests(
    *,
    dash_sheet_id: int,
    existing_table_ids: list[str],
    existing_banded_range_ids: list[int],
    existing_conditional_format_rule_count: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    # Drain CF rules FIRST. ``deleteTable`` cascades and removes any CF
    # rule whose range targeted that table's cells, so deleting tables
    # before CFs would invalidate the count and 400 on the now-vanished
    # rules. Index 0 every time — Sheets re-indexes after each delete.
    for _ in range(existing_conditional_format_rule_count):
        out.append(
            {
                "deleteConditionalFormatRule": {
                    "sheetId": dash_sheet_id,
                    "index": 0,
                }
            }
        )
    for tid in existing_table_ids:
        out.append({"deleteTable": {"tableId": tid}})
    for bid in existing_banded_range_ids:
        out.append({"deleteBanding": {"bandedRangeId": bid}})
    # Unmerge anything in the lists region. Leftover merges from the
    # previous publish's footer cause ``addTable`` requests to fail —
    # Sheets sometimes returns a clean 400 ("merged range") but more
    # often a bare 500 even when the merge does not directly overlap
    # the new table's range. ``unmergeCells`` is a no-op when the range
    # contains no merges, so this is safe to emit unconditionally. The
    # frame's row-1 greeting merge sits above ``_LISTS_START_ROW`` and
    # is untouched.
    out.append(
        {
            "unmergeCells": {
                "range": {
                    "sheetId": dash_sheet_id,
                    "startRowIndex": _LISTS_START_ROW,
                    "endRowIndex": _LISTS_END_ROW_BOUND,
                    "startColumnIndex": 0,
                    "endColumnIndex": _COL_COUNT,
                }
            }
        }
    )
    return out


def _clear_lists_region_request(dash_sheet_id: int, end_row: int) -> dict[str, Any]:
    """Wipe contents + format of every cell from the lists-region start
    through (and including) the footer row, leaving the bottom border row
    intact (it's re-written by the frame, not by us)."""
    return {
        "updateCells": {
            "range": {
                "sheetId": dash_sheet_id,
                "startRowIndex": _LISTS_START_ROW,
                "endRowIndex": end_row,
                "startColumnIndex": 0,
                "endColumnIndex": _COL_COUNT,
            },
            "fields": "userEnteredValue,userEnteredFormat",
        }
    }


def _row_height_requests(
    *,
    dash_sheet_id: int,
    sections: tuple[SectionLayout, ...],
    footer_row: int,
) -> list[dict[str, Any]]:
    """Set per-row heights for the dynamic region.

    Frame rows (top border / greeting / graphics) are already sized by
    the template's reapply path. Here we size the lists region and the
    footer. The grid is pre-sized generously at template bootstrap, so
    rows below the footer keep Sheets' default height.
    """
    out: list[dict[str, Any]] = []
    # Leading spacer row(s) between the graphics frame and the first
    # table's header — sized as spacer rows for consistency with the
    # spacers between sections.
    if sections[0].header_row > _LISTS_START_ROW:
        out.append(
            _dim_height(
                dash_sheet_id,
                _LISTS_START_ROW,
                sections[0].header_row,
                _ROW_HEIGHT_SPACER,
            )
        )
    # Lists region — every header + data row is content height.
    list_start = sections[0].header_row
    list_end = sections[-1].data_end_row  # exclusive
    out.append(_dim_height(dash_sheet_id, list_start, list_end, _ROW_HEIGHT_CONTENT))
    # Spacer rows between sections.
    for i in range(len(sections) - 1):
        spacer_start = sections[i].data_end_row
        spacer_end = sections[i + 1].header_row
        if spacer_end > spacer_start:
            out.append(_dim_height(dash_sheet_id, spacer_start, spacer_end, _ROW_HEIGHT_SPACER))
    # Footer row.
    out.append(_dim_height(dash_sheet_id, footer_row, footer_row + 1, _ROW_HEIGHT_FOOTER))
    return out


def _dim_height(sheet_id: int, start: int, end: int, px: int) -> dict[str, Any]:
    return {
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id,
                "dimension": "ROWS",
                "startIndex": start,
                "endIndex": end,
            },
            "properties": {"pixelSize": px},
            "fields": "pixelSize",
        }
    }


# --------------------------------------------------------------------------- #
# Per-section requests
# --------------------------------------------------------------------------- #


def _section_requests(
    dash_sheet_id: int,
    section: SectionLayout,
    section_tasks: list[DashboardTask],
) -> list[dict[str, Any]]:
    """Emit per-section requests in an order Sheets won't 500 on.

    Important quirks discovered the hard way:

    * ``addTable`` with ``columnProperties`` reliably 500s on a
      Dashboard tab that hosts the KPI scorecard / donut charts. We
      emit a bare ``addTable`` (no column metadata) and immediately
      follow it with an ``updateTable`` that sets ``columnProperties``
      — ``updateTable`` does NOT trip the same poison, so we get
      header-chip column-type icons (Tt / calendar / dropdown) and the
      native full-width DROPDOWN chip on the Status column, matching
      the Tasks tab's look exactly.
    * Layering ``addBanding`` over a Table range also 500s, so we
      don't band Dashboard tables — alternating row tints come from
      the ``addTable``-implicit banding instead.
    """
    out: list[dict[str, Any]] = []
    out.append(_write_header_row(dash_sheet_id, section))
    out.append(_write_data_rows(dash_sheet_id, section, section_tasks))
    out.append(_add_table_request(dash_sheet_id, section))
    out.append(_update_table_columns_request(section))
    out.extend(_header_format_request(dash_sheet_id, section))
    return out


def _write_header_row(dash_sheet_id: int, section: SectionLayout) -> dict[str, Any]:
    """Write the Table header row (Subject | Title | Due | Status)."""
    return _update_cells(
        dash_sheet_id,
        row_index=section.header_row,
        column_index=_COL_CONTENT_START,
        rows=[[_string_cell(h) for h in ("Subject", "Title", "Due", "Status")]],
        fields="userEnteredValue,userEnteredFormat",
    )


def _write_data_rows(
    dash_sheet_id: int,
    section: SectionLayout,
    section_tasks: list[DashboardTask],
) -> dict[str, Any]:
    """Write the data rows. Empty sections get a single fallback row."""
    rows: list[list[dict[str, Any]]] = []
    if not section_tasks:
        fallback = _FALLBACKS.get(section.name, "Nothing here.")
        rows.append(
            [
                _string_cell(""),
                _string_cell(fallback),
                _string_cell(""),
                _string_cell(""),
            ]
        )
    else:
        for t in section_tasks:
            rows.append(
                [
                    _string_cell(t.subject),
                    _title_cell(t),
                    _due_cell(t.due),
                    _string_cell(t.status),
                ]
            )
    return _update_cells(
        dash_sheet_id,
        row_index=section.data_start_row,
        column_index=_COL_CONTENT_START,
        rows=rows,
        fields="userEnteredValue,userEnteredFormat",
    )


def _add_table_request(dash_sheet_id: int, section: SectionLayout) -> dict[str, Any]:
    """Emit a bare ``addTable`` request — NO ``columnProperties``.

    Discovery: when the Dashboard tab carries floating charts (the KPI
    scorecards + donut), ``addTable`` with any ``columnProperties``
    field reliably 500s with "Internal error encountered." regardless of
    range, column count, or types. Removing ``columnProperties`` lets
    the table register cleanly; Sheets infers TEXT for every column.
    Column types (and the Status DROPDOWN) are then set via a separate
    ``updateTable`` request — see :func:`_update_table_columns_request`.
    """
    return {
        "addTable": {
            "table": {
                "name": section.name,
                "tableId": section.table_id,
                "range": {
                    "sheetId": dash_sheet_id,
                    "startRowIndex": section.header_row,
                    "endRowIndex": section.data_end_row,
                    "startColumnIndex": _COL_CONTENT_START,
                    "endColumnIndex": _COL_CONTENT_END,
                },
            }
        }
    }


def _add_banding_request(dash_sheet_id: int, section: SectionLayout) -> dict[str, Any]:
    """Alternating-row banding for the section data rows.

    Uses muted RGB tints — the ``BandingProperties`` colour fields don't
    yet accept ``colorStyle`` so we cannot use theme colours here.
    """
    return {
        "addBanding": {
            "bandedRange": {
                "range": {
                    "sheetId": dash_sheet_id,
                    "startRowIndex": section.data_start_row,
                    "endRowIndex": section.data_end_row,
                    "startColumnIndex": _COL_CONTENT_START,
                    "endColumnIndex": _COL_CONTENT_END,
                },
                "rowProperties": {
                    "firstBandColor": {"red": 1, "green": 1, "blue": 1},
                    "secondBandColor": {
                        "red": 0.96,
                        "green": 0.97,
                        "blue": 0.98,
                    },
                },
            }
        }
    }


def _update_table_columns_request(section: SectionLayout) -> dict[str, Any]:
    """Set ``columnProperties`` + ``rowsProperties.headerColorStyle`` on the
    just-added Table via ``updateTable``.

    Sets column types so the Sheets header renders the column-type icons
    (Tt for TEXT, calendar for DATE, dropdown chip for DROPDOWN) — same
    affordance as the Tasks tab. The Status column uses the native Table
    ``DROPDOWN`` type so the cell renders the full-width chip rather
    than the bare-text + tiny caret you get from a standalone
    ``setDataValidation`` rule.

    Also paints the Table's header chip with :data:`_TABLE_HEADER_COLOR`
    — the same sage green for every section so the lists region reads as
    a cohesive group. Band colours are left at the Sheets defaults.

    Why ``updateTable`` and not ``addTable`` with these fields inline?
    See :func:`_add_table_request` — inline columnProperties 500s when
    the Dashboard hosts floating charts. ``updateTable`` against an
    already-registered table sidesteps the poison; ``rowsProperties``
    rides along on that same request without issue.
    """
    return {
        "updateTable": {
            "table": {
                "tableId": section.table_id,
                "columnProperties": [
                    {
                        "columnIndex": _TBL_COL_SUBJECT,
                        "columnName": "Subject",
                        "columnType": "TEXT",
                    },
                    {
                        "columnIndex": _TBL_COL_TITLE,
                        "columnName": "Title",
                        "columnType": "TEXT",
                    },
                    {
                        "columnIndex": _TBL_COL_DUE,
                        "columnName": "Due",
                        "columnType": "DATE",
                    },
                    {
                        "columnIndex": _TBL_COL_STATUS,
                        "columnName": "Status",
                        "columnType": "DROPDOWN",
                        "dataValidationRule": {
                            "condition": {
                                "type": "ONE_OF_LIST",
                                "values": [{"userEnteredValue": v} for v in STATUS_VALUES],
                            },
                        },
                    },
                ],
                "rowsProperties": {
                    "headerColorStyle": {"rgbColor": _TABLE_HEADER_COLOR},
                },
            },
            "fields": "columnProperties,rowsProperties.headerColorStyle",
        }
    }


def _header_format_request(dash_sheet_id: int, section: SectionLayout) -> list[dict[str, Any]]:
    """Bold header row with a subtle background, scoped to the Table's
    header range. Keeps the header visually distinct from the data rows
    without competing with the floating KPI scorecards above."""
    return [
        {
            "repeatCell": {
                "range": {
                    "sheetId": dash_sheet_id,
                    "startRowIndex": section.header_row,
                    "endRowIndex": section.header_row + 1,
                    "startColumnIndex": _COL_CONTENT_START,
                    "endColumnIndex": _COL_CONTENT_END,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True, "fontSize": 10},
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": (
                    "userEnteredFormat.textFormat.bold,"
                    "userEnteredFormat.textFormat.fontSize,"
                    "userEnteredFormat.horizontalAlignment,"
                    "userEnteredFormat.verticalAlignment"
                ),
            }
        }
    ]


# --------------------------------------------------------------------------- #
# Footer
# --------------------------------------------------------------------------- #

_LASTSYNC_FORMULA = (
    '=IFERROR("Last sync: " & TEXT(VLOOKUP("Last full sync", Settings!A:B, 2, FALSE), '
    '"dd/mm/yyyy HH:mm"), "")'
)


def _footer_requests(dash_sheet_id: int, footer_row: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    out.append(
        _update_cells(
            dash_sheet_id,
            row_index=footer_row,
            column_index=_COL_CONTENT_START,
            rows=[[_formula_cell(_LASTSYNC_FORMULA)]],
            fields="userEnteredValue",
        )
    )
    out.append(
        {
            "mergeCells": {
                "range": {
                    "sheetId": dash_sheet_id,
                    "startRowIndex": footer_row,
                    "endRowIndex": footer_row + 1,
                    "startColumnIndex": _COL_CONTENT_START,
                    "endColumnIndex": _COL_CONTENT_END,
                },
                "mergeType": "MERGE_ALL",
            }
        }
    )
    out.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": dash_sheet_id,
                    "startRowIndex": footer_row,
                    "endRowIndex": footer_row + 1,
                    "startColumnIndex": _COL_CONTENT_START,
                    "endColumnIndex": _COL_CONTENT_END,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"italic": True, "fontSize": 9},
                        "horizontalAlignment": "RIGHT",
                        "padding": {"right": 12},
                    }
                },
                "fields": (
                    "userEnteredFormat.textFormat.italic,"
                    "userEnteredFormat.textFormat.fontSize,"
                    "userEnteredFormat.horizontalAlignment,"
                    "userEnteredFormat.padding"
                ),
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


def _title_cell(task: DashboardTask) -> dict[str, Any]:
    """Title cell — HYPERLINK formula if a link is present, plain text otherwise.

    Escapes embedded double quotes in the title so the formula parses.
    """
    if not task.link:
        return _string_cell(task.title)
    safe_title = task.title.replace('"', '""')
    safe_link = task.link.replace('"', '""')
    formula = f'=HYPERLINK("{safe_link}","{safe_title}")'
    return _formula_cell(formula)


def _due_cell(due: date | None) -> dict[str, Any]:
    """Due cell — written as a Sheets date serial number so the DATE
    column type sorts chronologically. Includes a per-cell numberFormat
    so the date renders as ``dd/MM/yyyy`` regardless of the column-level
    format (which Sheets sometimes drops when an addTable resizes)."""
    if due is None:
        return {
            "userEnteredValue": {"stringValue": ""},
            "userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}},
        }
    serial = (due - _SHEETS_EPOCH).days
    return {
        "userEnteredValue": {"numberValue": serial},
        "userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}},
    }


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


__all__ = [
    "DashboardTask",
    "SectionLayout",
    "build_requests",
    "filter_done",
    "filter_overdue",
    "filter_upcoming",
    "filter_week",
    "task_rows_to_dashboard_tasks",
]


# Tiny convenience for callers that already have a ``datetime`` rather
# than a ``date``: the publish layer typically computes "today" as a
# Melbourne-local date and we accept either.
def _coerce_today(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value

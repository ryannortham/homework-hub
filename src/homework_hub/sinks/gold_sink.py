"""Live :class:`GoldSink` implementation backed by gspread (M5c).

The publish layer's :class:`~homework_hub.pipeline.publish.GoldSink`
Protocol describes four operations:

* :meth:`read_user_edits` — pull the hidden ``UserEdits`` Table.
* :meth:`read_duplicate_checkboxes` — read Confirm/Dismiss state from
  the ``Possible Duplicates`` Table.
* :meth:`write_tab` — replace a tab's data area with the supplied rows
  (header is preserved).
* :meth:`set_tab_hidden` — toggle a tab's ``hidden`` property.

This module wires those onto the live Sheets API. The bootstrap step
(creating the spreadsheet from scratch) lives in
:class:`homework_hub.sinks.sheets_client.SheetsClient.create_sheet`;
``GoldSink`` only handles per-sync reads and writes.

All work goes through gspread for routine ops (cleaner range handling)
and the discovery client only for ``set_tab_hidden`` (gspread has no
direct equivalent).
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import gspread
from google.auth.credentials import Credentials
from googleapiclient.discovery import build

from homework_hub.pipeline.publish import DuplicateCheckboxState, UserEdit
from homework_hub.schema import ColumnKind, ColumnSpec, TabSpec

log = logging.getLogger(__name__)


class GoldSinkError(Exception):
    """Raised when the gold sink can't satisfy a request."""


class GspreadGoldSink:
    """gspread-backed :class:`GoldSink` for the medallion publish stage."""

    def __init__(self, credentials: Credentials):
        self._credentials = credentials
        self._gspread: gspread.Client | None = None
        self._discovery: Any | None = None

    # ------------------------------------------------------------------ #
    # Lazy clients
    # ------------------------------------------------------------------ #

    def _gs(self) -> gspread.Client:
        if self._gspread is None:
            self._gspread = gspread.authorize(self._credentials)
        return self._gspread

    def _disc(self) -> Any:
        if self._discovery is None:
            self._discovery = build(
                "sheets", "v4", credentials=self._credentials, cache_discovery=False
            )
        return self._discovery

    # ------------------------------------------------------------------ #
    # GoldSink: reads
    # ------------------------------------------------------------------ #

    def read_user_edits(self, spreadsheet_id: str) -> list[UserEdit]:
        """Return every persisted kid override from the hidden UserEdits tab.

        Returns an empty list (silently) if the tab is missing — first-run
        spreadsheets won't have a UserEdits tab until publish populates it.
        """
        rows = self._read_tab_rows(spreadsheet_id, "UserEdits")
        if not rows:
            return []
        out: list[UserEdit] = []
        for row in rows:
            # Tab schema: task_uid, column, value, updated_at
            if len(row) < 4:
                continue
            task_uid, column, value, updated_at = row[0], row[1], row[2], row[3]
            if not task_uid or not column:
                continue
            coerced: object = value
            if value in ("TRUE", "FALSE"):
                coerced = value == "TRUE"
            out.append(
                UserEdit(
                    task_uid=task_uid,
                    column=column,
                    value=coerced,
                    updated_at=updated_at,
                )
            )
        return out

    def read_duplicate_checkboxes(self, spreadsheet_id: str) -> list[DuplicateCheckboxState]:
        """Read Confirm/Dismiss state from the Possible Duplicates tab."""
        rows = self._read_tab_rows(spreadsheet_id, "Possible Duplicates")
        if not rows:
            return []
        out: list[DuplicateCheckboxState] = []
        for row in rows:
            # Tab schema columns 0=link_id, 7=confirm, 8=dismiss
            if len(row) < 9:
                continue
            try:
                link_id = int(row[0])
            except (TypeError, ValueError):
                continue
            confirm = row[7] == "TRUE" if len(row) > 7 else False
            dismiss = row[8] == "TRUE" if len(row) > 8 else False
            out.append(DuplicateCheckboxState(link_id=link_id, confirm=confirm, dismiss=dismiss))
        return out

    # ------------------------------------------------------------------ #
    # GoldSink: writes
    # ------------------------------------------------------------------ #

    def write_tab(
        self,
        spreadsheet_id: str,
        tab: TabSpec,
        rows: list[tuple[object, ...]],
    ) -> None:
        """Replace the data area of ``tab`` with ``rows``.

        Plain tabs (no native Table):
          1. Clear everything below row 1 (preserves the header).
          2. Write rows starting at A2 via ``values.update``.

        Table-backed tabs (``tab.table_id`` is set):
          Native Sheets Tables auto-extend only when rows are *appended*
          below their current range via ``values.append``; writing via
          ``values.update`` populates the underlying grid cells but the
          Table widget does not include those rows.  Strategy:
          1. Delete all data rows (row 2 onward) from the sheet so the
             Table is reset to header-only.
          2. Append the new rows via ``append_rows`` (``values.append``),
             which causes the Table to auto-extend to include them.

        ``rows`` are tuples of cell values matching the tab's column order.
        ``None`` becomes an empty string. ``datetime`` objects are
        formatted as ISO date so Sheets parses them as dates.
        """
        sh = self._open(spreadsheet_id)
        try:
            ws = sh.worksheet(tab.name)
        except gspread.WorksheetNotFound as exc:
            raise GoldSinkError(
                f"Tab {tab.name!r} not found in {spreadsheet_id} — "
                "was it bootstrapped via `homework-hub bootstrap-sheet`?"
            ) from exc

        encoded = [[_encode_cell(v) for v in row] for row in rows]

        if tab.table_id:
            self._write_table_tab(spreadsheet_id, ws, tab, encoded)
        else:
            self._write_plain_tab(ws, len(tab.columns), encoded)

    def _write_plain_tab(
        self,
        ws: gspread.Worksheet,
        num_cols: int,
        encoded: list[list[object]],
    ) -> None:
        """Clear + range-write for non-Table tabs."""
        last_col = _col_letter(num_cols)
        ws.batch_clear([f"A2:{last_col}"])
        if not encoded:
            return
        end_row = 1 + len(encoded)
        ws.update(
            range_name=f"A2:{last_col}{end_row}",
            values=encoded,
            value_input_option="USER_ENTERED",
        )

    def _write_table_tab(
        self,
        spreadsheet_id: str,
        ws: gspread.Worksheet,
        tab: TabSpec,
        encoded: list[list[object]],
    ) -> None:
        """Delete all data rows, write fresh rows via updateCells, resize Table.

        ``append_rows`` (``values.append``) deduplicates identical formula
        strings across rows — only the first row receives the formula, all
        subsequent rows get empty cells.  Writing via ``batchUpdate →
        updateCells`` with explicit ``formulaValue`` / ``boolValue`` /
        ``numberValue`` / ``stringValue`` cell descriptors sidesteps this and
        correctly populates every row.

        The ``updateTable`` call at the end resizes the Table range to cover
        header + all data rows so that column type semantics (DATE sort,
        BOOLEAN checkbox, DROPDOWN enforcement, DOUBLE formula evaluation)
        apply to every row.
        """
        disc = self._disc()
        requests: list[dict[str, Any]] = []

        # 1. Delete all current data rows (keep header at row index 0).
        #    Use the actual populated row count, not ws.row_count (which is
        #    the full grid capacity, e.g. 1000). Sheets rejects deleteDimension
        #    when endIndex == row_count because it would remove all non-frozen
        #    rows — we must leave at least one grid row.
        all_values = ws.get_all_values()
        populated_rows = len(all_values)  # includes header
        if populated_rows > 1:
            requests.append({
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "ROWS",
                        "startIndex": 1,
                        "endIndex": populated_rows,
                    }
                }
            })

        # 2. Resize the Table *before* writing data so that structured column
        #    references in formula cells (e.g. ``=[@Due]-TODAY()``) resolve
        #    correctly — they only work when the cell is inside a named Table
        #    column.  endRowIndex = 1 header + len(encoded) data rows.
        requests.append({
            "updateTable": {
                "table": {
                    "tableId": tab.table_id,
                    "name": tab.table_id,
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1 + len(encoded),
                        "startColumnIndex": 0,
                        "endColumnIndex": len(tab.columns),
                    },
                },
                "fields": "range",
            }
        })

        # 3. Write fresh data rows via updateCells with explicit cell value
        #    dicts so formulas, booleans, numbers and strings each use the
        #    correct Sheets API value type — no server-side deduplication.
        #    Formula templates containing ``{row}`` are substituted with the
        #    1-based row number (data starts at row 2 of the sheet).
        #    DATE columns also carry ``userEnteredFormat.numberFormat`` so the
        #    dd/mm/yyyy pattern survives the per-sync deleteDimension.
        if encoded:
            requests.append({
                "updateCells": {
                    "rows": [
                        {"values": [
                            _to_cell_with_format(
                                v.format(row=2 + i) if isinstance(v, str) and "{row}" in v else v,
                                col,
                            )
                            for v, col in zip(row, tab.columns)
                        ]}
                        for i, row in enumerate(encoded)
                    ],
                    "fields": "userEnteredValue,userEnteredFormat.numberFormat",
                    "start": {
                        "sheetId": ws.id,
                        "rowIndex": 1,       # 0-based → row 2
                        "columnIndex": 0,
                    },
                }
            })

        disc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

    def set_tab_hidden(self, spreadsheet_id: str, tab: TabSpec, hidden: bool) -> None:
        """Toggle ``hidden`` on ``tab``."""
        sh = self._open(spreadsheet_id)
        try:
            ws = sh.worksheet(tab.name)
        except gspread.WorksheetNotFound as exc:
            raise GoldSinkError(
                f"Cannot hide missing tab {tab.name!r} in {spreadsheet_id}"
            ) from exc
        self._disc().spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": ws.id, "hidden": hidden},
                            "fields": "hidden",
                        }
                    }
                ]
            },
        ).execute()

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _open(self, spreadsheet_id: str) -> gspread.Spreadsheet:
        return self._gs().open_by_key(spreadsheet_id)

    def _read_tab_rows(self, spreadsheet_id: str, tab_name: str) -> list[list[str]]:
        """Return every data row (row 2+) of ``tab_name`` as raw strings."""
        sh = self._open(spreadsheet_id)
        try:
            ws = sh.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            return []
        all_rows = ws.get_all_values()
        return all_rows[1:] if len(all_rows) > 1 else []


# --------------------------------------------------------------------------- #
# Cell encoding
# --------------------------------------------------------------------------- #

# Sheets date serial epoch: days since 30 Dec 1899.
_SHEETS_EPOCH = date(1899, 12, 30)

# Date format applied to DATE columns on every write so the format survives
# deleteDimension wiping bootstrap repeatCell formats.
_DATE_FORMAT = {"type": "DATE", "pattern": "dd/MM/yyyy"}


def _date_serial(d: date) -> int:
    """Convert a Python date to a Sheets date serial number.

    Sheets stores dates as integer days since 30 Dec 1899.  Writing a
    ``numberValue`` with this integer (rather than a string like
    ``"2026-05-01"``) ensures the cell is treated as a native date —
    enabling chronological TABLE sort rather than lexicographic A-Z sort.
    """
    return (d - _SHEETS_EPOCH).days


def _encode_cell(value: object) -> object:
    """Coerce a Python value into something Sheets' USER_ENTERED accepts.

    ``date`` / ``datetime`` objects are converted to Sheets date serial
    numbers (int days since 30 Dec 1899) so the cell is stored as a
    numeric date rather than a string.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)
        return _date_serial(value.date())
    if isinstance(value, date):
        return _date_serial(value)
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float | str):
        return value
    return str(value)


def _to_cell_value(value: object) -> dict[str, Any]:
    """Convert an already-encoded Python value to a Sheets API cell value dict.

    Used by ``_write_table_tab`` to build ``updateCells`` request bodies so
    that each cell gets the correct value type (formulaValue / boolValue /
    numberValue / stringValue).
    """
    if isinstance(value, bool):
        return {"userEnteredValue": {"boolValue": value}}
    if isinstance(value, (int, float)):
        return {"userEnteredValue": {"numberValue": value}}
    if isinstance(value, str) and value.startswith("="):
        return {"userEnteredValue": {"formulaValue": value}}
    return {"userEnteredValue": {"stringValue": str(value) if value is not None else ""}}


def _to_cell_with_format(value: object, col: ColumnSpec) -> dict[str, Any]:
    """Like ``_to_cell_value`` but also sets ``userEnteredFormat`` for DATE
    columns so the display pattern survives the per-sync deleteDimension.
    """
    cell = _to_cell_value(value)
    if col.kind is ColumnKind.DATE:
        cell = dict(cell)  # shallow copy so we don't mutate the original
        cell["userEnteredFormat"] = {"numberFormat": _DATE_FORMAT}
    return cell


def _col_letter(n: int) -> str:
    """1-based column number → A1 letters (1 → A, 27 → AA)."""
    if n < 1:
        raise ValueError(f"column index must be >= 1, got {n}")
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(ord("A") + rem) + letters
    return letters


__all__ = ["GoldSinkError", "GspreadGoldSink"]

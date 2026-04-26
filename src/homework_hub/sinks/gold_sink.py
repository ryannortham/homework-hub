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
from datetime import UTC, datetime
from typing import Any

import gspread
from google.auth.credentials import Credentials
from googleapiclient.discovery import build

from homework_hub.pipeline.publish import DuplicateCheckboxState, UserEdit
from homework_hub.schema import TabSpec

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
        """Delete all data rows, append fresh rows, then resize the Table.

        Deleting rows resets the Table's row range back to header-only.
        ``append_rows`` (``values.append``) writes data into the grid but
        does NOT cause the native Table to auto-extend its range — the Table
        widget stays frozen at its original ``endRowIndex``.  We therefore
        follow every write with an ``updateTable`` batchUpdate to explicitly
        set the Table range to cover header + all appended rows, which is what
        makes column types (DATE, BOOLEAN, DROPDOWN, FORMULA) apply correctly.
        """
        disc = self._disc()

        # 1. Delete all current data rows (keep header at row 0).
        current_row_count = ws.row_count
        if current_row_count > 1:
            disc.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "deleteDimension": {
                                "range": {
                                    "sheetId": ws.id,
                                    "dimension": "ROWS",
                                    "startIndex": 1,  # 0-based → row 2
                                    "endIndex": current_row_count,
                                }
                            }
                        }
                    ]
                },
            ).execute()

        # 2. Append new rows (grid write — Table range not updated yet).
        if encoded:
            ws.append_rows(
                encoded,
                value_input_option="USER_ENTERED",
                insert_data_option="INSERT_ROWS",
                table_range="A1",
            )

        # 3. Resize the Table to cover header + all data rows so that column
        #    types (DATE sort, BOOLEAN checkbox, DROPDOWN, FORMULA) apply.
        #    endRowIndex = 1 header row + len(encoded) data rows.
        #    When encoded is empty the Table shrinks to header-only (valid).
        disc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
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
                    }
                ]
            },
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


def _encode_cell(value: object) -> object:
    """Coerce a Python value into something Sheets' USER_ENTERED accepts."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        # Convert UTC to a naive ISO date for Sheets DATE parsing.
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)
        return value.date().isoformat()
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float | str):
        return value
    return str(value)


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

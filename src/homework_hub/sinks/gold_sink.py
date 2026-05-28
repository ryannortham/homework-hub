"""Live :class:`GoldSink` implementation backed by gspread (M5c).

The publish layer's :class:`~homework_hub.pipeline.publish.GoldSink`
Protocol describes four operations:

* :meth:`read_user_edits` — pull the hidden ``UserEdits`` Table.
* :meth:`read_tab_raw` — return raw rows from any named tab.
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

from homework_hub.pipeline.publish import DashboardMeta, UserEdit
from homework_hub.schema import DASHBOARD_TAB, ColumnKind, TabSpec

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
            # Tab schema: task_uid, column, original_value, value, updated_at.
            # Tolerate legacy 4-column rows (task_uid, column, value, updated_at)
            # produced before original_value was introduced — they get
            # original_value="" until the next publish refreshes them.
            if len(row) < 4:
                continue
            task_uid = row[0]
            column = row[1]
            if len(row) >= 5:
                original_value, value, updated_at = row[2], row[3], row[4]
            else:
                original_value = ""
                value, updated_at = row[2], row[3]
            if not task_uid or not column:
                continue
            coerced: object = value
            if value in ("TRUE", "FALSE"):
                coerced = value == "TRUE"
            coerced_orig: object = original_value
            if original_value in ("TRUE", "FALSE"):
                coerced_orig = original_value == "TRUE"
            out.append(
                UserEdit(
                    task_uid=task_uid,
                    column=column,
                    value=coerced,
                    updated_at=updated_at,
                    original_value=coerced_orig,
                )
            )
        return out

    def read_tab_raw(self, spreadsheet_id: str, tab_name: str) -> list[list[str]]:
        """Return raw data rows (header stripped) from any named tab."""
        return self._read_tab_rows(spreadsheet_id, tab_name)

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
            self._write_plain_tab(ws, tab, encoded)

    def _write_plain_tab(
        self,
        ws: gspread.Worksheet,
        tab: TabSpec,
        encoded: list[list[object]],
    ) -> None:
        """Clear + range-write for non-Table tabs.

        For data-bearing plain tabs (e.g. Settings), the header row is
        rewritten from the tab schema so on-sheet headers stay in sync
        with code-side schema changes. Formula-only tabs (e.g. Today —
        a single ``=QUERY(...)`` in A1) are not header-managed: we clear
        from A2 down so the formula cell is preserved.
        """
        num_cols = len(tab.columns)
        last_col = _col_letter(num_cols)
        is_formula_only = all(c.kind is ColumnKind.FORMULA for c in tab.columns)
        if is_formula_only:
            ws.batch_clear([f"A2:{last_col}"])
            if not encoded:
                return
            end_row = 1 + len(encoded)
            ws.update(
                range_name=f"A2:{last_col}{end_row}",
                values=encoded,
                value_input_option="USER_ENTERED",
            )
            return

        headers = [[c.header for c in tab.columns]]
        ws.batch_clear([f"A1:{last_col}"])
        ws.update(
            range_name=f"A1:{last_col}1",
            values=headers,
            value_input_option="USER_ENTERED",
        )
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
            requests.append(
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "ROWS",
                            "startIndex": 1,
                            "endIndex": populated_rows,
                        }
                    }
                }
            )

        # 1b. If we have more data rows than the grid can hold after the delete,
        #     append the extra rows. After deleteDimension, the grid has 1 row
        #     (the header). ws.row_count is the total grid capacity, but after
        #     deletion we only have max(1, ws.row_count - (populated_rows - 1))
        #     rows. Simpler: always append enough rows to fit all new data.
        #     appendDimension is safe to call even when rows already exist —
        #     it adds rows at the end of the grid.
        if encoded:
            rows_needed = len(encoded)  # data rows after header
            # After deletion we have at most ws.row_count - (populated_rows - 1) rows.
            # To be safe, just always insert the required number of rows.
            requests.append(
                {
                    "appendDimension": {
                        "sheetId": ws.id,
                        "dimension": "ROWS",
                        "length": rows_needed,
                    }
                }
            )

        # 2. Resize the Table *before* writing data so that structured column
        #    references in formula cells (e.g. ``=[@Due]-TODAY()``) resolve
        #    correctly — they only work when the cell is inside a named Table
        #    column.  endRowIndex = 1 header + len(encoded) data rows.
        requests.append(
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
        )

        # 3. Write fresh data rows via updateCells with explicit cell value
        #    dicts so formulas, booleans, numbers and strings each use the
        #    correct Sheets API value type — no server-side deduplication.
        #    Formula templates containing ``{row}`` are substituted with the
        #    1-based row number (data starts at row 2 of the sheet).
        #    We do NOT write userEnteredFormat here: the bootstrap repeatCell
        #    pass applies column formats (e.g. dd/MM/yyyy for DATE) and they
        #    survive deleteDimension. Writing format via SA credentials would
        #    normalise dd/MM/yyyy to M/d/yyyy (SA account is en_US).
        if encoded:
            requests.append(
                {
                    "updateCells": {
                        "rows": [
                            {
                                "values": [
                                    _to_cell_value(
                                        v.format(row=2 + i)
                                        if isinstance(v, str) and "{row}" in v
                                        else v
                                    )
                                    for v in row
                                ]
                            }
                            for i, row in enumerate(encoded)
                        ],
                        "fields": "userEnteredValue",
                        "start": {
                            "sheetId": ws.id,
                            "rowIndex": 1,  # 0-based → row 2
                            "columnIndex": 0,
                        },
                    }
                }
            )

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
    # GoldSink: Dashboard layout (v5.0)
    # ------------------------------------------------------------------ #

    def read_dashboard_meta(self, spreadsheet_id: str) -> DashboardMeta:
        """Return the Dashboard tab's live metadata for publish-time relayout.

        We ask the API for the Dashboard sheet's properties, tables,
        banded ranges, conditional-format rule count and protected-range
        ids in a single ``spreadsheets.get`` call so the per-publish
        overhead is bounded.

        ``tables`` are filtered to those whose tableId matches a known
        Dashboard table id — leaves the user's own Tables (if any) alone.
        Banded ranges and CF rule counts are returned wholesale because
        the v5.0 layout module owns 100% of the Dashboard sheet's
        banding + CF surface (no other code emits those artefacts on
        this tab).

        ``protected_range_ids`` lists only *whole-sheet* protected
        ranges (no inner ``range`` body) — those are the ones publish
        installs and would otherwise re-install on every sync. Any
        cell-scoped protections a kid or admin might add by hand are
        ignored on read so they don't accidentally suppress the
        whole-sheet install.
        """
        from homework_hub.schema import DASHBOARD_TABLE_IDS

        disc = self._disc()
        resp = (
            disc.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields=(
                    "properties(spreadsheetTheme(themeColors(colorType,color(rgbColor)))),"
                    "sheets(properties(sheetId,title),"
                    "tables(tableId),"
                    "bandedRanges(bandedRangeId),"
                    "conditionalFormats,"
                    "protectedRanges(protectedRangeId,range))"
                ),
            )
            .execute()
        )
        theme_accent = _extract_accent1(
            (resp.get("properties") or {}).get("spreadsheetTheme")
        )
        for sheet in resp.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("title") != DASHBOARD_TAB.name:
                continue
            tables = sheet.get("tables", []) or []
            bandings = sheet.get("bandedRanges", []) or []
            cf_rules = sheet.get("conditionalFormats", []) or []
            protected = sheet.get("protectedRanges", []) or []
            # Whole-sheet protection has either no ``range`` at all or a
            # ``range`` whose only field is ``sheetId`` — neither
            # ``startRowIndex``/``endRowIndex`` nor the column equivalents
            # are present. Anything else is a kid- or admin-installed
            # cell-scoped protection we shouldn't touch.
            whole_sheet_ids = [
                int(pr["protectedRangeId"])
                for pr in protected
                if "protectedRangeId" in pr
                and not any(
                    k in (pr.get("range") or {})
                    for k in ("startRowIndex", "endRowIndex", "startColumnIndex", "endColumnIndex")
                )
            ]
            return DashboardMeta(
                sheet_id=int(props["sheetId"]),
                table_ids=[
                    str(t["tableId"]) for t in tables if t.get("tableId") in DASHBOARD_TABLE_IDS
                ],
                banded_range_ids=[
                    int(b["bandedRangeId"]) for b in bandings if "bandedRangeId" in b
                ],
                conditional_format_rule_count=len(cf_rules),
                protected_range_ids=whole_sheet_ids,
                theme_accent=theme_accent,
            )
        raise GoldSinkError(f"Dashboard tab {DASHBOARD_TAB.name!r} not found in {spreadsheet_id}")

    def write_dashboard_layout(self, spreadsheet_id: str, requests: list[dict[str, Any]]) -> None:
        """Execute the pre-built Dashboard layout batchUpdate.

        ``requests`` comes from
        :func:`homework_hub.dashboard_layout.build_requests`. The
        Dashboard grid is pre-sized at template bootstrap so all
        ``addTable`` ranges fit within the existing grid bounds, letting
        the whole layout ship as a single batchUpdate call.

        A no-op when ``requests`` is empty.
        """
        if not requests:
            return
        self._disc().spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

    def write_dashboard_protection(
        self,
        spreadsheet_id: str,
        dashboard_sheet_id: int,
    ) -> None:
        """Install a whole-sheet hard-lock protected range on the Dashboard.

        The only editor listed is this sink's own service account, so
        the publisher continues to write through regardless of who owns
        the spreadsheet. Idempotency is enforced by the caller — see
        :func:`homework_hub.pipeline.publish.publish_for_child`, which
        skips this call when ``DashboardMeta.protected_range_ids`` is
        non-empty.

        Raises ``GoldSinkError`` if the underlying credentials don't
        expose a ``service_account_email`` (e.g. user-installed
        application-default credentials), since omitting the editors
        list would defer to the file owner and could lock the publisher
        out on kid-owned sheets.
        """
        from homework_hub.dashboard_layout import build_protect_dashboard_request

        email = getattr(self._credentials, "service_account_email", None)
        if not email:
            raise GoldSinkError(
                "write_dashboard_protection requires service-account "
                "credentials with a ``service_account_email`` attribute"
            )
        request = build_protect_dashboard_request(
            dashboard_sheet_id=dashboard_sheet_id,
            service_account_email=email,
        )
        self._disc().spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [request]},
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


def _date_serial(d: date) -> int:
    """Convert a Python date to a Sheets date serial number.

    Sheets stores dates as integer days since 30 Dec 1899.  Writing a
    ``numberValue`` with this integer (rather than a string like
    ``"2026-05-01"``) ensures the cell is treated as a native date —
    enabling chronological TABLE sort rather than lexicographic A-Z sort.
    The column format (dd/MM/yyyy) is applied once at bootstrap via
    ``repeatCell`` and survives ``deleteDimension`` automatically.
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


def _col_letter(n: int) -> str:
    """1-based column number → A1 letters (1 → A, 27 → AA)."""
    if n < 1:
        raise ValueError(f"column index must be >= 1, got {n}")
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(ord("A") + rem) + letters
    return letters


def _extract_accent1(theme: dict[str, Any] | None) -> dict[str, float] | None:
    """Return the spreadsheet theme's ACCENT1 colour as an
    ``{"red","green","blue"}`` float dict, or ``None`` if unreadable.

    Sheets omits channel keys whose value is ``0.0``; callers must
    therefore default missing channels to zero. We do that here so
    consumers get a fully-populated dict.
    """
    if not isinstance(theme, dict):
        return None
    for entry in theme.get("themeColors", []) or []:
        if not isinstance(entry, dict):
            continue
        if entry.get("colorType") != "ACCENT1":
            continue
        rgb = (entry.get("color") or {}).get("rgbColor") or {}
        if not isinstance(rgb, dict):
            return None
        return {
            "red": float(rgb.get("red", 0.0)),
            "green": float(rgb.get("green", 0.0)),
            "blue": float(rgb.get("blue", 0.0)),
        }
    return None


__all__ = ["GoldSinkError", "GspreadGoldSink"]

"""Google Sheets API client — wraps service-account auth + bootstrap + sync writes.

Two API surfaces are used together:

- ``googleapiclient.discovery`` for ``spreadsheets.create`` and
  ``spreadsheets.batchUpdate`` (needed to apply the template; gspread
  doesn't expose the full update-cells / addConditionalFormatRule surface).
- ``gspread`` for the per-sync read of the Raw tab, in-place row updates,
  and the bottom-append. Faster to write against and handles range/A1
  conversions cleanly.

This module isolates all live Google API calls behind a small interface so
the orchestrator can be unit-tested with a fake.
"""

from __future__ import annotations

import json
from typing import Any, Protocol

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from homework_hub.sheet_template import bootstrap_requests
from homework_hub.sinks.sheets import RAW_HEADERS
from homework_hub.sinks.sheets_diff import RawDiff

# Scopes needed for: create sheet, batchUpdate (apply template), read/write
# values, share via drive API. The drive scope is needed by gspread.share().
DEFAULT_SCOPES: list[str] = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

RAW_TAB_NAME = "Raw"


class SheetsAPIError(Exception):
    """Raised when the Sheets API returns an unrecoverable error."""


class SheetsBackend(Protocol):
    """Interface the orchestrator depends on; lets tests inject a fake."""

    def create_sheet(self, title: str, *, share_with: list[str] | None = None) -> str: ...

    def read_raw_rows(self, spreadsheet_id: str) -> list[list[str]]: ...

    def apply_diff(self, spreadsheet_id: str, diff: RawDiff) -> None: ...


def load_service_account_credentials(
    raw: str | dict[str, Any], *, scopes: list[str] | None = None
) -> Credentials:
    """Build google-auth Credentials from a service-account key JSON.

    ``raw`` may be the JSON text or a parsed dict (matches the BitwardenCLI
    notes / file-read pattern).
    """
    info = json.loads(raw) if isinstance(raw, str) else raw
    return Credentials.from_service_account_info(info, scopes=scopes or DEFAULT_SCOPES)


class SheetsClient:
    """Concrete SheetsBackend backed by the live Google APIs."""

    def __init__(self, credentials: Credentials):
        self._credentials = credentials
        # Lazy: only construct the discovery service once needed.
        self._sheets_service: Any | None = None
        self._gspread_client: gspread.Client | None = None

    # ------------------------------------------------------------------ #
    # Lazy clients
    # ------------------------------------------------------------------ #

    def _sheets(self) -> Any:
        if self._sheets_service is None:
            self._sheets_service = build(
                "sheets", "v4", credentials=self._credentials, cache_discovery=False
            )
        return self._sheets_service

    def _gspread(self) -> gspread.Client:
        if self._gspread_client is None:
            self._gspread_client = gspread.authorize(self._credentials)
        return self._gspread_client

    # ------------------------------------------------------------------ #
    # Bootstrap
    # ------------------------------------------------------------------ #

    def create_sheet(self, title: str, *, share_with: list[str] | None = None) -> str:
        """Create a new spreadsheet and apply the homework-hub template.

        Returns the new spreadsheet's ID. Optionally shares it with the given
        emails as Editors so the kid (and you) can open it.
        """
        try:
            created = (
                self._sheets()
                .spreadsheets()
                .create(body={"properties": {"title": title}})
                .execute()
            )
        except HttpError as exc:
            raise SheetsAPIError(f"Failed to create sheet '{title}': {exc}") from exc

        spreadsheet_id = created["spreadsheetId"]

        try:
            self._sheets().spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": bootstrap_requests()},
            ).execute()
        except HttpError as exc:
            raise SheetsAPIError(f"Failed to apply template to {spreadsheet_id}: {exc}") from exc

        if share_with:
            sh = self._gspread().open_by_key(spreadsheet_id)
            for email in share_with:
                sh.share(email, perm_type="user", role="writer", notify=False)

        return spreadsheet_id

    # ------------------------------------------------------------------ #
    # Raw-tab read + diff apply
    # ------------------------------------------------------------------ #

    def read_raw_rows(self, spreadsheet_id: str) -> list[list[str]]:
        """Return all rows currently on the Raw tab, including the header."""
        sh = self._gspread().open_by_key(spreadsheet_id)
        try:
            ws = sh.worksheet(RAW_TAB_NAME)
        except gspread.WorksheetNotFound as exc:
            raise SheetsAPIError(
                f"Spreadsheet {spreadsheet_id} has no '{RAW_TAB_NAME}' tab — "
                "was it bootstrapped via `homework-hub bootstrap-sheet`?"
            ) from exc
        return ws.get_all_values()

    def apply_diff(self, spreadsheet_id: str, diff: RawDiff) -> None:
        """Apply a precomputed RawDiff to the Raw tab.

        Strategy:
        - In-place updates: one batch_update call grouping all changed rows.
        - Appends: a single batch range write at ``next_append_row``.
        Both no-ops are skipped to avoid empty API calls.
        """
        if not diff.updates and not diff.appends:
            return

        sh = self._gspread().open_by_key(spreadsheet_id)
        ws = sh.worksheet(RAW_TAB_NAME)

        last_col = _col_letter(len(RAW_HEADERS))

        if diff.updates:
            data: list[dict[str, Any]] = []
            for row_num, values in diff.updates.items():
                rng = f"{RAW_TAB_NAME}!A{row_num}:{last_col}{row_num}"
                data.append({"range": rng, "values": [values]})
            ws.batch_update(data, value_input_option="USER_ENTERED")

        if diff.appends:
            start_row = diff.next_append_row
            end_row = start_row + len(diff.appends) - 1
            rng = f"A{start_row}:{last_col}{end_row}"
            ws.update(values=diff.appends, range_name=rng, value_input_option="USER_ENTERED")


def _col_letter(n: int) -> str:
    """Convert 1-based column number to A1 letters (1 -> A, 27 -> AA)."""
    if n < 1:
        raise ValueError(f"column index must be >= 1, got {n}")
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(ord("A") + rem) + letters
    return letters

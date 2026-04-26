"""Google Sheets API client — service-account auth + sheet bootstrap.

Two API surfaces are used together:

- ``googleapiclient.discovery`` for ``spreadsheets.create`` and
  ``spreadsheets.batchUpdate`` (needed to apply the template; gspread
  doesn't expose the full update-cells / addTable / setDataValidation
  surface).
- ``gspread`` for the post-create share() (cleaner than driveapi's
  permissions endpoint).

This module only handles **sheet bootstrap**. Per-sync reads and writes
live in :class:`homework_hub.sinks.gold_sink.GspreadGoldSink`, which is
constructed against either the SA (daemon) or the human bootstrap user
(``homework-hub bootstrap-sheet``).
"""

from __future__ import annotations

import json
from typing import Any, Protocol

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from homework_hub.sheet_template import bootstrap_requests

# Scopes needed for: create sheet, batchUpdate (apply template), read/write
# values, share via drive API. The drive scope is needed by gspread.share().
DEFAULT_SCOPES: list[str] = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetsAPIError(Exception):
    """Raised when the Sheets API returns an unrecoverable error."""


class SheetsBackend(Protocol):
    """Minimal interface used by ``bootstrap-sheet``; lets tests inject a fake."""

    def create_sheet(self, title: str, *, share_with: list[str] | None = None) -> str: ...


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
    """Concrete :class:`SheetsBackend` backed by the live Google APIs."""

    def __init__(self, credentials: Credentials):
        self._credentials = credentials
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

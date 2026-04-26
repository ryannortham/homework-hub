"""Tests for the Google Sheets client wrapper.

We don't hit the live API; instead we exercise the pure helpers
(``_col_letter``, ``load_service_account_credentials``) and verify the
``apply_diff`` orchestration against an in-memory fake worksheet that
captures the ``batch_update`` / ``update`` calls gspread would have made.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import pytest

from homework_hub.sinks.sheets import RAW_HEADERS
from homework_hub.sinks.sheets_client import (
    SheetsClient,
    _col_letter,
    load_service_account_credentials,
)
from homework_hub.sinks.sheets_diff import RawDiff

# --------------------------------------------------------------------------- #
# _col_letter
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("n", "expected"),
    [
        (1, "A"),
        (2, "B"),
        (26, "Z"),
        (27, "AA"),
        (28, "AB"),
        (52, "AZ"),
        (53, "BA"),
        (702, "ZZ"),
    ],
)
def test_col_letter_known_values(n: int, expected: str):
    assert _col_letter(n) == expected


def test_col_letter_rejects_zero():
    with pytest.raises(ValueError):
        _col_letter(0)


# --------------------------------------------------------------------------- #
# load_service_account_credentials
# --------------------------------------------------------------------------- #


def test_load_service_account_credentials_accepts_str_or_dict():
    info = {
        "type": "service_account",
        "project_id": "p",
        "private_key_id": "k",
        # Valid-looking PEM body is required by google-auth's parser.
        "private_key": _DUMMY_KEY,
        "client_email": "sa@p.iam.gserviceaccount.com",
        "client_id": "1",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://example.com",
    }
    creds_from_dict = load_service_account_credentials(info)
    creds_from_str = load_service_account_credentials(json.dumps(info))
    assert creds_from_dict.service_account_email == "sa@p.iam.gserviceaccount.com"
    assert creds_from_str.service_account_email == "sa@p.iam.gserviceaccount.com"


# --------------------------------------------------------------------------- #
# apply_diff against a fake gspread worksheet
# --------------------------------------------------------------------------- #


class _FakeWorksheet:
    def __init__(self) -> None:
        self.batch_calls: list[tuple[list[dict[str, Any]], str]] = []
        self.update_calls: list[tuple[list[list[str]], str, str]] = []

    def batch_update(self, data: list[dict[str, Any]], value_input_option: str) -> None:
        self.batch_calls.append((data, value_input_option))

    def update(self, *, values: list[list[str]], range_name: str, value_input_option: str) -> None:
        self.update_calls.append((values, range_name, value_input_option))


class _FakeSpreadsheet:
    def __init__(self, ws: _FakeWorksheet):
        self._ws = ws

    def worksheet(self, name: str) -> _FakeWorksheet:
        assert name == "Raw"
        return self._ws


class _FakeGspread:
    def __init__(self, ws: _FakeWorksheet):
        self._sh = _FakeSpreadsheet(ws)

    def open_by_key(self, _spreadsheet_id: str) -> _FakeSpreadsheet:
        return self._sh


def _client_with_fake(ws: _FakeWorksheet) -> SheetsClient:
    client = SheetsClient.__new__(SheetsClient)  # bypass __init__ (no live creds)
    client._credentials = None  # type: ignore[attr-defined]
    client._sheets_service = None  # type: ignore[attr-defined]
    client._gspread_client = _FakeGspread(ws)  # type: ignore[attr-defined]
    return client


def test_apply_diff_noop_when_empty():
    ws = _FakeWorksheet()
    client = _client_with_fake(ws)
    diff = RawDiff(updates={}, appends=[], unchanged_keys=[], next_append_row=2)
    client.apply_diff("sheet-id", diff)
    assert ws.batch_calls == []
    assert ws.update_calls == []


def test_apply_diff_writes_updates_and_appends():
    ws = _FakeWorksheet()
    client = _client_with_fake(ws)
    row_a = ["a"] * len(RAW_HEADERS)
    row_b = ["b"] * len(RAW_HEADERS)
    diff = RawDiff(
        updates={3: row_a},
        appends=[row_b],
        unchanged_keys=[],
        next_append_row=5,
    )
    client.apply_diff("sheet-id", diff)
    last_col = _col_letter(len(RAW_HEADERS))

    assert len(ws.batch_calls) == 1
    data, opt = ws.batch_calls[0]
    assert opt == "USER_ENTERED"
    assert data == [{"range": f"A3:{last_col}3", "values": [row_a]}]

    assert len(ws.update_calls) == 1
    values, range_name, opt = ws.update_calls[0]
    assert opt == "USER_ENTERED"
    assert values == [row_b]
    assert range_name == f"A5:{last_col}5"


def test_apply_diff_skips_appends_when_only_updates():
    ws = _FakeWorksheet()
    client = _client_with_fake(ws)
    diff = RawDiff(
        updates={2: ["x"] * len(RAW_HEADERS)},
        appends=[],
        unchanged_keys=[],
        next_append_row=3,
    )
    client.apply_diff("sheet-id", diff)
    assert len(ws.batch_calls) == 1
    assert ws.update_calls == []


# --------------------------------------------------------------------------- #
# read_raw_rows wraps WorksheetNotFound nicely
# --------------------------------------------------------------------------- #


def test_read_raw_rows_raises_helpful_error_when_tab_missing():
    import gspread

    from homework_hub.sinks.sheets_client import SheetsAPIError

    class _MissingTabSpreadsheet:
        def worksheet(self, _name: str):
            raise gspread.WorksheetNotFound("nope")

    class _MissingTabGspread:
        def open_by_key(self, _id: str):
            return _MissingTabSpreadsheet()

    client = SheetsClient.__new__(SheetsClient)
    client._credentials = None  # type: ignore[attr-defined]
    client._sheets_service = None  # type: ignore[attr-defined]
    client._gspread_client = _MissingTabGspread()  # type: ignore[attr-defined]

    with pytest.raises(SheetsAPIError, match="bootstrap-sheet"):
        client.read_raw_rows("sheet-id")


# --------------------------------------------------------------------------- #
# create_sheet wires through the discovery service + applies the template
# --------------------------------------------------------------------------- #


def test_create_sheet_applies_bootstrap_requests_and_shares():
    """The discovery service is patched; we verify the call sequence."""
    captured: dict[str, Any] = {}

    class _FakeBatchUpdate:
        def execute(self) -> dict[str, Any]:
            return {}

    class _FakeCreate:
        def execute(self) -> dict[str, Any]:
            return {"spreadsheetId": "new-id"}

    class _FakeSpreadsheetsAPI:
        def create(self, body: dict[str, Any]) -> _FakeCreate:
            captured["create_body"] = body
            return _FakeCreate()

        def batchUpdate(  # noqa: N802 — Google API names
            self,
            *,
            spreadsheetId: str,  # noqa: N803
            body: dict[str, Any],
        ) -> _FakeBatchUpdate:
            captured["batch_id"] = spreadsheetId
            captured["batch_body"] = body
            return _FakeBatchUpdate()

    class _FakeService:
        def spreadsheets(self) -> _FakeSpreadsheetsAPI:
            return _FakeSpreadsheetsAPI()

    class _FakeSpreadsheetForShare:
        def __init__(self) -> None:
            self.shared: list[tuple[str, str, str]] = []

        def share(self, email: str, *, perm_type: str, role: str, notify: bool) -> None:
            self.shared.append((email, perm_type, role))

    fake_share_sh = _FakeSpreadsheetForShare()

    class _FakeGspreadForShare:
        def open_by_key(self, _id: str) -> _FakeSpreadsheetForShare:
            return fake_share_sh

    client = SheetsClient.__new__(SheetsClient)
    client._credentials = None  # type: ignore[attr-defined]
    client._sheets_service = _FakeService()  # type: ignore[attr-defined]
    client._gspread_client = _FakeGspreadForShare()  # type: ignore[attr-defined]

    with patch(
        "homework_hub.sinks.sheets_client.bootstrap_requests",
        return_value=[{"addSheet": {"properties": {"title": "Raw"}}}],
    ):
        sheet_id = client.create_sheet("Test", share_with=["kid@example.com"])

    assert sheet_id == "new-id"
    assert captured["create_body"] == {"properties": {"title": "Test"}}
    assert captured["batch_id"] == "new-id"
    assert captured["batch_body"]["requests"][0]["addSheet"]["properties"]["title"] == "Raw"
    assert fake_share_sh.shared == [("kid@example.com", "user", "writer")]


# --------------------------------------------------------------------------- #
# Test-only key fixture (RSA private key in PEM form, generated at import).
# --------------------------------------------------------------------------- #


def _generate_test_pem() -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


_DUMMY_KEY = _generate_test_pem()

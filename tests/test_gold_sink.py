"""Tests for ``homework_hub.sinks.gold_sink``.

These exercise:

* Cell encoding (``_encode_cell``) — None, datetime (UTC + naive), bool,
  numeric, string, and arbitrary objects.
* Column-letter helper (``_col_letter``) — single, boundary, and
  double-letter columns.
* :class:`GspreadGoldSink` against an in-memory fake gspread client to
  validate read/write/hide behaviour without touching the network.
"""

from __future__ import annotations

from datetime import UTC, datetime, timezone
from unittest.mock import MagicMock

import gspread
import pytest

from homework_hub.schema import TASKS_TAB, USER_EDITS_TAB
from homework_hub.sinks.gold_sink import (
    GoldSinkError,
    GspreadGoldSink,
    _col_letter,
    _encode_cell,
)

# --------------------------------------------------------------------------- #
# _encode_cell
# --------------------------------------------------------------------------- #


def test_encode_cell_none_becomes_empty_string():
    assert _encode_cell(None) == ""


def test_encode_cell_utc_datetime_becomes_iso_date():
    dt = datetime(2026, 4, 26, 10, 30, tzinfo=UTC)
    assert _encode_cell(dt) == "2026-04-26"


def test_encode_cell_non_utc_datetime_is_normalised_to_utc_first():
    # 23:30 in UTC+10 == 13:30 UTC same day
    aedt = timezone(__import__("datetime").timedelta(hours=10))
    dt = datetime(2026, 4, 26, 23, 30, tzinfo=aedt)
    assert _encode_cell(dt) == "2026-04-26"


def test_encode_cell_naive_datetime_uses_date_directly():
    dt = datetime(2026, 4, 26, 10, 30)
    assert _encode_cell(dt) == "2026-04-26"


def test_encode_cell_preserves_bool():
    assert _encode_cell(True) is True
    assert _encode_cell(False) is False


def test_encode_cell_preserves_str_int_float():
    assert _encode_cell("hello") == "hello"
    assert _encode_cell(42) == 42
    assert _encode_cell(3.14) == 3.14


def test_encode_cell_falls_back_to_str_for_unknown_types():
    class Foo:
        def __str__(self) -> str:
            return "foo!"

    assert _encode_cell(Foo()) == "foo!"


# --------------------------------------------------------------------------- #
# _col_letter
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "n,expected",
    [
        (1, "A"),
        (2, "B"),
        (26, "Z"),
        (27, "AA"),
        (52, "AZ"),
        (53, "BA"),
        (702, "ZZ"),
    ],
)
def test_col_letter_known_values(n: int, expected: str):
    assert _col_letter(n) == expected


def test_col_letter_rejects_zero_and_negative():
    with pytest.raises(ValueError):
        _col_letter(0)
    with pytest.raises(ValueError):
        _col_letter(-3)


# --------------------------------------------------------------------------- #
# Fake gspread plumbing
# --------------------------------------------------------------------------- #


class FakeWorksheet:
    """Minimal gspread.Worksheet stand-in capturing calls + holding rows."""

    def __init__(self, name: str, rows: list[list[str]] | None = None, ws_id: int = 0):
        self.title = name
        self.id = ws_id
        self._rows = rows or []
        self.cleared: list[list[str]] = []
        self.updates: list[dict] = []

    def get_all_values(self) -> list[list[str]]:
        return [list(r) for r in self._rows]

    def batch_clear(self, ranges: list[str]) -> None:
        self.cleared.append(ranges)

    def update(self, *, range_name: str, values, value_input_option: str) -> None:
        self.updates.append(
            {
                "range_name": range_name,
                "values": values,
                "value_input_option": value_input_option,
            }
        )


class FakeSpreadsheet:
    def __init__(self, worksheets: dict[str, FakeWorksheet]):
        self._ws = worksheets

    def worksheet(self, name: str) -> FakeWorksheet:
        if name not in self._ws:
            raise gspread.WorksheetNotFound(name)
        return self._ws[name]


class FakeGspreadClient:
    def __init__(self, spreadsheet: FakeSpreadsheet):
        self._sh = spreadsheet
        self.opened: list[str] = []

    def open_by_key(self, key: str) -> FakeSpreadsheet:
        self.opened.append(key)
        return self._sh


def _make_sink(worksheets: dict[str, FakeWorksheet]) -> tuple[GspreadGoldSink, FakeGspreadClient]:
    sink = GspreadGoldSink(credentials=MagicMock())
    fake_client = FakeGspreadClient(FakeSpreadsheet(worksheets))
    sink._gspread = fake_client  # type: ignore[assignment]
    return sink, fake_client


# --------------------------------------------------------------------------- #
# read_user_edits
# --------------------------------------------------------------------------- #


def test_read_user_edits_returns_empty_when_tab_missing():
    sink, _ = _make_sink({})
    assert sink.read_user_edits("sheet-id") == []


def test_read_user_edits_returns_empty_when_only_header_present():
    ws = FakeWorksheet("UserEdits", rows=[["task_uid", "column", "value", "updated_at"]])
    sink, _ = _make_sink({"UserEdits": ws})
    assert sink.read_user_edits("sheet-id") == []


def test_read_user_edits_parses_rows_and_coerces_booleans():
    ws = FakeWorksheet(
        "UserEdits",
        rows=[
            ["task_uid", "column", "value", "updated_at"],
            ["uid-1", "priority", "High", "2026-04-26T10:00:00Z"],
            ["uid-2", "done", "TRUE", "2026-04-26T11:00:00Z"],
            ["uid-3", "done", "FALSE", "2026-04-26T12:00:00Z"],
            ["", "column", "value", "ts"],  # missing task_uid -> dropped
            ["uid-5", "", "value", "ts"],  # missing column -> dropped
            ["uid-6", "col"],  # too short -> dropped
        ],
    )
    sink, _ = _make_sink({"UserEdits": ws})
    edits = sink.read_user_edits("sheet-id")
    assert len(edits) == 3
    assert edits[0].task_uid == "uid-1"
    assert edits[0].column == "priority"
    assert edits[0].value == "High"
    assert edits[1].value is True
    assert edits[2].value is False


# --------------------------------------------------------------------------- #
# read_duplicate_checkboxes
# --------------------------------------------------------------------------- #


def test_read_duplicate_checkboxes_empty_when_tab_missing():
    sink, _ = _make_sink({})
    assert sink.read_duplicate_checkboxes("sheet-id") == []


def test_read_duplicate_checkboxes_parses_columns_correctly():
    # 9 columns: link_id, ..., confirm at idx 7, dismiss at idx 8
    header = ["link_id", "a", "b", "c", "d", "e", "f", "Confirm", "Dismiss"]
    ws = FakeWorksheet(
        "Possible Duplicates",
        rows=[
            header,
            ["1", "", "", "", "", "", "", "TRUE", "FALSE"],
            ["2", "", "", "", "", "", "", "FALSE", "TRUE"],
            ["3", "", "", "", "", "", "", "FALSE", "FALSE"],
            ["bad", "", "", "", "", "", "", "TRUE", "FALSE"],  # non-int link_id
            ["4", "", "", "", "", "", "", "TRUE"],  # too short -> dropped
        ],
    )
    sink, _ = _make_sink({"Possible Duplicates": ws})
    states = sink.read_duplicate_checkboxes("sheet-id")
    assert [(s.link_id, s.confirm, s.dismiss) for s in states] == [
        (1, True, False),
        (2, False, True),
        (3, False, False),
    ]


# --------------------------------------------------------------------------- #
# write_tab
# --------------------------------------------------------------------------- #


def test_write_tab_clears_data_area_when_rows_empty():
    ws = FakeWorksheet("Tasks", ws_id=1)
    sink, _ = _make_sink({"Tasks": ws})
    sink.write_tab("sheet-id", TASKS_TAB, rows=[])
    last_col = _col_letter(len(TASKS_TAB.columns))
    assert ws.cleared == [[f"A2:{last_col}"]]
    assert ws.updates == []


def test_write_tab_writes_encoded_rows_with_user_entered():
    ws = FakeWorksheet("UserEdits", ws_id=2)
    sink, _ = _make_sink({"UserEdits": ws})
    rows = [
        ("uid-1", "priority", "High", datetime(2026, 4, 26, 10, 0, tzinfo=UTC)),
        ("uid-2", "done", True, None),
    ]
    sink.write_tab("sheet-id", USER_EDITS_TAB, rows)
    last_col = _col_letter(len(USER_EDITS_TAB.columns))
    assert ws.cleared == [[f"A2:{last_col}"]]
    assert len(ws.updates) == 1
    upd = ws.updates[0]
    assert upd["value_input_option"] == "USER_ENTERED"
    assert upd["range_name"] == f"A2:{last_col}3"
    assert upd["values"] == [
        ["uid-1", "priority", "High", "2026-04-26"],
        ["uid-2", "done", True, ""],
    ]


def test_write_tab_raises_when_tab_missing():
    sink, _ = _make_sink({})
    with pytest.raises(GoldSinkError, match="bootstrap-sheet"):
        sink.write_tab("sheet-id", TASKS_TAB, rows=[])


# --------------------------------------------------------------------------- #
# set_tab_hidden
# --------------------------------------------------------------------------- #


def test_set_tab_hidden_issues_batch_update_with_correct_sheet_id():
    ws = FakeWorksheet("UserEdits", ws_id=42)
    sink, _ = _make_sink({"UserEdits": ws})

    discovery = MagicMock()
    sink._discovery = discovery  # type: ignore[assignment]

    sink.set_tab_hidden("sheet-id", USER_EDITS_TAB, hidden=True)

    discovery.spreadsheets.assert_called_once()
    batch = discovery.spreadsheets.return_value.batchUpdate
    batch.assert_called_once()
    kwargs = batch.call_args.kwargs
    assert kwargs["spreadsheetId"] == "sheet-id"
    request = kwargs["body"]["requests"][0]
    assert request["updateSheetProperties"]["properties"] == {
        "sheetId": 42,
        "hidden": True,
    }
    assert request["updateSheetProperties"]["fields"] == "hidden"
    batch.return_value.execute.assert_called_once()


def test_set_tab_hidden_raises_when_tab_missing():
    sink, _ = _make_sink({})
    with pytest.raises(GoldSinkError, match="Cannot hide"):
        sink.set_tab_hidden("sheet-id", USER_EDITS_TAB, hidden=True)

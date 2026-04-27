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

from homework_hub.schema import SETTINGS_TAB, TASKS_TAB, TODAY_TAB, USER_EDITS_TAB
from homework_hub.sinks.gold_sink import (
    GoldSinkError,
    GspreadGoldSink,
    _col_letter,
    _encode_cell,
    _to_cell_value,
    _to_cell_with_format,
)

# --------------------------------------------------------------------------- #
# _encode_cell
# --------------------------------------------------------------------------- #


def test_encode_cell_none_becomes_empty_string():
    assert _encode_cell(None) == ""


def test_encode_cell_utc_datetime_becomes_date_serial():
    dt = datetime(2026, 4, 26, 10, 30, tzinfo=UTC)
    # 2026-04-26 = 46138 days since 30 Dec 1899
    assert _encode_cell(dt) == 46138


def test_encode_cell_non_utc_datetime_is_normalised_to_utc_first():
    # 23:30 in UTC+10 == 13:30 UTC same day → still 2026-04-26
    aedt = timezone(__import__("datetime").timedelta(hours=10))
    dt = datetime(2026, 4, 26, 23, 30, tzinfo=aedt)
    assert _encode_cell(dt) == 46138


def test_encode_cell_naive_datetime_uses_date_directly():
    dt = datetime(2026, 4, 26, 10, 30)
    assert _encode_cell(dt) == 46138


def test_encode_cell_date_object_becomes_serial():
    from datetime import date as _date
    assert _encode_cell(_date(2026, 4, 26)) == 46138
    assert _encode_cell(_date(2026, 5, 1)) == 46143


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
        self.appended: list[list[list[object]]] = []

    @property
    def row_count(self) -> int:
        return len(self._rows)

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

    def append_rows(
        self,
        values: list[list[object]],
        *,
        value_input_option: str = "RAW",
        insert_data_option: str = "INSERT_ROWS",
        table_range: str = "A1",
    ) -> None:
        self.appended.append(values)


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


def _make_sink(
    worksheets: dict[str, FakeWorksheet],
    with_discovery: bool = False,
) -> tuple[GspreadGoldSink, FakeGspreadClient]:
    sink = GspreadGoldSink(credentials=MagicMock())
    fake_client = FakeGspreadClient(FakeSpreadsheet(worksheets))
    sink._gspread = fake_client  # type: ignore[assignment]
    if with_discovery:
        sink._discovery = MagicMock()  # type: ignore[assignment]
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


# --------------------------------------------------------------------------- #
# write_tab — table-backed tabs (Tasks, UserEdits, Possible Duplicates)
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# _to_cell_value
# --------------------------------------------------------------------------- #


def test_to_cell_value_bool():
    assert _to_cell_value(True) == {"userEnteredValue": {"boolValue": True}}
    assert _to_cell_value(False) == {"userEnteredValue": {"boolValue": False}}


def test_to_cell_value_number():
    assert _to_cell_value(42) == {"userEnteredValue": {"numberValue": 42}}
    assert _to_cell_value(3.14) == {"userEnteredValue": {"numberValue": 3.14}}


def test_to_cell_value_formula():
    assert _to_cell_value("=C2-TODAY()") == {"userEnteredValue": {"formulaValue": "=C2-TODAY()"}}


def test_to_cell_value_string():
    assert _to_cell_value("hello") == {"userEnteredValue": {"stringValue": "hello"}}
    assert _to_cell_value("") == {"userEnteredValue": {"stringValue": ""}}
    assert _to_cell_value(None) == {"userEnteredValue": {"stringValue": ""}}


# --------------------------------------------------------------------------- #
# write_tab — table-backed tabs (Tasks, UserEdits, Possible Duplicates)
# --------------------------------------------------------------------------- #


def _single_batch_requests(sink: GspreadGoldSink) -> list[dict]:
    """Return the requests list from the single batchUpdate call."""
    calls = sink._discovery.spreadsheets.return_value.batchUpdate.call_args_list
    assert len(calls) == 1, f"Expected 1 batchUpdate call, got {len(calls)}"
    return calls[0].kwargs["body"]["requests"]


def test_write_table_tab_single_batchupdate_with_all_requests():
    """deleteDimension, updateTable, updateCells go in one batchUpdate.
    updateTable comes before updateCells so structured column references
    in formula cells resolve correctly (cells must be inside the Table)."""
    header = ["subject", "title", "due", "days", "status", "priority", "done", "notes", "source", "link", "task_uid"]
    ws = FakeWorksheet("Tasks", rows=[header, [""]*11], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    rows = [("Maths", "Chapter 3", None, "=C{row}-TODAY()", "Not started", "", False, "", "Classroom", "", "uid-1")]
    sink.write_tab("sheet-id", TASKS_TAB, rows)

    assert ws.cleared == []
    assert ws.updates == []
    assert ws.appended == []

    reqs = _single_batch_requests(sink)
    req_kinds = [list(r.keys())[0] for r in reqs]
    assert req_kinds == ["deleteDimension", "updateTable", "updateCells"]


def test_write_table_tab_delete_covers_all_existing_rows():
    ws = FakeWorksheet("Tasks", rows=[["h"]] + [[""] * 11] * 5, ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, rows=[("Maths", "HW", None, "=C{row}-TODAY()", "Not started", "", False, "", "Classroom", "", "uid-1")])
    reqs = _single_batch_requests(sink)
    del_range = reqs[0]["deleteDimension"]["range"]
    assert del_range["startIndex"] == 1
    assert del_range["endIndex"] == 6  # ws had 6 rows total


def test_write_table_tab_updatecells_uses_correct_value_types():
    """Booleans, formulas, numbers and strings each get the right cell type.
    DATE columns also carry userEnteredFormat so the pattern survives deleteDimension."""
    ws = FakeWorksheet("Tasks", rows=[["h"], [""]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    rows = [("Maths", "HW", 46143, "=C{row}-TODAY()", "Not started", "", False, "", "Classroom", "", "uid-1")]
    sink.write_tab("sheet-id", TASKS_TAB, rows)

    reqs = _single_batch_requests(sink)
    # order: deleteDimension, updateTable, updateCells
    cells = reqs[2]["updateCells"]["rows"][0]["values"]
    # Due (DATE) — carries format alongside value
    assert cells[2] == {
        "userEnteredValue": {"numberValue": 46143},
        "userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd/MM/yyyy"}},
    }
    # Days formula — substituted with row 2
    assert cells[3] == {"userEnteredValue": {"formulaValue": "=C2-TODAY()"}}
    # Done checkbox
    assert cells[6] == {"userEnteredValue": {"boolValue": False}}
    # Subject text — no format
    assert cells[0] == {"userEnteredValue": {"stringValue": "Maths"}}
    # fields mask covers both value and format
    assert reqs[2]["updateCells"]["fields"] == "userEnteredValue,userEnteredFormat.numberFormat"


def test_write_table_tab_updatetable_endrow_covers_data_rows():
    ws = FakeWorksheet("Tasks", rows=[["h"], [""]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    rows = [
        ("Maths", "HW1", None, "=C{row}-TODAY()", "Not started", "", False, "", "Classroom", "", "uid-1"),
        ("English", "Essay", None, "=C{row}-TODAY()", "Not started", "", False, "", "Compass", "", "uid-2"),
        ("Science", "Lab", None, "=C{row}-TODAY()", "Not started", "", False, "", "Compass", "", "uid-3"),
    ]
    sink.write_tab("sheet-id", TASKS_TAB, rows)
    reqs = _single_batch_requests(sink)
    # order: deleteDimension, updateTable, updateCells
    upd = reqs[1]["updateTable"]["table"]
    assert upd["tableId"] == TASKS_TAB.table_id
    assert upd["range"]["endRowIndex"] == 4   # 1 header + 3 data rows
    assert upd["range"]["endColumnIndex"] == len(TASKS_TAB.columns)


def test_write_table_tab_empty_rows_no_updatecells_endrow_1():
    """Empty rows: deleteDimension + updateTable(endRow=1), no updateCells."""
    ws = FakeWorksheet("Tasks", rows=[["h"], [""]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, rows=[])

    reqs = _single_batch_requests(sink)
    req_kinds = [list(r.keys())[0] for r in reqs]
    assert req_kinds == ["deleteDimension", "updateTable"]
    assert reqs[1]["updateTable"]["table"]["range"]["endRowIndex"] == 1


def test_write_table_tab_header_only_no_delete():
    """Header-only sheet: no deleteDimension, just updateTable + updateCells."""
    ws = FakeWorksheet("Tasks", rows=[["h"]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, rows=[("Maths", "HW", None, "=C{row}-TODAY()", "Not started", "", False, "", "Classroom", "", "uid-1")])
    reqs = _single_batch_requests(sink)
    req_kinds = [list(r.keys())[0] for r in reqs]
    assert req_kinds == ["updateTable", "updateCells"]


def test_write_table_tab_header_only_empty_rows_only_updatetable():
    """Header-only sheet + empty rows: only updateTable(endRow=1)."""
    ws = FakeWorksheet("Tasks", rows=[["h"]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, rows=[])
    reqs = _single_batch_requests(sink)
    assert len(reqs) == 1
    assert "updateTable" in reqs[0]
    assert reqs[0]["updateTable"]["table"]["range"]["endRowIndex"] == 1


def test_write_table_tab_encodes_values_correctly():
    """Rows are encoded before being turned into cell value dicts.
    In real usage updated_at is always a str; date/datetime objects encode to serials."""
    ws = FakeWorksheet("UserEdits", rows=[["h"]], ws_id=2)
    sink, _ = _make_sink({"UserEdits": ws}, with_discovery=True)
    rows = [
        ("uid-1", "priority", "High", "2026-04-26T10:00:00+00:00"),
        ("uid-2", "done", True, None),
    ]
    sink.write_tab("sheet-id", USER_EDITS_TAB, rows)
    reqs = _single_batch_requests(sink)
    # order: updateTable, updateCells (no delete — header-only)
    update_rows = reqs[1]["updateCells"]["rows"]
    assert update_rows[0]["values"][2] == {"userEnteredValue": {"stringValue": "High"}}
    assert update_rows[0]["values"][3] == {"userEnteredValue": {"stringValue": "2026-04-26T10:00:00+00:00"}}
    assert update_rows[1]["values"][2] == {"userEnteredValue": {"boolValue": True}}
    assert update_rows[1]["values"][3] == {"userEnteredValue": {"stringValue": ""}}


# --------------------------------------------------------------------------- #
# write_tab — plain tabs (Today, Settings)
# --------------------------------------------------------------------------- #


def test_write_plain_tab_clears_then_updates():
    """Plain tabs use batch_clear + update (no deleteDimension, no append_rows)."""
    ws = FakeWorksheet("Today", ws_id=3)
    sink, _ = _make_sink({"Today": ws})
    rows = [("=TODAY()",), ("=A1+1",)]
    sink.write_tab("sheet-id", TODAY_TAB, rows)
    last_col = _col_letter(len(TODAY_TAB.columns))
    assert ws.cleared == [[f"A2:{last_col}"]]
    assert len(ws.updates) == 1
    assert ws.updates[0]["range_name"] == f"A2:{last_col}3"
    assert ws.appended == []


def test_write_plain_tab_empty_rows_only_clears():
    ws = FakeWorksheet("Today", ws_id=3)
    sink, _ = _make_sink({"Today": ws})
    sink.write_tab("sheet-id", TODAY_TAB, rows=[])
    last_col = _col_letter(len(TODAY_TAB.columns))
    assert ws.cleared == [[f"A2:{last_col}"]]
    assert ws.updates == []
    assert ws.appended == []


def test_write_tab_raises_when_tab_missing():
    sink, _ = _make_sink({}, with_discovery=True)
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

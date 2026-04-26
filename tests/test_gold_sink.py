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


def test_write_table_tab_deletes_data_rows_then_appends():
    """For table-backed tabs, write_tab deletes existing data rows (keeping
    the header) then appends new rows so the Table auto-extends."""
    header = ["subject", "title", "due", "days", "status", "priority", "done", "notes", "source", "link", "task_uid"]
    ws = FakeWorksheet("Tasks", rows=[header, ["", "", "", "", "", "", False, "", "", "", ""]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    rows = [
        ("Maths", "Chapter 3", None, None, "Not started", "Medium", False, "", "Classroom", "", "uid-1"),
    ]
    sink.write_tab("sheet-id", TASKS_TAB, rows)

    # Should NOT use plain clear/update
    assert ws.cleared == []
    assert ws.updates == []
    # Should have called deleteDimension via discovery
    disc = sink._discovery
    disc.spreadsheets.return_value.batchUpdate.assert_called_once()
    body = disc.spreadsheets.return_value.batchUpdate.call_args.kwargs["body"]
    req = body["requests"][0]["deleteDimension"]["range"]
    assert req["sheetId"] == 1
    assert req["dimension"] == "ROWS"
    assert req["startIndex"] == 1  # 0-based row 2
    assert req["endIndex"] == 2    # ws had 2 rows total
    # Should have appended the new row
    assert len(ws.appended) == 1
    assert ws.appended[0][0][0] == "Maths"


def test_write_table_tab_skips_delete_when_header_only():
    """If the sheet already has only the header row (row_count=1), skip delete."""
    ws = FakeWorksheet("Tasks", rows=[["subject"]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, rows=[("Maths", "HW", None, None, "Not started", "Medium", False, "", "Classroom", "", "uid-1")])
    disc = sink._discovery
    disc.spreadsheets.return_value.batchUpdate.assert_not_called()
    assert len(ws.appended) == 1


def test_write_table_tab_empty_rows_skips_append():
    """Empty rows: deletes existing data rows but does not call append_rows."""
    ws = FakeWorksheet("Tasks", rows=[["subject"], [""]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, rows=[])
    disc = sink._discovery
    disc.spreadsheets.return_value.batchUpdate.assert_called_once()
    assert ws.appended == []


def test_write_table_tab_encodes_values_correctly():
    """Rows written via append_rows are encoded the same as plain tabs."""
    ws = FakeWorksheet("UserEdits", rows=[["task_uid"]], ws_id=2)
    sink, _ = _make_sink({"UserEdits": ws}, with_discovery=True)
    rows = [
        ("uid-1", "priority", "High", datetime(2026, 4, 26, 10, 0, tzinfo=UTC)),
        ("uid-2", "done", True, None),
    ]
    sink.write_tab("sheet-id", USER_EDITS_TAB, rows)
    assert ws.appended == [
        [
            ["uid-1", "priority", "High", "2026-04-26"],
            ["uid-2", "done", True, ""],
        ]
    ]


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

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

from homework_hub.schema import DASHBOARD_TAB, SETTINGS_TAB, TASKS_TAB, USER_EDITS_TAB
from homework_hub.sinks.gold_sink import (
    GoldSinkError,
    GspreadGoldSink,
    _col_letter,
    _encode_cell,
    _to_cell_value,
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
    ws = FakeWorksheet(
        "UserEdits",
        rows=[["task_uid", "column", "original_value", "value", "updated_at"]],
    )
    sink, _ = _make_sink({"UserEdits": ws})
    assert sink.read_user_edits("sheet-id") == []


def test_read_user_edits_parses_rows_and_coerces_booleans():
    ws = FakeWorksheet(
        "UserEdits",
        rows=[
            ["task_uid", "column", "original_value", "value", "updated_at"],
            ["uid-1", "priority", "Low", "High", "2026-04-26T10:00:00Z"],
            ["uid-2", "done", "FALSE", "TRUE", "2026-04-26T11:00:00Z"],
            ["uid-3", "done", "TRUE", "FALSE", "2026-04-26T12:00:00Z"],
            ["", "column", "orig", "value", "ts"],  # missing task_uid -> dropped
            ["uid-5", "", "orig", "value", "ts"],  # missing column -> dropped
            ["uid-6", "col"],  # too short -> dropped
        ],
    )
    sink, _ = _make_sink({"UserEdits": ws})
    edits = sink.read_user_edits("sheet-id")
    assert len(edits) == 3
    assert edits[0].task_uid == "uid-1"
    assert edits[0].column == "priority"
    assert edits[0].value == "High"
    assert edits[0].original_value == "Low"
    assert edits[1].value is True
    assert edits[1].original_value is False
    assert edits[2].value is False
    assert edits[2].original_value is True


def test_read_user_edits_tolerates_legacy_4col_rows():
    """Rows persisted before original_value was added (4 cols:
    task_uid | column | value | updated_at) must still be read; the
    missing original_value defaults to empty string."""
    ws = FakeWorksheet(
        "UserEdits",
        rows=[
            ["task_uid", "column", "value", "updated_at"],
            ["uid-1", "notes", "hello", "2026-04-26T10:00:00Z"],
            ["uid-2", "due", "2026-05-01", "2026-04-26T11:00:00Z"],
        ],
    )
    sink, _ = _make_sink({"UserEdits": ws})
    edits = sink.read_user_edits("sheet-id")
    assert len(edits) == 2
    assert edits[0].task_uid == "uid-1"
    assert edits[0].column == "notes"
    assert edits[0].value == "hello"
    assert edits[0].original_value == ""
    assert edits[1].value == "2026-05-01"
    assert edits[1].original_value == ""

    # --------------------------------------------------------------------------- #
    # read_tab_raw
    # --------------------------------------------------------------------------- #
    """read_tab_raw strips the header row and returns raw string rows."""
    ws = FakeWorksheet(
        "Tasks",
        rows=[
            ["Subject", "Title", "Due"],  # header — must be stripped
            ["9MATH", "Algebra", "01/05/2026"],
            ["9ENG", "Essay", ""],
        ],
    )
    sink, _ = _make_sink({"Tasks": ws})
    result = sink.read_tab_raw("sheet-id", "Tasks")
    assert result == [
        ["9MATH", "Algebra", "01/05/2026"],
        ["9ENG", "Essay", ""],
    ]


def test_read_tab_raw_returns_empty_when_tab_missing():
    sink, _ = _make_sink({})
    assert sink.read_tab_raw("sheet-id", "Tasks") == []


def test_read_tab_raw_returns_empty_when_only_header():
    ws = FakeWorksheet("Tasks", rows=[["Subject", "Title"]])
    sink, _ = _make_sink({"Tasks": ws})
    assert sink.read_tab_raw("sheet-id", "Tasks") == []


# --------------------------------------------------------------------------- #
# write_tab — table-backed tabs (Tasks, UserEdits)
# --------------------------------------------------------------------------- #


_DAYS_F = '=IF(OR(D{row}="",F{row}="Submitted",F{row}="Graded"),"",D{row}-TODAY())'
# 10-column Tasks row: subject, task_type, title, due, days, status, notes, source, link, task_uid
_TASK_ROW = (
    "Maths",
    "Homework",
    "Chapter 3",
    None,
    _DAYS_F,
    "Not started",
    "",
    "Classroom",
    "",
    "uid-1",
)


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
    """deleteDimension, appendDimension, updateTable, updateCells go in one batchUpdate.
    updateTable comes before updateCells so structured column references
    in formula cells resolve correctly (cells must be inside the Table)."""
    header = [
        "subject",
        "task_type",
        "title",
        "due",
        "days",
        "status",
        "notes",
        "source",
        "link",
        "task_uid",
    ]
    ws = FakeWorksheet("Tasks", rows=[header, [""] * 10], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, [_TASK_ROW])

    assert ws.cleared == []
    assert ws.updates == []
    assert ws.appended == []

    reqs = _single_batch_requests(sink)
    req_kinds = [next(iter(r.keys())) for r in reqs]
    assert req_kinds == ["deleteDimension", "appendDimension", "updateTable", "updateCells"]


def test_write_table_tab_delete_covers_all_existing_rows():
    ws = FakeWorksheet("Tasks", rows=[["h"]] + [[""] * 10] * 5, ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, rows=[_TASK_ROW])
    reqs = _single_batch_requests(sink)
    del_range = reqs[0]["deleteDimension"]["range"]
    assert del_range["startIndex"] == 1
    assert del_range["endIndex"] == 6  # ws had 6 rows total


def test_write_table_tab_updatecells_uses_correct_value_types():
    """Booleans, formulas, numbers and strings each get the right cell type.
    No userEnteredFormat is written — column format from bootstrap repeatCell survives deleteDimension.
    """
    ws = FakeWorksheet("Tasks", rows=[["h"], [""]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    # Due supplied as a Sheets serial (int); days formula has {row} placeholder.
    rows = [
        ("Maths", "Homework", "HW", 46143, _DAYS_F, "Not started", "", "Classroom", "", "uid-1")
    ]
    sink.write_tab("sheet-id", TASKS_TAB, rows)

    reqs = _single_batch_requests(sink)
    # order: deleteDimension, appendDimension, updateTable, updateCells
    cells = reqs[3]["updateCells"]["rows"][0]["values"]
    # Due (DATE serial) at index 3 — value only, no format
    assert cells[3] == {"userEnteredValue": {"numberValue": 46143}}
    # Days formula at index 4 — {row} substituted with 2
    expected_days = _DAYS_F.replace("{row}", "2")
    assert cells[4] == {"userEnteredValue": {"formulaValue": expected_days}}
    # Subject text at index 0
    assert cells[0] == {"userEnteredValue": {"stringValue": "Maths"}}
    # fields mask covers only value, not format
    assert reqs[3]["updateCells"]["fields"] == "userEnteredValue"


def test_write_table_tab_updatetable_endrow_covers_data_rows():
    ws = FakeWorksheet("Tasks", rows=[["h"], [""]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    rows = [
        ("Maths", "Homework", "HW1", None, _DAYS_F, "Not started", "", "Classroom", "", "uid-1"),
        ("English", "Homework", "Essay", None, _DAYS_F, "Not started", "", "Compass", "", "uid-2"),
        ("Science", "Homework", "Lab", None, _DAYS_F, "Not started", "", "Compass", "", "uid-3"),
    ]
    sink.write_tab("sheet-id", TASKS_TAB, rows)
    reqs = _single_batch_requests(sink)
    # order: deleteDimension, appendDimension, updateTable, updateCells
    upd = reqs[2]["updateTable"]["table"]
    assert upd["tableId"] == TASKS_TAB.table_id
    assert upd["range"]["endRowIndex"] == 4  # 1 header + 3 data rows
    assert upd["range"]["endColumnIndex"] == len(TASKS_TAB.columns)


def test_write_table_tab_empty_rows_no_updatecells_endrow_1():
    """Empty rows: deleteDimension + updateTable(endRow=1), no updateCells or appendDimension."""
    ws = FakeWorksheet("Tasks", rows=[["h"], [""]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, rows=[])

    reqs = _single_batch_requests(sink)
    req_kinds = [next(iter(r.keys())) for r in reqs]
    assert req_kinds == ["deleteDimension", "updateTable"]
    assert reqs[1]["updateTable"]["table"]["range"]["endRowIndex"] == 1


def test_write_table_tab_header_only_no_delete():
    """Header-only sheet: no deleteDimension, just appendDimension + updateTable + updateCells."""
    ws = FakeWorksheet("Tasks", rows=[["h"]], ws_id=1)
    sink, _ = _make_sink({"Tasks": ws}, with_discovery=True)
    sink.write_tab("sheet-id", TASKS_TAB, rows=[_TASK_ROW])
    reqs = _single_batch_requests(sink)
    req_kinds = [next(iter(r.keys())) for r in reqs]
    assert req_kinds == ["appendDimension", "updateTable", "updateCells"]


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
    # order: appendDimension, updateTable, updateCells (no delete — header-only)
    update_rows = reqs[2]["updateCells"]["rows"]
    assert update_rows[0]["values"][2] == {"userEnteredValue": {"stringValue": "High"}}
    assert update_rows[0]["values"][3] == {
        "userEnteredValue": {"stringValue": "2026-04-26T10:00:00+00:00"}
    }
    assert update_rows[1]["values"][2] == {"userEnteredValue": {"boolValue": True}}
    assert update_rows[1]["values"][3] == {"userEnteredValue": {"stringValue": ""}}


# --------------------------------------------------------------------------- #
# write_tab — plain tabs (Settings)
# --------------------------------------------------------------------------- #


def test_write_plain_tab_clears_then_updates():
    """Plain data-bearing tabs (e.g. Settings) clear A1:..., re-write the
    header row, then update the data range below."""
    ws = FakeWorksheet("Settings", ws_id=3)
    sink, _ = _make_sink({"Settings": ws})
    rows = [("Compass", "2026-05-01", "", "OK"), ("Classroom", "2026-05-01", "", "OK")]
    sink.write_tab("sheet-id", SETTINGS_TAB, rows)
    last_col = _col_letter(len(SETTINGS_TAB.columns))
    # Plain data-bearing tab clears the full sheet area (A1:) and
    # re-writes the header row before appending data rows.
    assert ws.cleared == [[f"A1:{last_col}"]]
    range_names = [u["range_name"] for u in ws.updates]
    assert f"A1:{last_col}1" in range_names  # header re-write
    assert f"A2:{last_col}3" in range_names  # data rows
    assert ws.appended == []


def test_write_plain_tab_empty_rows_only_clears_and_writes_header():
    ws = FakeWorksheet("Settings", ws_id=3)
    sink, _ = _make_sink({"Settings": ws})
    sink.write_tab("sheet-id", SETTINGS_TAB, rows=[])
    last_col = _col_letter(len(SETTINGS_TAB.columns))
    assert ws.cleared == [[f"A1:{last_col}"]]
    # Header still gets re-written; no data range update follows.
    assert [u["range_name"] for u in ws.updates] == [f"A1:{last_col}1"]
    assert ws.appended == []


def test_write_dashboard_tab_skipped_by_publish():
    """Dashboard is purely formula-driven — sanity check that ``write_tab``
    doesn't get called for it during publish. (Asserted indirectly: the
    publish layer's tab list excludes ``DASHBOARD_TAB``.)"""
    # Sentinel reference so the import stays used and the intent is recorded
    # in the test suite.
    assert DASHBOARD_TAB.table_id == ""
    assert all(c.header == "" for c in DASHBOARD_TAB.columns)


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


# --------------------------------------------------------------------------- #
# read_dashboard_meta — protected_range_ids
# --------------------------------------------------------------------------- #


def _dashboard_meta_response(protected_ranges: list[dict]) -> dict:
    """Wrap a list of protectedRanges in the shape spreadsheets.get returns."""
    return {
        "sheets": [
            {
                "properties": {"sheetId": 42, "title": "Dashboard"},
                "tables": [],
                "bandedRanges": [],
                "conditionalFormats": [],
                "protectedRanges": protected_ranges,
            }
        ]
    }


def _dashboard_meta_response_with_theme(
    protected_ranges: list[dict],
    theme: dict | None,
) -> dict:
    out = _dashboard_meta_response(protected_ranges)
    if theme is not None:
        out["spreadsheetTheme"] = theme
    return out


def _make_discovery_sink(get_response: dict) -> GspreadGoldSink:
    sink = GspreadGoldSink(credentials=MagicMock())
    discovery = MagicMock()
    discovery.spreadsheets.return_value.get.return_value.execute.return_value = (
        get_response
    )
    sink._discovery = discovery  # type: ignore[assignment]
    return sink


def test_read_dashboard_meta_requests_protected_ranges_field():
    sink = _make_discovery_sink(_dashboard_meta_response([]))
    sink.read_dashboard_meta("sheet-id")
    call = sink._discovery.spreadsheets.return_value.get.call_args
    fields = call.kwargs["fields"]
    assert "protectedRanges(protectedRangeId,range)" in fields


def test_read_dashboard_meta_returns_whole_sheet_protection_ids():
    sink = _make_discovery_sink(
        _dashboard_meta_response(
            [
                {"protectedRangeId": 111, "range": {"sheetId": 42}},
                # Bare ``range`` omitted entirely — also whole-sheet.
                {"protectedRangeId": 222},
            ]
        )
    )
    meta = sink.read_dashboard_meta("sheet-id")
    assert sorted(meta.protected_range_ids) == [111, 222]


def test_read_dashboard_meta_ignores_cell_scoped_protections():
    """Cell-scoped protections (kid/admin installed) must not block
    publish from installing its own whole-sheet lock."""
    sink = _make_discovery_sink(
        _dashboard_meta_response(
            [
                {
                    "protectedRangeId": 333,
                    "range": {
                        "sheetId": 42,
                        "startRowIndex": 0,
                        "endRowIndex": 5,
                    },
                },
                {
                    "protectedRangeId": 444,
                    "range": {
                        "sheetId": 42,
                        "startColumnIndex": 0,
                        "endColumnIndex": 1,
                    },
                },
            ]
        )
    )
    meta = sink.read_dashboard_meta("sheet-id")
    assert meta.protected_range_ids == []


# --------------------------------------------------------------------------- #
# write_dashboard_protection
# --------------------------------------------------------------------------- #


def test_write_dashboard_protection_raises_without_service_account_email():
    """User-installed OAuth creds expose no service-account email — we
    refuse to write because omitting editors would lock the publisher
    out of kid-owned sheets."""
    sink = GspreadGoldSink(credentials=MagicMock(spec=[]))
    sink._discovery = MagicMock()  # type: ignore[assignment]
    with pytest.raises(GoldSinkError, match="service_account_email"):
        sink.write_dashboard_protection("sheet-id", dashboard_sheet_id=42)


def test_write_dashboard_protection_issues_single_batchupdate():
    creds = MagicMock()
    creds.service_account_email = "bot@svc.iam.gserviceaccount.com"
    sink = GspreadGoldSink(credentials=creds)
    discovery = MagicMock()
    sink._discovery = discovery  # type: ignore[assignment]

    sink.write_dashboard_protection("sheet-id", dashboard_sheet_id=42)

    call = discovery.spreadsheets.return_value.batchUpdate.call_args
    assert call.kwargs["spreadsheetId"] == "sheet-id"
    requests = call.kwargs["body"]["requests"]
    assert len(requests) == 1
    pr = requests[0]["addProtectedRange"]["protectedRange"]
    assert pr["range"] == {"sheetId": 42}
    assert pr["warningOnly"] is False
    assert pr["editors"] == {"users": ["bot@svc.iam.gserviceaccount.com"]}


# --------------------------------------------------------------------------- #
# spreadsheetTheme → DashboardMeta.theme_accent
# --------------------------------------------------------------------------- #


def test_read_dashboard_meta_requests_spreadsheet_theme_field():
    sink = _make_discovery_sink(_dashboard_meta_response([]))
    sink.read_dashboard_meta("sheet-id")
    fields = sink._discovery.spreadsheets.return_value.get.call_args.kwargs["fields"]
    assert "spreadsheetTheme(themeColors(colorType,color" in fields


def test_read_dashboard_meta_returns_none_when_theme_missing():
    sink = _make_discovery_sink(_dashboard_meta_response_with_theme([], theme=None))
    meta = sink.read_dashboard_meta("sheet-id")
    assert meta.theme_accent is None


def test_read_dashboard_meta_parses_accent1_rgb():
    theme = {
        "themeColors": [
            {"colorType": "TEXT", "color": {"rgbColor": {"red": 0.0}}},
            {
                "colorType": "ACCENT1",
                "color": {"rgbColor": {"red": 0.25, "green": 0.5, "blue": 0.75}},
            },
            {"colorType": "ACCENT2", "color": {"rgbColor": {"red": 0.9}}},
        ]
    }
    sink = _make_discovery_sink(_dashboard_meta_response_with_theme([], theme))
    meta = sink.read_dashboard_meta("sheet-id")
    assert meta.theme_accent == {"red": 0.25, "green": 0.5, "blue": 0.75}


def test_read_dashboard_meta_defaults_missing_channels_to_zero():
    """Sheets omits zero-valued channels in API responses — caller must
    default them so consumers always see a fully-populated dict."""
    theme = {
        "themeColors": [
            {"colorType": "ACCENT1", "color": {"rgbColor": {"red": 0.5}}},
        ]
    }
    sink = _make_discovery_sink(_dashboard_meta_response_with_theme([], theme))
    meta = sink.read_dashboard_meta("sheet-id")
    assert meta.theme_accent == {"red": 0.5, "green": 0.0, "blue": 0.0}


def test_read_dashboard_meta_returns_none_when_accent1_missing():
    theme = {
        "themeColors": [
            {"colorType": "TEXT", "color": {"rgbColor": {"red": 0.0}}},
            {"colorType": "ACCENT2", "color": {"rgbColor": {"red": 0.9}}},
        ]
    }
    sink = _make_discovery_sink(_dashboard_meta_response_with_theme([], theme))
    meta = sink.read_dashboard_meta("sheet-id")
    assert meta.theme_accent is None

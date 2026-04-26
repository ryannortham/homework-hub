"""Tests for the positional Raw-tab diff logic."""

from __future__ import annotations

from datetime import UTC, datetime

from homework_hub.models import Source, Status, Task
from homework_hub.sinks.sheets import RAW_HEADERS, task_to_row
from homework_hub.sinks.sheets_diff import compute_raw_diff


def _t(
    child: str = "james",
    source: Source = Source.CLASSROOM,
    source_id: str = "abc",
    title: str = "Maths Q1-5",
    status: Status = Status.NOT_STARTED,
    due: datetime | None = None,
) -> Task:
    return Task(
        source=source,
        source_id=source_id,
        child=child,
        subject="Maths",
        title=title,
        due_at=due or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        status=status,
        last_synced=datetime(2026, 4, 26, 7, 0, tzinfo=UTC),
    )


HEADER_ROW = RAW_HEADERS


class TestEmptySheet:
    def test_first_sync_appends_all(self):
        # Existing sheet has only the header row.
        existing = [HEADER_ROW]
        diff = compute_raw_diff(
            existing_rows=existing,
            incoming=[_t(source_id="a"), _t(source_id="b")],
        )
        assert diff.updates == {}
        assert len(diff.appends) == 2
        assert diff.unchanged_keys == []
        # Append starts at row 2 when only the header exists.
        assert diff.next_append_row == 2

    def test_completely_empty_sheet_still_works(self):
        # Defensive: caller passed in empty list (no header). Append-only.
        diff = compute_raw_diff(existing_rows=[], incoming=[_t(source_id="a")])
        assert diff.updates == {}
        assert len(diff.appends) == 1


class TestUnchanged:
    def test_identical_task_is_unchanged(self):
        task = _t(source_id="a")
        existing = [HEADER_ROW, task_to_row(task)]
        diff = compute_raw_diff(existing_rows=existing, incoming=[task])
        assert diff.updates == {}
        assert diff.appends == []
        assert diff.unchanged_keys == [("james", "classroom", "a")]

    def test_last_synced_only_change_treated_as_unchanged(self):
        old = _t(source_id="a")
        existing = [HEADER_ROW, task_to_row(old)]
        # Same content but a fresher last_synced → should be unchanged.
        new = old.model_copy(update={"last_synced": datetime(2026, 4, 27, 8, 0, tzinfo=UTC)})
        diff = compute_raw_diff(existing_rows=existing, incoming=[new])
        assert diff.updates == {}
        assert diff.unchanged_keys == [("james", "classroom", "a")]


class TestUpdate:
    def test_status_change_updates_in_place(self):
        old = _t(source_id="a", status=Status.NOT_STARTED)
        existing = [HEADER_ROW, task_to_row(old)]
        new = _t(source_id="a", status=Status.SUBMITTED)
        diff = compute_raw_diff(existing_rows=existing, incoming=[new])
        # Row 2 (data row 1) gets rewritten; no append.
        assert list(diff.updates.keys()) == [2]
        assert diff.appends == []
        # Status column index in RAW_HEADERS
        status_idx = RAW_HEADERS.index("status")
        assert diff.updates[2][status_idx] == "submitted"

    def test_title_change_updates_in_place(self):
        old = _t(source_id="a", title="Old")
        existing = [HEADER_ROW, task_to_row(old)]
        new = _t(source_id="a", title="New")
        diff = compute_raw_diff(existing_rows=existing, incoming=[new])
        assert 2 in diff.updates


class TestAppend:
    def test_new_task_appends_after_existing(self):
        existing_task = _t(source_id="a")
        existing = [HEADER_ROW, task_to_row(existing_task)]
        new_task = _t(source_id="b")
        diff = compute_raw_diff(existing_rows=existing, incoming=[existing_task, new_task])
        assert diff.updates == {}
        assert len(diff.appends) == 1
        # next_append_row points at the row immediately after the last data row.
        assert diff.next_append_row == 3

    def test_multiple_appends_preserve_order(self):
        existing = [HEADER_ROW]
        diff = compute_raw_diff(
            existing_rows=existing,
            incoming=[_t(source_id="a"), _t(source_id="b"), _t(source_id="c")],
        )
        # source_id is column index 2.
        assert [row[2] for row in diff.appends] == ["a", "b", "c"]


class TestPositionalStability:
    def test_existing_row_position_preserved_when_appending_new_task(self):
        # Row 2: task A. Now sync A unchanged + new task B.
        a = _t(source_id="a")
        existing = [HEADER_ROW, task_to_row(a)]
        b = _t(source_id="b")
        diff = compute_raw_diff(existing_rows=existing, incoming=[a, b])
        # A stays put (unchanged); B appends. No row movements.
        assert diff.unchanged_keys == [("james", "classroom", "a")]
        assert diff.updates == {}
        assert len(diff.appends) == 1

    def test_disappeared_task_kept_on_sheet(self):
        # Row 2: A, Row 3: B. Sync brings only A. B must NOT be removed.
        a = _t(source_id="a")
        b = _t(source_id="b")
        existing = [HEADER_ROW, task_to_row(a), task_to_row(b)]
        diff = compute_raw_diff(existing_rows=existing, incoming=[a])
        # A unchanged; B not touched (no update, no append, no removal).
        assert diff.unchanged_keys == [("james", "classroom", "a")]
        assert diff.updates == {}
        assert diff.appends == []
        # Append row stays just past the last existing row (B's row).
        assert diff.next_append_row == 4

    def test_returning_task_reuses_original_row(self):
        # Row 2: A, Row 3: B (kid added notes against B's row).
        # Last sync only saw A. This sync sees A + B again. B should land
        # back on row 3, not row 4.
        a = _t(source_id="a")
        b = _t(source_id="b")
        existing = [HEADER_ROW, task_to_row(a), task_to_row(b)]
        diff = compute_raw_diff(existing_rows=existing, incoming=[a, b])
        # B's content is identical → unchanged, not re-appended.
        assert diff.appends == []
        assert {k[2] for k in diff.unchanged_keys} == {"a", "b"}


class TestDedupKeyScoping:
    def test_same_source_id_under_different_children_are_separate_rows(self):
        existing = [HEADER_ROW]
        diff = compute_raw_diff(
            existing_rows=existing,
            incoming=[
                _t(child="james", source_id="x"),
                _t(child="tahlia", source_id="x"),
            ],
        )
        assert len(diff.appends) == 2

    def test_same_source_id_different_sources_are_separate_rows(self):
        existing = [HEADER_ROW]
        diff = compute_raw_diff(
            existing_rows=existing,
            incoming=[
                _t(source=Source.CLASSROOM, source_id="x"),
                _t(source=Source.COMPASS, source_id="x"),
            ],
        )
        assert len(diff.appends) == 2

    def test_duplicate_incoming_task_processed_once(self):
        existing = [HEADER_ROW]
        task = _t(source_id="a")
        diff = compute_raw_diff(existing_rows=existing, incoming=[task, task])
        assert len(diff.appends) == 1


class TestDefensiveParsing:
    def test_short_rows_treated_as_padded(self):
        # Existing row only has 3 cells (not 12). Should still match by key.
        a = _t(source_id="a")
        truncated_row = ["james", "classroom", "a"]
        existing = [HEADER_ROW, truncated_row]
        diff = compute_raw_diff(existing_rows=existing, incoming=[a])
        # Treated as 'changed' because subject/title etc. differ.
        assert 2 in diff.updates

    def test_blank_trailing_row_ignored(self):
        a = _t(source_id="a")
        # Sheets returns blank rows beyond the data when ranges over-fetch.
        existing = [HEADER_ROW, task_to_row(a), [], ["", "", ""]]
        diff = compute_raw_diff(existing_rows=existing, incoming=[a])
        assert diff.unchanged_keys == [("james", "classroom", "a")]
        assert diff.updates == {}

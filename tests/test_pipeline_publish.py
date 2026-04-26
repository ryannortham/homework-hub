"""Tests for the Gold publish layer (M5)."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import UTC, date, datetime
from pathlib import Path

from homework_hub.models import Source, Status, Task
from homework_hub.pipeline.publish import (
    DuplicateCheckboxState,
    LinkProjectionInput,
    UserEdit,
    apply_link_state_writebacks,
    diff_user_edits,
    load_links_for_publish,
    melbourne_local_date,
    merge_user_edits,
    project_duplicates_rows,
    project_settings_rows,
    project_tasks_rows,
    publish_for_child,
    reconcile_link_state,
    task_uid,
)
from homework_hub.schema import DUPLICATES_TAB, TASKS_TAB
from homework_hub.state.store import StateStore

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _task(
    *,
    source: Source = Source.COMPASS,
    source_id: str = "T1",
    child: str = "james",
    subject: str = "Year 9 Science",
    title: str = "Photosynthesis Worksheet",
    due_at: datetime | None = None,
    status: Status = Status.NOT_STARTED,
    url: str = "https://example/test",
) -> Task:
    return Task(
        source=source,
        source_id=source_id,
        child=child,
        subject=subject,
        title=title,
        due_at=due_at,
        status=status,
        url=url,
    )


def _idx(key: str) -> int:
    return TASKS_TAB.column_index(key)


# --------------------------------------------------------------------------- #
# melbourne_local_date
# --------------------------------------------------------------------------- #


class TestMelbourneLocalDate:
    def test_none_in_none_out(self):
        assert melbourne_local_date(None) is None

    def test_utc_to_melbourne_crosses_date(self):
        # 14:00 UTC on Jan 1 = 01:00 next day in Melbourne (AEDT, UTC+11).
        utc = datetime(2026, 1, 1, 14, 0, tzinfo=UTC)
        assert melbourne_local_date(utc) == date(2026, 1, 2)

    def test_dst_aware_winter(self):
        # July: AEST = UTC+10. 23:00 UTC Jul 1 → 09:00 Jul 2 Melbourne.
        utc = datetime(2026, 7, 1, 23, 0, tzinfo=UTC)
        assert melbourne_local_date(utc) == date(2026, 7, 2)

    def test_naive_assumed_utc(self):
        naive = datetime(2026, 1, 1, 14, 0)
        assert melbourne_local_date(naive) == date(2026, 1, 2)


# --------------------------------------------------------------------------- #
# Tasks projection
# --------------------------------------------------------------------------- #


class TestProjectTasksRows:
    def test_basic_row_shape(self):
        rows = project_tasks_rows([_task(due_at=datetime(2026, 5, 1, 14, 0, tzinfo=UTC))])
        assert len(rows) == 1
        cells = rows[0].cells
        assert cells[_idx("subject")] == "Year 9 Science"
        assert cells[_idx("title")] == "Photosynthesis Worksheet"
        # Due converted to Melbourne local date (May 2).
        assert cells[_idx("due")] == date(2026, 5, 2)
        # Days column left blank for Sheets formula.
        assert cells[_idx("days")] == ""
        assert cells[_idx("status")] == "Not started"
        assert cells[_idx("priority")] == ""
        assert cells[_idx("done")] is False
        assert cells[_idx("notes")] == ""
        assert cells[_idx("source")] == "Compass"
        assert cells[_idx("link")] == "https://example/test"
        assert cells[_idx("task_uid")] == "compass:T1"

    def test_classroom_source_label(self):
        rows = project_tasks_rows([_task(source=Source.CLASSROOM, source_id="K1")])
        assert rows[0].cells[_idx("source")] == "Classroom"
        assert rows[0].task_uid == "classroom:K1"

    def test_edrolo_source_label(self):
        rows = project_tasks_rows([_task(source=Source.EDROLO, source_id="E1")])
        assert rows[0].cells[_idx("source")] == "Edrolo"

    def test_overdue_status_label(self):
        rows = project_tasks_rows([_task(status=Status.OVERDUE)])
        assert rows[0].cells[_idx("status")] == "Overdue"

    def test_no_due_date(self):
        rows = project_tasks_rows([_task(due_at=None)])
        assert rows[0].cells[_idx("due")] is None


# --------------------------------------------------------------------------- #
# Duplicates projection
# --------------------------------------------------------------------------- #


class TestProjectDuplicatesRows:
    def _link(
        self,
        *,
        state: str = "pending",
        confidence: str = "auto_high",
        link_id: int = 1,
    ) -> LinkProjectionInput:
        return LinkProjectionInput(
            link_id=link_id,
            confidence=confidence,
            state=state,
            subject="Year 9 Humanities",
            compass_title="WW1 Benchmark",
            compass_due=datetime(2026, 5, 1, tzinfo=UTC),
            classroom_title="WW1",
            classroom_due=datetime(2026, 5, 1, tzinfo=UTC),
        )

    def test_pending_row_emitted(self):
        rows = project_duplicates_rows([self._link()])
        assert len(rows) == 1
        cells = rows[0].cells
        idx = lambda k: DUPLICATES_TAB.column_index(k)  # noqa: E731
        assert cells[idx("link_id")] == "1"
        assert cells[idx("confidence")] == "High"
        assert cells[idx("subject")] == "Year 9 Humanities"
        assert cells[idx("compass_title")] == "WW1 Benchmark"
        assert cells[idx("classroom_title")] == "WW1"
        assert cells[idx("confirm")] is False
        assert cells[idx("dismiss")] is False

    def test_medium_label(self):
        rows = project_duplicates_rows([self._link(confidence="auto_medium")])
        assert rows[0].cells[DUPLICATES_TAB.column_index("confidence")] == "Medium"

    def test_confirmed_link_dropped(self):
        assert project_duplicates_rows([self._link(state="confirmed")]) == []

    def test_dismissed_link_dropped(self):
        assert project_duplicates_rows([self._link(state="dismissed")]) == []


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #


class TestProjectSettingsRows:
    def test_basic(self):
        rows = project_settings_rows(
            child="james",
            last_synced=datetime(2026, 5, 1, 14, 0, tzinfo=UTC),
        )
        keys = [k for k, _ in rows]
        assert "Child" in keys
        assert "Last synced (UTC)" in keys
        assert "Last synced (Melbourne date)" in keys
        # Tabs managed should not include the hidden UserEdits tab.
        tabs_value = dict(rows)["Tabs managed"]
        assert "UserEdits" not in tabs_value
        assert "Today" in tabs_value
        assert "Tasks" in tabs_value

    def test_no_last_synced(self):
        rows = project_settings_rows(child="james", last_synced=None)
        d = dict(rows)
        assert d["Last synced (UTC)"] == "—"
        assert d["Last synced (Melbourne date)"] == "—"


# --------------------------------------------------------------------------- #
# UserEdits merge
# --------------------------------------------------------------------------- #


class TestMergeUserEdits:
    def test_no_edits_passthrough(self):
        rows = project_tasks_rows([_task()])
        merged = merge_user_edits(rows, [])
        assert merged == rows

    def test_priority_override_applied(self):
        rows = project_tasks_rows([_task()])
        edits = [UserEdit("compass:T1", "priority", "High", "now")]
        merged = merge_user_edits(rows, edits)
        assert merged[0].cells[_idx("priority")] == "High"
        # Other cells untouched.
        assert merged[0].cells[_idx("title")] == "Photosynthesis Worksheet"

    def test_done_checkbox_override(self):
        rows = project_tasks_rows([_task()])
        edits = [UserEdit("compass:T1", "done", True, "now")]
        merged = merge_user_edits(rows, edits)
        assert merged[0].cells[_idx("done")] is True

    def test_non_editable_column_ignored(self):
        # Status is read-only — an edit referencing it must be dropped.
        rows = project_tasks_rows([_task()])
        edits = [UserEdit("compass:T1", "status", "Submitted", "now")]
        merged = merge_user_edits(rows, edits)
        assert merged[0].cells[_idx("status")] == "Not started"

    def test_orphan_edit_dropped(self):
        # Edit for a task_uid not in silver — silently discarded.
        rows = project_tasks_rows([_task()])
        edits = [UserEdit("compass:GHOST", "priority", "High", "now")]
        merged = merge_user_edits(rows, edits)
        assert merged[0].cells[_idx("priority")] == ""


class TestDiffUserEdits:
    def test_default_values_not_emitted(self):
        rows = project_tasks_rows([_task()])
        edits = diff_user_edits(rows, existing=[])
        assert edits == []

    def test_overridden_priority_emitted(self):
        rows = merge_user_edits(
            project_tasks_rows([_task()]),
            [UserEdit("compass:T1", "priority", "High", "old")],
        )
        out = diff_user_edits(rows, existing=[])
        assert len(out) == 1
        assert out[0].column == "priority"
        assert out[0].value == "High"

    def test_unchanged_value_keeps_old_timestamp(self):
        rows = merge_user_edits(
            project_tasks_rows([_task()]),
            [UserEdit("compass:T1", "priority", "High", "OLD-TS")],
        )
        out = diff_user_edits(
            rows,
            existing=[UserEdit("compass:T1", "priority", "High", "OLD-TS")],
        )
        assert out[0].updated_at == "OLD-TS"

    def test_changed_value_gets_new_timestamp(self):
        rows = merge_user_edits(
            project_tasks_rows([_task()]),
            [UserEdit("compass:T1", "priority", "High", "now")],
        )
        out = diff_user_edits(
            rows,
            existing=[UserEdit("compass:T1", "priority", "Low", "OLD-TS")],
        )
        assert out[0].value == "High"
        assert out[0].updated_at != "OLD-TS"


# --------------------------------------------------------------------------- #
# Checkbox readback
# --------------------------------------------------------------------------- #


class TestReconcileLinkState:
    def test_neither_returns_none(self):
        assert reconcile_link_state(DuplicateCheckboxState(1, confirm=False, dismiss=False)) is None

    def test_confirm(self):
        assert (
            reconcile_link_state(DuplicateCheckboxState(1, confirm=True, dismiss=False))
            == "confirmed"
        )

    def test_dismiss(self):
        assert (
            reconcile_link_state(DuplicateCheckboxState(1, confirm=False, dismiss=True))
            == "dismissed"
        )

    def test_both_confirm_wins(self):
        assert (
            reconcile_link_state(DuplicateCheckboxState(1, confirm=True, dismiss=True))
            == "confirmed"
        )


def _store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


def _seed_link(store: StateStore) -> int:
    """Insert a Compass↔Classroom link + the two silver_tasks. Returns link id."""
    due = datetime(2026, 5, 1, tzinfo=UTC).isoformat()
    now = datetime.now(UTC).isoformat()
    with closing(sqlite3.connect(store.db_path)) as conn, conn:
        for source, source_id, title in [
            ("compass", "C1", "WW1 Benchmark"),
            ("classroom", "K1", "WW1"),
        ]:
            conn.execute(
                "INSERT INTO silver_tasks "
                "(child, source, source_id, subject_raw, subject_canonical, "
                "subject_short, title, status, last_synced, due_at) "
                "VALUES ('james', ?, ?, '', 'Year 9 Humanities', 'Hum', "
                "?, 'not_started', ?, ?)",
                (source, source_id, title, now, due),
            )
        cur = conn.execute(
            "INSERT INTO silver_task_links "
            "(child, primary_source, primary_source_id, "
            "secondary_source, secondary_source_id, confidence, state, "
            "score_subject, score_title, score_due, detected_at) "
            "VALUES ('james', 'compass', 'C1', 'classroom', 'K1', "
            "'auto_high', 'pending', 1.0, 1.0, 0, ?)",
            (now,),
        )
        return int(cur.lastrowid or 0)


class TestLoadLinksForPublish:
    def test_loads_with_titles_and_dues(self, tmp_path: Path):
        store = _store(tmp_path)
        link_id = _seed_link(store)
        rows = load_links_for_publish(store, "james")
        assert len(rows) == 1
        assert rows[0].link_id == link_id
        assert rows[0].compass_title == "WW1 Benchmark"
        assert rows[0].classroom_title == "WW1"
        assert rows[0].state == "pending"


class TestApplyLinkStateWritebacks:
    def test_confirm_persists(self, tmp_path: Path):
        store = _store(tmp_path)
        link_id = _seed_link(store)
        updated = apply_link_state_writebacks(
            store,
            [DuplicateCheckboxState(link_id, confirm=True, dismiss=False)],
        )
        assert updated == 1
        with closing(sqlite3.connect(store.db_path)) as conn:
            (state,) = conn.execute(
                "SELECT state FROM silver_task_links WHERE id = ?", (link_id,)
            ).fetchone()
        assert state == "confirmed"

    def test_no_change_when_neither_ticked(self, tmp_path: Path):
        store = _store(tmp_path)
        link_id = _seed_link(store)
        updated = apply_link_state_writebacks(
            store,
            [DuplicateCheckboxState(link_id, confirm=False, dismiss=False)],
        )
        assert updated == 0


# --------------------------------------------------------------------------- #
# task_uid
# --------------------------------------------------------------------------- #


class TestTaskUid:
    def test_format(self):
        assert task_uid(_task(source=Source.COMPASS, source_id="42")) == "compass:42"
        assert task_uid(_task(source=Source.CLASSROOM, source_id="abc")) == "classroom:abc"


# --------------------------------------------------------------------------- #
# publish_for_child — integration with a fake sink
# --------------------------------------------------------------------------- #


class FakeGoldSink:
    def __init__(
        self,
        *,
        user_edits: list[UserEdit] | None = None,
        checkboxes: list[DuplicateCheckboxState] | None = None,
    ):
        self._user_edits = user_edits or []
        self._checkboxes = checkboxes or []
        self.writes: dict[str, list[tuple]] = {}
        self.hidden_state: dict[str, bool] = {}

    def read_user_edits(self, spreadsheet_id: str) -> list[UserEdit]:
        return list(self._user_edits)

    def read_duplicate_checkboxes(self, spreadsheet_id: str) -> list[DuplicateCheckboxState]:
        return list(self._checkboxes)

    def write_tab(self, spreadsheet_id: str, tab, rows: list[tuple]) -> None:
        self.writes[tab.name] = rows

    def set_tab_hidden(self, spreadsheet_id: str, tab, hidden: bool) -> None:
        self.hidden_state[tab.name] = hidden


class TestPublishForChild:
    def test_writes_all_managed_tabs(self, tmp_path: Path):
        store = _store(tmp_path)
        sink = FakeGoldSink()
        result = publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task(due_at=datetime(2026, 5, 1, 14, 0, tzinfo=UTC))],
            last_synced=datetime(2026, 5, 1, tzinfo=UTC),
        )
        assert result.tasks_written == 1
        assert "Tasks" in sink.writes
        assert "Possible Duplicates" in sink.writes
        assert "Settings" in sink.writes
        assert "UserEdits" in sink.writes

    def test_duplicates_tab_hidden_when_empty(self, tmp_path: Path):
        store = _store(tmp_path)
        sink = FakeGoldSink()
        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        assert sink.hidden_state["Possible Duplicates"] is True

    def test_duplicates_tab_visible_when_pending_link_exists(self, tmp_path: Path):
        store = _store(tmp_path)
        _seed_link(store)
        sink = FakeGoldSink()
        result = publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        assert result.duplicates_written == 1
        assert sink.hidden_state["Possible Duplicates"] is False

    def test_checkbox_state_persisted_before_link_load(self, tmp_path: Path):
        store = _store(tmp_path)
        link_id = _seed_link(store)
        sink = FakeGoldSink(
            checkboxes=[DuplicateCheckboxState(link_id, confirm=True, dismiss=False)]
        )
        result = publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        # Confirmed link is dropped from this publish (state is no longer pending).
        assert result.duplicates_written == 0
        assert result.duplicates_state_updates == 1

    def test_user_edit_round_trip(self, tmp_path: Path):
        store = _store(tmp_path)
        existing = [UserEdit("compass:T1", "priority", "High", "OLD-TS")]
        sink = FakeGoldSink(user_edits=existing)
        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        # Tasks tab reflects the override.
        tasks_rows = sink.writes["Tasks"]
        assert tasks_rows[0][_idx("priority")] == "High"
        # UserEdits writeback preserves the old timestamp (no churn).
        ue_rows = sink.writes["UserEdits"]
        assert len(ue_rows) == 1
        assert ue_rows[0][0] == "compass:T1"
        assert ue_rows[0][1] == "priority"
        assert ue_rows[0][2] == "High"
        assert ue_rows[0][3] == "OLD-TS"

    def test_idempotent_when_state_unchanged(self, tmp_path: Path):
        store = _store(tmp_path)
        sink = FakeGoldSink()
        tasks = [_task(due_at=datetime(2026, 5, 1, tzinfo=UTC))]
        first = publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=tasks,
            last_synced=None,
        )
        second_sink = FakeGoldSink()
        second = publish_for_child(
            store,
            second_sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=tasks,
            last_synced=None,
        )
        assert first.tasks_written == second.tasks_written
        assert sink.writes["Tasks"] == second_sink.writes["Tasks"]

"""Tests for the Gold publish layer (M5)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from homework_hub.models import Source, Status, Task, TaskType
from homework_hub.pipeline.publish import (
    DashboardMeta,
    UserEdit,
    apply_archive_edits,
    apply_unarchive_edits,
    cap_future_dates,
    capture_tab_edits,
    checkpoint_uid,
    diff_user_edits,
    filter_superseded_edits,
    melbourne_local_date,
    merge_user_edits,
    parent_uid_from_checkpoint,
    partition_tasks,
    project_settings_rows,
    project_tasks_rows,
    publish_for_child,
    task_uid,
)
from homework_hub.schema import TASKS_TAB
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
    submitted_at: datetime | None = None,
    status: Status = Status.NOT_STARTED,
    url: str = "https://example/test",
    first_seen_at: datetime | None = None,
    archived_at: datetime | None = None,
    archived_reason: str | None = None,
) -> Task:
    return Task(
        source=source,
        source_id=source_id,
        child=child,
        subject=subject,
        title=title,
        due_at=due_at,
        submitted_at=submitted_at,
        status=status,
        url=url,
        first_seen_at=first_seen_at,
        archived_at=archived_at,
        archived_reason=archived_reason,
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
        # Days written as a row-relative formula for Sheets to evaluate.
        assert "TODAY()" in str(cells[_idx("days")])
        assert cells[_idx("status")] == "Not started"
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

    def test_task_type_defaults_to_homework(self):
        rows = project_tasks_rows([_task()])
        assert rows[0].cells[_idx("task_type")] == "Homework"

    def test_task_type_assessment(self):
        t = Task(
            source=Source.COMPASS,
            source_id="X1",
            child="james",
            subject="11BIO",
            title="SAC 1",
            task_type=TaskType.ASSESSMENT,
            url="https://example/x",
        )
        rows = project_tasks_rows([t])
        assert rows[0].cells[_idx("task_type")] == "Assessment"

    def test_task_type_general(self):
        t = Task(
            source=Source.COMPASS,
            source_id="X2",
            child="james",
            subject="11BIO",
            title="Advocacy task",
            task_type=TaskType.GENERAL,
            url="https://example/x",
        )
        rows = project_tasks_rows([t])
        assert rows[0].cells[_idx("task_type")] == "General"

    def test_checkpoint_sub_tasks_expanded(self):
        t = Task(
            source=Source.COMPASS,
            source_id="CP1",
            child="james",
            subject="11MAM",
            title="Chapter 1 Coursework",
            checkpoints=[
                {"id": 101, "name": "Equations"},
                {"id": 102, "name": "Inequalities"},
            ],
            url="https://example/cp",
        )
        rows = project_tasks_rows([t])
        # Parent row suppressed — one row per checkpoint only.
        assert len(rows) == 2
        assert rows[0].task_uid == "compass:CP1:gi:101"
        assert rows[1].task_uid == "compass:CP1:gi:102"
        # Title merges parent + checkpoint name.
        assert rows[0].cells[_idx("title")] == "Chapter 1 Coursework: Equations"
        assert rows[1].cells[_idx("title")] == "Chapter 1 Coursework: Inequalities"
        # Checkpoints inherit parent subject.
        assert rows[0].cells[_idx("subject")] == "11MAM"

    def test_checkpoint_with_no_id_skipped(self):
        t = Task(
            source=Source.COMPASS,
            source_id="CP2",
            child="james",
            subject="11MAM",
            title="Task",
            checkpoints=[{"id": None, "name": "Should be skipped"}],
            url="https://example/cp",
        )
        rows = project_tasks_rows([t])
        assert len(rows) == 0  # parent suppressed, bad checkpoint dropped


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #


class TestProjectSettingsRows:
    def test_basic(self):
        rows = project_settings_rows(
            child="james",
            last_synced=datetime(2026, 5, 1, 14, 0, tzinfo=UTC),
        )
        # All rows are 4-column tuples.
        for r in rows:
            assert len(r) == 4
        keys = [r[0] for r in rows]
        assert "Child" in keys
        assert "Last full sync" in keys
        assert "Tabs managed" in keys
        # Tabs managed should not include the hidden UserEdits tab.
        tabs_value = next(r[1] for r in rows if r[0] == "Tabs managed")
        assert "UserEdits" not in tabs_value
        assert "Dashboard" in tabs_value
        assert "Tasks" in tabs_value
        assert "History" in tabs_value
        # Last full sync renders in Melbourne dd/mm/yyyy.
        last_full = next(r[1] for r in rows if r[0] == "Last full sync")
        # 2026-05-01 14:00 UTC == 2026-05-02 00:00 AEST (UTC+10, no DST in May).
        assert last_full.startswith("02/05/2026")

    def test_no_last_synced(self):
        rows = project_settings_rows(child="james", last_synced=None)
        last_full = next(r[1] for r in rows if r[0] == "Last full sync")
        assert last_full == "—"

    def test_with_source_auth_rows(self):
        from homework_hub.pipeline.auth_status import SourceAuthRow

        auth_rows = [
            SourceAuthRow(
                source="classroom",
                display_name="Classroom",
                last_success_at=datetime(2026, 5, 1, 14, 0, tzinfo=UTC),
                last_failure_at=None,
                last_failure_kind=None,
                token_expires_at=datetime(2027, 8, 14, 0, 0, tzinfo=UTC),
                token_present=True,
                status="ok",
            ),
            SourceAuthRow(
                source="eduperfect",
                display_name="EduPerfect",
                last_success_at=datetime(2026, 5, 1, 14, 0, tzinfo=UTC),
                last_failure_at=datetime(2026, 5, 1, 15, 0, tzinfo=UTC),
                last_failure_kind="auth_expired",
                token_expires_at=datetime(2026, 5, 1, 13, 0, tzinfo=UTC),
                token_present=True,
                status="expired",
            ),
        ]
        rows = project_settings_rows(
            child="james",
            last_synced=datetime(2026, 5, 1, 14, 0, tzinfo=UTC),
            source_auth_rows=auth_rows,
        )
        # First two rows are the per-source rows.
        assert rows[0][0] == "Classroom"
        assert rows[0][3] == "OK"
        assert rows[1][0] == "EduPerfect"
        assert rows[1][3] == "Expired"
        # Last full sync still present in trailer.
        assert any(r[0] == "Last full sync" for r in rows)


# --------------------------------------------------------------------------- #
# UserEdits merge
# --------------------------------------------------------------------------- #


class TestMergeUserEdits:
    def test_no_edits_passthrough(self):
        rows = project_tasks_rows([_task()])
        merged = merge_user_edits(rows, [])
        assert merged == rows

    def test_notes_override_applied(self):
        rows = project_tasks_rows([_task()])
        edits = [UserEdit("compass:T1", "notes", "check chapter 4", "now")]
        merged = merge_user_edits(rows, edits)
        assert merged[0].cells[_idx("notes")] == "check chapter 4"
        # Other cells untouched.
        assert merged[0].cells[_idx("title")] == "Photosynthesis Worksheet"

    def test_non_editable_column_ignored(self):
        # Source is read-only — an edit referencing it must be dropped.
        rows = project_tasks_rows([_task(source=Source.COMPASS)])
        edits = [UserEdit("compass:T1", "source", "Classroom", "now")]
        merged = merge_user_edits(rows, edits)
        assert merged[0].cells[_idx("source")] == "Compass"

    def test_orphan_edit_dropped(self):
        # Edit for a task_uid not in silver — silently discarded.
        rows = project_tasks_rows([_task()])
        edits = [UserEdit("compass:GHOST", "notes", "nope", "now")]
        merged = merge_user_edits(rows, edits)
        assert merged[0].cells[_idx("notes")] == ""

    def test_persisted_iso_due_date_is_restored_as_date(self):
        projected = project_tasks_rows(
            [_task(due_at=datetime(2026, 6, 26, 13, 59, tzinfo=UTC))]
        )
        edits = [
            UserEdit(
                "compass:T1",
                "due",
                "2026-06-26",
                "old",
                original_value="2026-06-26",
            )
        ]

        merged = merge_user_edits(projected, edits)

        assert merged[0].cells[_idx("due")] == date(2026, 6, 26)
        assert diff_user_edits(merged, edits, projected=projected) == []


class TestDiffUserEdits:
    def test_default_values_not_emitted(self):
        projected = project_tasks_rows([_task()])
        rows = merge_user_edits(projected, [])
        edits = diff_user_edits(rows, existing=[], projected=projected)
        assert edits == []

    def test_overridden_notes_emitted(self):
        projected = project_tasks_rows([_task()])
        rows = merge_user_edits(
            projected,
            [UserEdit("compass:T1", "notes", "check chapter 4", "old")],
        )
        out = diff_user_edits(rows, existing=[], projected=projected)
        assert len(out) == 1
        assert out[0].column == "notes"
        assert out[0].value == "check chapter 4"

    def test_unchanged_value_keeps_old_timestamp(self):
        projected = project_tasks_rows([_task()])
        rows = merge_user_edits(
            projected,
            [UserEdit("compass:T1", "notes", "check chapter 4", "OLD-TS")],
        )
        out = diff_user_edits(
            rows,
            existing=[UserEdit("compass:T1", "notes", "check chapter 4", "OLD-TS")],
            projected=projected,
        )
        assert out[0].updated_at == "OLD-TS"

    def test_changed_value_gets_new_timestamp(self):
        projected = project_tasks_rows([_task()])
        rows = merge_user_edits(
            projected,
            [UserEdit("compass:T1", "notes", "updated note", "now")],
        )
        out = diff_user_edits(
            rows,
            existing=[UserEdit("compass:T1", "notes", "old note", "OLD-TS")],
            projected=projected,
        )
        assert out[0].value == "updated note"
        assert out[0].updated_at != "OLD-TS"


# --------------------------------------------------------------------------- #
# Helper: build a raw Tasks-tab row matching TASKS_TAB column order
# --------------------------------------------------------------------------- #


def _raw_row(task: Task, **overrides: str) -> list[str]:
    """Build a raw string row as returned by ``get_all_values()``.

    Projected defaults are used as the base; ``overrides`` replace specific
    column keys.  Date objects are formatted ``dd/MM/yyyy``; ``None`` becomes ``""``.
    """
    projected = project_tasks_rows([task])[0]
    cells = list(projected.cells)
    for key, val in overrides.items():
        cells[_idx(key)] = val

    row: list[str] = []
    for cell in cells:
        if cell is None:
            row.append("")
        elif isinstance(cell, date):
            row.append(cell.strftime("%d/%m/%Y"))
        else:
            row.append(str(cell))
    return row


# --------------------------------------------------------------------------- #
# capture_tab_edits
# --------------------------------------------------------------------------- #


class TestCaptureTabEdits:
    def test_empty_raw_rows_returns_no_edits(self):
        projected = project_tasks_rows([_task()])
        assert capture_tab_edits([], projected) == []

    def test_unknown_task_uid_ignored(self):
        projected = project_tasks_rows([_task()])
        row = _raw_row(_task(source_id="GHOST"))
        # uid is compass:GHOST — not in projected
        assert capture_tab_edits([row], projected) == []

    def test_notes_override_captured(self):
        t = _task()
        projected = project_tasks_rows([t])
        row = _raw_row(t, notes="check chapter 4")
        edits = capture_tab_edits([row], projected)
        assert any(e.column == "notes" and e.value == "check chapter 4" for e in edits)

    def test_status_override_captured(self):
        t = _task(status=Status.NOT_STARTED)
        projected = project_tasks_rows([t])
        row = _raw_row(t, status="Submitted")
        edits = capture_tab_edits([row], projected)
        assert any(e.column == "status" and e.value == "Submitted" for e in edits)

    def test_status_not_captured_when_matches_projected(self):
        t = _task(status=Status.NOT_STARTED)
        projected = project_tasks_rows([t])
        row = _raw_row(t)  # status="Not started" in both
        edits = capture_tab_edits([row], projected)
        assert not any(e.column == "status" for e in edits)

    def test_due_override_captured_when_projected_none(self):
        t = _task(due_at=None)
        projected = project_tasks_rows([t])
        row = _raw_row(t, due="15/05/2026")
        edits = capture_tab_edits([row], projected)
        assert any(e.column == "due" and e.value == date(2026, 5, 15) for e in edits)

    def test_due_not_captured_when_matches_projected(self):
        t = _task(due_at=datetime(2026, 5, 2, 0, 0, tzinfo=UTC))
        projected = project_tasks_rows([t])
        row = _raw_row(t)  # formatted date matches projected
        edits = capture_tab_edits([row], projected)
        assert not any(e.column == "due" for e in edits)

    def test_due_parse_failure_skipped(self):
        t = _task(due_at=None)
        projected = project_tasks_rows([t])
        row = _raw_row(t, due="not-a-date")
        edits = capture_tab_edits([row], projected)
        assert not any(e.column == "due" for e in edits)

    def test_due_serial_number_parsed(self):
        # Sheets serial for 2026-05-01 = days since 1899-12-30
        from datetime import date as _date

        serial = (_date(2026, 5, 1) - _date(1899, 12, 30)).days
        t = _task(due_at=None)
        projected = project_tasks_rows([t])
        row = _raw_row(t, due=str(serial))
        edits = capture_tab_edits([row], projected)
        assert any(e.column == "due" and e.value == _date(2026, 5, 1) for e in edits)

    def test_empty_notes_not_captured(self):
        t = _task()
        projected = project_tasks_rows([t])
        row = _raw_row(t, notes="")
        edits = capture_tab_edits([row], projected)
        assert not any(e.column == "notes" for e in edits)

    def test_history_tab_due_not_editable(self):
        """History tab does not capture due overrides — due is locked."""
        from homework_hub.schema import HISTORY_TAB

        t = _task(due_at=None)
        projected = project_tasks_rows([t])
        row = _raw_row(t, due="15/05/2026")
        edits = capture_tab_edits([row], projected, tab=HISTORY_TAB)
        # HISTORY_TAB has due_editable=False, so no due edits captured.
        assert not any(e.column == "due" for e in edits)


# --------------------------------------------------------------------------- #
# _merge_edit_sources
# --------------------------------------------------------------------------- #


from homework_hub.pipeline.publish import _merge_edit_sources  # noqa: E402


class TestMergeEditSources:
    def test_live_only(self):
        live = [UserEdit("compass:T1", "notes", "check it", "now")]
        result = _merge_edit_sources(live, [])
        assert result == live

    def test_persisted_only(self):
        persisted = [UserEdit("compass:T1", "notes", "do it", "old")]
        result = _merge_edit_sources([], persisted)
        assert result == persisted

    def test_live_wins_on_conflict(self):
        persisted = [UserEdit("compass:T1", "notes", "old note", "old")]
        live = [UserEdit("compass:T1", "notes", "new note", "now")]
        result = _merge_edit_sources(live, persisted)
        assert len(result) == 1
        assert result[0].value == "new note"

    def test_different_columns_both_kept(self):
        persisted = [UserEdit("compass:T1", "notes", "do it", "old")]
        live = [UserEdit("compass:T1", "status", "Submitted", "now")]
        result = _merge_edit_sources(live, persisted)
        assert len(result) == 2
        keys = {(e.task_uid, e.column) for e in result}
        assert ("compass:T1", "status") in keys
        assert ("compass:T1", "notes") in keys


# --------------------------------------------------------------------------- #
# filter_superseded_edits
# --------------------------------------------------------------------------- #


class TestFilterSupersededEdits:
    def test_status_edit_dropped_when_graded(self):
        t = _task(status=Status.GRADED)
        edits = [UserEdit("compass:T1", "status", "Not started", "now")]
        assert filter_superseded_edits(edits, [t]) == []

    def test_status_edit_dropped_when_overdue(self):
        t = _task(status=Status.OVERDUE)
        edits = [UserEdit("compass:T1", "status", "Not started", "now")]
        assert filter_superseded_edits(edits, [t]) == []

    def test_status_edit_dropped_when_submitted(self):
        # Submitted is terminal — the source system is authoritative on
        # completion, so a kid downgrade ("Not started" / "In progress")
        # is dropped. Without this lock, the diff layer would re-emit
        # the override forever (the edit's original_value matches the
        # current Submitted state, so drift detection can't fire).
        t = _task(status=Status.SUBMITTED)
        edits = [UserEdit("compass:T1", "status", "Not started", "now")]
        assert filter_superseded_edits(edits, [t]) == []

    def test_status_archive_edit_survives_terminal_lock(self):
        # The terminal-status lock has one exception: a kid setting
        # ``Archived`` is always allowed through, so apply_archive_edits
        # downstream can shelve Submitted/Graded/Overdue work.
        for terminal in (Status.SUBMITTED, Status.GRADED, Status.OVERDUE):
            t = _task(status=terminal)
            edits = [UserEdit("compass:T1", "status", "Archived", "now")]
            assert filter_superseded_edits(edits, [t]) == edits, (
                f"Archive edit must survive when silver={terminal}"
            )

    def test_status_edit_kept_when_not_started(self):
        t = _task(status=Status.NOT_STARTED)
        edits = [UserEdit("compass:T1", "status", "Submitted", "now")]
        assert filter_superseded_edits(edits, [t]) == edits

    def test_due_edit_kept_when_silver_has_date(self):
        # Kid always wins on due — silver date does not override their choice.
        t = _task(due_at=datetime(2026, 5, 1, tzinfo=UTC))
        edits = [UserEdit("compass:T1", "due", date(2026, 6, 1), "now")]
        assert filter_superseded_edits(edits, [t]) == edits

    def test_notes_never_dropped(self):
        t = _task(status=Status.GRADED)
        edits = [UserEdit("compass:T1", "notes", "talk to teacher", "now")]
        assert filter_superseded_edits(edits, [t]) == edits

    def test_orphan_edit_kept(self):
        # Edit for a task no longer in silver passes through (pruned later by diff).
        edits = [UserEdit("compass:GHOST", "notes", "old note", "now")]
        assert filter_superseded_edits(edits, []) == edits


# --------------------------------------------------------------------------- #
# task_uid
# --------------------------------------------------------------------------- #


class TestTaskUid:
    def test_format(self):
        assert task_uid(_task(source=Source.COMPASS, source_id="42")) == "compass:42"
        assert task_uid(_task(source=Source.CLASSROOM, source_id="abc")) == "classroom:abc"


class TestCheckpointUid:
    def test_format(self):
        t = _task(source=Source.COMPASS, source_id="42")
        assert checkpoint_uid(t, 101) == "compass:42:gi:101"

    def test_different_gi_ids_are_distinct(self):
        t = _task(source=Source.COMPASS, source_id="42")
        assert checkpoint_uid(t, 101) != checkpoint_uid(t, 102)


class TestParentUidFromCheckpoint:
    def test_checkpoint_uid_extracts_parent(self):
        assert parent_uid_from_checkpoint("compass:42:gi:101") == "compass:42"

    def test_regular_uid_returns_none(self):
        assert parent_uid_from_checkpoint("compass:42") is None

    def test_classroom_checkpoint(self):
        assert parent_uid_from_checkpoint("classroom:abc:gi:5") == "classroom:abc"


# --------------------------------------------------------------------------- #
# partition_tasks
# --------------------------------------------------------------------------- #


class TestPartitionTasks:
    def test_active_task_stays_active(self):
        t = _task(status=Status.NOT_STARTED)
        rows = project_tasks_rows([t])
        active, history = partition_tasks(rows, [t], [])
        assert len(active) == 1
        assert len(history) == 0

    def test_recent_submitted_stays_active(self):
        """Submitted within cutoff stays in Active (not yet historical)."""
        t = _task(
            status=Status.SUBMITTED,
            due_at=datetime.now(UTC),  # very recent
        )
        rows = project_tasks_rows([t])
        active, _history = partition_tasks(rows, [t], [], cutoff_days=30)
        assert len(active) == 1

    def test_old_submitted_moves_to_history(self):
        """Submitted > cutoff_days ago moves to History."""
        from datetime import timedelta

        old_due = datetime.now(UTC) - timedelta(days=40)
        t = _task(status=Status.SUBMITTED, due_at=old_due)
        rows = project_tasks_rows([t])
        active, history = partition_tasks(rows, [t], [], cutoff_days=30)
        assert len(history) == 1
        assert len(active) == 0

    def test_old_graded_moves_to_history(self):
        from datetime import timedelta

        old_due = datetime.now(UTC) - timedelta(days=45)
        t = _task(status=Status.GRADED, due_at=old_due)
        rows = project_tasks_rows([t])
        _active, history = partition_tasks(rows, [t], [], cutoff_days=30)
        assert len(history) == 1


# --------------------------------------------------------------------------- #
# publish_for_child — integration with a fake sink
# --------------------------------------------------------------------------- #


class FakeGoldSink:
    def __init__(
        self,
        *,
        user_edits: list[UserEdit] | None = None,
        raw_tab_rows: dict[str, list[list[str]]] | None = None,
        dashboard_meta: DashboardMeta | None = None,
    ):
        self._user_edits = user_edits or []
        self._raw_tab_rows = raw_tab_rows or {}
        self.writes: dict[str, list[tuple]] = {}
        self.hidden_state: dict[str, bool] = {}
        self.dashboard_requests: list[list[dict]] = []
        self.protection_installs: list[tuple[str, int]] = []
        self._dashboard_meta = dashboard_meta or DashboardMeta(
            sheet_id=0,
            table_ids=[],
            banded_range_ids=[],
            conditional_format_rule_count=0,
        )

    def read_user_edits(self, spreadsheet_id: str) -> list[UserEdit]:
        return list(self._user_edits)

    def read_tab_raw(self, spreadsheet_id: str, tab_name: str) -> list[list[str]]:
        return list(self._raw_tab_rows.get(tab_name, []))

    def write_tab(self, spreadsheet_id: str, tab, rows: list[tuple]) -> None:
        self.writes[tab.name] = rows

    def set_tab_hidden(self, spreadsheet_id: str, tab, hidden: bool) -> None:
        self.hidden_state[tab.name] = hidden

    def read_dashboard_meta(self, spreadsheet_id: str) -> DashboardMeta:
        return self._dashboard_meta

    def write_dashboard_layout(self, spreadsheet_id: str, requests: list[dict]) -> None:
        self.dashboard_requests.append(list(requests))

    def write_dashboard_protection(
        self, spreadsheet_id: str, dashboard_sheet_id: int
    ) -> None:
        self.protection_installs.append((spreadsheet_id, dashboard_sheet_id))


def _store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


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
        assert "History" in sink.writes
        assert "Settings" in sink.writes
        assert "UserEdits" in sink.writes

    def test_user_edit_round_trip(self, tmp_path: Path):
        store = _store(tmp_path)
        existing = [UserEdit("compass:T1", "notes", "check chapter 4", "OLD-TS")]
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
        assert tasks_rows[0][_idx("notes")] == "check chapter 4"
        # UserEdits writeback preserves the old timestamp (no churn).
        # Tab schema: task_uid | column | original_value | value | updated_at.
        ue_rows = sink.writes["UserEdits"]
        assert len(ue_rows) == 1
        assert ue_rows[0][0] == "compass:T1"
        assert ue_rows[0][1] == "notes"
        assert ue_rows[0][2] == ""  # original_value: notes default is empty
        assert ue_rows[0][3] == "check chapter 4"
        assert ue_rows[0][4] == "OLD-TS"

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

    def test_tasks_tab_edit_captured_via_read_tab_raw(self, tmp_path: Path):
        """A notes override in the live Tasks tab is captured and round-tripped."""
        store = _store(tmp_path)
        t = _task()
        raw_row = _raw_row(t, notes="do chapter 4")
        sink = FakeGoldSink(raw_tab_rows={"Tasks": [raw_row]})
        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[t],
            last_synced=None,
        )
        tasks_rows = sink.writes["Tasks"]
        assert tasks_rows[0][_idx("notes")] == "do chapter 4"
        ue_rows = sink.writes["UserEdits"]
        # UserEdits row: task_uid | column | original_value | value | updated_at
        assert any(r[1] == "notes" and r[3] == "do chapter 4" for r in ue_rows)

    def test_graded_status_not_overridden_by_tasks_tab(self, tmp_path: Path):
        """Silver Graded locks status — even if the Tasks tab shows something else."""
        store = _store(tmp_path)
        # Recent first_seen_at so the row stays on Tasks (within the
        # history cutoff) rather than partitioning straight to History.
        t = _task(status=Status.GRADED, first_seen_at=datetime.now(UTC))
        raw_row = _raw_row(t, status="Not started")
        sink = FakeGoldSink(raw_tab_rows={"Tasks": [raw_row]})
        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[t],
            last_synced=None,
        )
        tasks_rows = sink.writes["Tasks"]
        assert tasks_rows[0][_idx("status")] == "Graded"

    def test_history_written_count(self, tmp_path: Path):
        """Old submitted tasks appear in history_written, not tasks_written."""
        from datetime import timedelta

        store = _store(tmp_path)
        old_due = datetime.now(UTC) - timedelta(days=40)
        t = _task(status=Status.SUBMITTED, due_at=old_due)
        sink = FakeGoldSink()
        result = publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[t],
            last_synced=None,
            cutoff_days=30,
        )
        assert result.tasks_written == 0
        assert result.history_written == 1


# --------------------------------------------------------------------------- #
# Archived routing in partition_tasks
# --------------------------------------------------------------------------- #


class TestPartitionTasksArchived:
    def test_archived_routes_immediately_to_history(self):
        """Archived rows skip the cutoff wait and go straight to History."""
        t = _task(
            status=Status.ARCHIVED,
            first_seen_at=datetime.now(UTC),  # very recent — would normally stay Active.
        )
        rows = project_tasks_rows([t])
        active, history = partition_tasks(rows, [t], [], cutoff_days=30)
        assert len(history) == 1
        assert len(active) == 0

    def test_terminal_without_anchor_routes_to_history(self):
        """Submitted/Graded with no submitted_at, due_at OR first_seen_at —
        treat as stale and route to History (fixes the 'Submitted with blank
        Due' zombie that used to linger on Tasks forever)."""
        t = _task(status=Status.SUBMITTED)  # no anchors
        rows = project_tasks_rows([t])
        active, history = partition_tasks(rows, [t], [], cutoff_days=30)
        assert len(history) == 1
        assert len(active) == 0

    def test_terminal_falls_back_to_first_seen_at(self):
        """When submitted_at and due_at are absent, first_seen_at anchors
        the cutoff comparison (replacing the old last_synced fallback that
        kept refreshing every sync)."""
        from datetime import timedelta

        old_first_seen = datetime.now(UTC) - timedelta(days=40)
        t = _task(status=Status.SUBMITTED, first_seen_at=old_first_seen)
        rows = project_tasks_rows([t])
        _active, history = partition_tasks(rows, [t], [], cutoff_days=30)
        assert len(history) == 1


# --------------------------------------------------------------------------- #
# cap_future_dates
# --------------------------------------------------------------------------- #


class TestCapFutureDates:
    def test_blanks_due_beyond_cap(self):
        """Due date more than ``cap_days`` in the future gets blanked."""
        from datetime import timedelta

        today = date(2026, 5, 26)
        far_future = today + timedelta(days=500)
        t = _task(due_at=datetime(far_future.year, far_future.month, far_future.day, tzinfo=UTC))
        rows = project_tasks_rows([t])
        capped = cap_future_dates(rows, cap_days=365, today=today)
        due_idx = TASKS_TAB.column_index("due")
        assert capped[0].cells[due_idx] == ""

    def test_preserves_due_within_cap(self):
        from datetime import timedelta

        today = date(2026, 5, 26)
        soon = today + timedelta(days=100)
        t = _task(due_at=datetime(soon.year, soon.month, soon.day, tzinfo=UTC))
        rows = project_tasks_rows([t])
        capped = cap_future_dates(rows, cap_days=365, today=today)
        due_idx = TASKS_TAB.column_index("due")
        assert capped[0].cells[due_idx] == soon

    def test_blank_due_is_passthrough(self):
        t = _task()  # no due
        rows = project_tasks_rows([t])
        capped = cap_future_dates(rows, cap_days=365)
        assert capped == rows


# --------------------------------------------------------------------------- #
# filter_superseded_edits — archived rules
# --------------------------------------------------------------------------- #


class TestFilterSupersededEditsArchived:
    def test_kid_can_set_archived(self):
        """Kids can manually archive via the sheet — the edit is preserved
        through filter_superseded_edits and apply_archive_edits writes the
        archive flags through to silver."""
        t = _task(status=Status.NOT_STARTED)
        edits = [UserEdit("compass:T1", "status", "Archived", "now")]
        assert filter_superseded_edits(edits, [t]) == edits

    def test_kid_can_set_archived_case_insensitive(self):
        t = _task(status=Status.NOT_STARTED)
        edits = [UserEdit("compass:T1", "status", "archived", "now")]
        assert filter_superseded_edits(edits, [t]) == edits

    def test_kid_can_un_archive_to_not_started(self):
        """Reverse direction is allowed — kid clears an archived task back to active."""
        t = _task(status=Status.ARCHIVED)
        edits = [UserEdit("compass:T1", "status", "Not started", "now")]
        assert filter_superseded_edits(edits, [t]) == edits


# --------------------------------------------------------------------------- #
# apply_unarchive_edits
# --------------------------------------------------------------------------- #


class TestApplyUnarchiveEdits:
    def test_clears_archive_flags_and_updates_task(self, tmp_path: Path):
        store = _store(tmp_path)
        t = _task(status=Status.ARCHIVED)
        # Seed silver with the archived row so clear_archive has something to update.
        from homework_hub.pipeline.transform import SilverWriter

        writer = SilverWriter(store)
        writer.upsert_many([(t.model_copy(update={"status": Status.NOT_STARTED}), None)])
        store.mark_archived(child="james", source="compass", source_id="T1", reason="manual")
        edits = [UserEdit("compass:T1", "status", "Not started", "now")]

        updated = apply_unarchive_edits(edits, [t], store)
        assert updated[0].status == Status.NOT_STARTED
        assert updated[0].archived_at is None
        assert updated[0].archived_reason is None
        # Silver row's archive flags should be cleared too.
        assert store.list_archived(child="james") == []

    def test_no_op_when_no_status_edits(self, tmp_path: Path):
        store = _store(tmp_path)
        t = _task(status=Status.ARCHIVED)
        edits = [UserEdit("compass:T1", "notes", "hello", "now")]
        tasks = [t]
        # When there are no status edits, the original list is returned as-is.
        assert apply_unarchive_edits(edits, tasks, store) is tasks

    def test_archived_to_archived_edit_does_nothing(self, tmp_path: Path):
        # An edit setting status="Archived" on an already-archived task should not
        # touch silver and should return the task list unchanged.
        store = _store(tmp_path)
        t = _task(status=Status.ARCHIVED)
        edits = [UserEdit("compass:T1", "status", "Archived", "now")]
        result = apply_unarchive_edits(edits, [t], store)
        assert result[0].status == Status.ARCHIVED


# --------------------------------------------------------------------------- #
# apply_archive_edits
# --------------------------------------------------------------------------- #


class TestApplyArchiveEdits:
    def test_kid_archive_writes_silver_flags(self, tmp_path: Path):
        """A status="Archived" edit on a non-archived task writes the archive
        flags through to silver so the row stays archived across syncs."""
        from homework_hub.pipeline.transform import SilverWriter

        store = _store(tmp_path)
        t = _task(status=Status.NOT_STARTED)
        SilverWriter(store).upsert_many([(t, None)])
        edits = [UserEdit("compass:T1", "status", "Archived", "now")]

        updated = apply_archive_edits(edits, [t], store)
        assert updated[0].status == Status.ARCHIVED
        assert updated[0].archived_reason == "kid_edit"
        # Silver row should now be archived.
        archived = store.list_archived(child="james")
        assert len(archived) == 1
        assert archived[0]["source_id"] == "T1"
        assert archived[0]["archived_reason"] == "kid_edit"

    def test_no_op_when_no_archive_edits(self, tmp_path: Path):
        store = _store(tmp_path)
        t = _task(status=Status.NOT_STARTED)
        edits = [UserEdit("compass:T1", "status", "In progress", "now")]
        tasks = [t]
        assert apply_archive_edits(edits, tasks, store) is tasks

    def test_already_archived_is_skipped(self, tmp_path: Path):
        store = _store(tmp_path)
        t = _task(status=Status.ARCHIVED)
        edits = [UserEdit("compass:T1", "status", "Archived", "now")]
        result = apply_archive_edits(edits, [t], store)
        # Already archived — task list returned unchanged (identity).
        assert result == [t] or result[0].status == Status.ARCHIVED

    def test_terminal_status_not_archived_by_kid(self, tmp_path: Path):
        """Graded / Overdue silver states cannot be archived via the sheet.
        ``filter_superseded_edits`` now lets the ``Archived`` edit through
        (so Submitted rows can be shelved), but ``apply_archive_edits``
        self-guards against Graded / Overdue as defence in depth — those
        are machine-derived terminal states the kid shouldn't be able to
        bury."""
        from homework_hub.pipeline.transform import SilverWriter

        store = _store(tmp_path)
        t = _task(status=Status.GRADED)
        SilverWriter(store).upsert_many([(t, None)])
        edits = [UserEdit("compass:T1", "status", "Archived", "now")]
        result = apply_archive_edits(edits, [t], store)
        assert result[0].status == Status.GRADED
        assert store.list_archived(child="james") == []


# --------------------------------------------------------------------------- #
# UserEdits original_value (Option B: refreshed each cycle)
# --------------------------------------------------------------------------- #


class TestUserEditOriginalValue:
    def test_capture_records_original_value_from_projected_default(self):
        """When a kid override is first observed, original_value captures the
        projected silver value at that moment."""
        from homework_hub.pipeline.publish import _STATUS_DISPLAY

        t = _task(status=Status.NOT_STARTED)
        projected = project_tasks_rows([t])
        # Kid set status to "In progress" — raw row reflects that.
        raw_row = _raw_row(t, status="In progress")
        edits = capture_tab_edits([raw_row], projected, tab=TASKS_TAB)
        assert len(edits) == 1
        assert edits[0].column == "status"
        assert edits[0].value == "In progress"
        # original_value = projected silver default at observation time.
        assert edits[0].original_value == _STATUS_DISPLAY[Status.NOT_STARTED.value]

    def test_diff_refreshes_original_value_each_cycle(self):
        """Even when value is unchanged across publish cycles, original_value
        is refreshed to the current projected silver value (Option B)."""
        t = _task(status=Status.IN_PROGRESS)  # silver advanced
        projected = project_tasks_rows([t])
        # Kid override still says "Submitted" — value diverges from new
        # silver default "In progress".
        merged_idx = TASKS_TAB.column_index("status")
        merged_cells = list(projected[0].cells)
        merged_cells[merged_idx] = "Submitted"
        from homework_hub.pipeline.publish import TaskRow

        merged_row = TaskRow(task_uid=projected[0].task_uid, cells=tuple(merged_cells))
        prior = UserEdit(
            task_uid=projected[0].task_uid,
            column="status",
            value="Submitted",
            updated_at="OLD-TS",
            original_value="Not started",  # stale: was captured when silver was Not started
        )
        out = diff_user_edits([merged_row], [prior], projected=projected)
        assert len(out) == 1
        assert out[0].value == "Submitted"
        assert out[0].updated_at == "OLD-TS"  # preserved
        assert out[0].original_value == "In progress"  # refreshed

    def test_diff_drops_override_when_system_catches_up(self):
        """When silver projects the same value the kid set, the override is
        automatically dropped from UserEdits (no row written)."""
        t = _task(status=Status.SUBMITTED)  # silver caught up
        projected = project_tasks_rows([t])
        # Merged row matches the projected default exactly (kid's earlier
        # override "Submitted" now agrees with silver).
        merged_row = projected[0]
        prior = UserEdit(
            task_uid=projected[0].task_uid,
            column="status",
            value="Submitted",
            updated_at="OLD-TS",
            original_value="Not started",
        )
        out = diff_user_edits([merged_row], [prior], projected=projected)
        # Override removed — system has caught up.
        assert out == []


# --------------------------------------------------------------------------- #
# Regression: row moving from Tasks → History must not phantom-unarchive
# --------------------------------------------------------------------------- #


class TestArchivedRowDoesNotPhantomUnarchive:
    """When a row transitions from Tasks to History this cycle (e.g. it was
    just archived by ``apply_age_cap``), the previous sheet still has it on
    the Tasks tab with its OLD status (e.g. ``Not started``). The publish
    pipeline must not interpret that stale Tasks-tab cell as a kid-driven
    un-archive edit. Otherwise the archive flags get cleared every sync and
    the row bounces back to the Tasks tab on the next projection."""

    def test_stale_tasks_tab_row_does_not_unarchive(self, tmp_path: Path):
        store = _store(tmp_path)
        archived_at = datetime(2026, 5, 26, tzinfo=UTC)
        t = _task(
            status=Status.ARCHIVED,
            due_at=datetime(2024, 11, 20, tzinfo=UTC),
            archived_at=archived_at,
            archived_reason="age_cap",
        )

        # Seed silver and mark archived so we can detect un-archive side-effects.
        from homework_hub.pipeline.transform import SilverWriter

        writer = SilverWriter(store)
        writer.upsert_many([(t.model_copy(update={"status": Status.NOT_STARTED}), None)])
        store.mark_archived(child="james", source="compass", source_id="T1", reason="age_cap")

        # The previous sheet had this row on the Tasks tab with status
        # "Not started". Simulate that stale state by feeding a raw Tasks-tab
        # row that matches the OLD projection (status "Not started").
        stale_task = t.model_copy(update={"status": Status.NOT_STARTED})
        stale_raw = _raw_row(stale_task)
        sink = FakeGoldSink(raw_tab_rows={TASKS_TAB.name: [stale_raw]})

        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[t],
            last_synced=None,
        )

        # Silver must still be archived — no phantom un-archive.
        archived = store.list_archived(child="james")
        assert len(archived) == 1
        assert archived[0]["source_id"] == "T1"
        assert archived[0]["archived_reason"] == "age_cap"

        # And the row must be on the History tab, not Tasks.
        tasks_rows = sink.writes[TASKS_TAB.name]
        history_rows = sink.writes["History"]
        assert all(row[_idx("task_uid")] != "compass:T1" for row in tasks_rows)
        assert any(row[_idx("task_uid")] == "compass:T1" for row in history_rows)


class TestPublishDashboard:
    """v5.0: publish_for_child must read Dashboard meta then emit a
    batchUpdate body with one addTable per section, scoped to the
    active (non-archived) tasks."""

    def test_publishes_dashboard_layout_with_five_tables(self, tmp_path: Path):
        store = _store(tmp_path)
        meta = DashboardMeta(
            sheet_id=42,
            table_ids=["t_a", "t_b"],
            banded_range_ids=[1, 2],
            conditional_format_rule_count=4,
        )
        sink = FakeGoldSink(dashboard_meta=meta)
        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task(due_at=datetime(2026, 5, 1, tzinfo=UTC))],
            last_synced=None,
        )
        assert len(sink.dashboard_requests) == 1
        reqs = sink.dashboard_requests[0]
        # Five addTable requests, one per section
        # (Overdue / DueThisWeek / NoDueDate / Upcoming / Done7D).
        addtables = [r for r in reqs if "addTable" in r]
        assert len(addtables) == 5
        # Teardown wired through from meta.
        assert sum(1 for r in reqs if "deleteTable" in r) == 2
        assert sum(1 for r in reqs if "deleteBanding" in r) == 2
        assert sum(1 for r in reqs if "deleteConditionalFormatRule" in r) == 4
        # All Tables anchored to the meta sheetId.
        for at in addtables:
            assert at["addTable"]["table"]["range"]["sheetId"] == 42

    def test_dashboard_failure_does_not_break_publish(self, tmp_path: Path):
        """Tasks/History/UserEdits writes are canonical state — a
        Dashboard refresh failure must be swallowed (logged)."""
        store = _store(tmp_path)

        class BrokenSink(FakeGoldSink):
            def write_dashboard_layout(self, spreadsheet_id, requests):
                raise RuntimeError("API blew up")

        sink = BrokenSink()
        result = publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        # Publish still returned a result; Tasks tab still written.
        assert result.tasks_written == 1
        assert "Tasks" in sink.writes


class TestPublishDashboardProtection:
    """v5.1: publish installs a whole-sheet protected range on the
    Dashboard exactly once, then leaves it alone on subsequent runs."""

    def test_installs_protection_when_meta_reports_none(self, tmp_path: Path):
        store = _store(tmp_path)
        meta = DashboardMeta(
            sheet_id=42,
            table_ids=[],
            banded_range_ids=[],
            conditional_format_rule_count=0,
            protected_range_ids=[],
        )
        sink = FakeGoldSink(dashboard_meta=meta)
        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        assert sink.protection_installs == [("SS1", 42)]

    def test_skips_protection_when_meta_already_has_one(self, tmp_path: Path):
        store = _store(tmp_path)
        meta = DashboardMeta(
            sheet_id=42,
            table_ids=[],
            banded_range_ids=[],
            conditional_format_rule_count=0,
            protected_range_ids=[999],
        )
        sink = FakeGoldSink(dashboard_meta=meta)
        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        assert sink.protection_installs == []

    def test_protection_failure_does_not_break_publish(self, tmp_path: Path):
        """Protection install is best-effort. A failure here must not
        prevent the rest of publish from completing — Tasks/History/etc.
        are canonical state."""
        store = _store(tmp_path)

        class BrokenProtectionSink(FakeGoldSink):
            def write_dashboard_protection(self, spreadsheet_id, dashboard_sheet_id):
                raise RuntimeError("addProtectedRange 500")

        meta = DashboardMeta(
            sheet_id=42,
            table_ids=[],
            banded_range_ids=[],
            conditional_format_rule_count=0,
            protected_range_ids=[],
        )
        sink = BrokenProtectionSink(dashboard_meta=meta)
        result = publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        assert result.tasks_written == 1
        assert "Tasks" in sink.writes


class TestPublishDashboardThemeAccent:
    """v5.2: publish reads ``DashboardMeta.theme_accent`` and threads it
    into the layout builder so every Sheets-Table header chip on the
    spreadsheet — Dashboard + Tasks + History + UserEdits + Settings —
    tracks the kid's chosen ``Format → Theme``."""

    def test_theme_accent_threaded_into_section_table_headers(self, tmp_path: Path):
        store = _store(tmp_path)
        accent = {"red": 0.2, "green": 0.4, "blue": 0.8}
        meta = DashboardMeta(
            sheet_id=42,
            table_ids=[],
            banded_range_ids=[],
            conditional_format_rule_count=0,
            theme_accent=accent,
        )
        sink = FakeGoldSink(dashboard_meta=meta)
        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        reqs = sink.dashboard_requests[0]
        section_updates = [
            r
            for r in reqs
            if "updateTable" in r and "columnProperties" in r["updateTable"]["table"]
        ]
        assert section_updates  # at least one section
        for r in section_updates:
            rgb = r["updateTable"]["table"]["rowsProperties"]["headerColorStyle"]["rgbColor"]
            assert rgb == accent

    def test_repaint_requests_present_for_non_dashboard_tables(self, tmp_path: Path):
        from homework_hub.schema import DASHBOARD_TAB, SCHEMA

        store = _store(tmp_path)
        meta = DashboardMeta(
            sheet_id=42,
            table_ids=[],
            banded_range_ids=[],
            conditional_format_rule_count=0,
            theme_accent={"red": 0.2, "green": 0.4, "blue": 0.8},
        )
        sink = FakeGoldSink(dashboard_meta=meta)
        publish_for_child(
            store,
            sink,
            child="james",
            spreadsheet_id="SS1",
            tasks=[_task()],
            last_synced=None,
        )
        reqs = sink.dashboard_requests[0]
        repaints = [
            r
            for r in reqs
            if "updateTable" in r
            and "columnProperties" not in r["updateTable"]["table"]
        ]
        expected = {
            tab.table_id
            for tab in SCHEMA.tabs
            if tab.table_id and tab.name != DASHBOARD_TAB.name
        }
        seen = {r["updateTable"]["table"]["tableId"] for r in repaints}
        assert seen == expected

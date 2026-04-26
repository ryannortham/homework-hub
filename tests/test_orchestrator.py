"""Tests for the sync orchestrator."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from homework_hub.config import ChildConfig, ChildrenConfig
from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task
from homework_hub.orchestrator import Orchestrator, summarise_for_humans
from homework_hub.sinks.sheets_diff import RawDiff
from homework_hub.sources.base import (
    AuthExpiredError,
    SchemaBreakError,
    Source,
    TransientError,
)
from homework_hub.state.store import StateStore

# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class FakeSource(Source):
    def __init__(self, name: str, *, tasks=None, raises=None):
        self.name = name
        self._tasks = tasks or []
        self._raises = raises
        self.calls: list[str] = []

    def fetch(self, child: str) -> list[Task]:
        self.calls.append(child)
        if self._raises:
            raise self._raises
        return list(self._tasks)


class FakeSheetsBackend:
    def __init__(self, existing_rows=None):
        self.existing_rows = existing_rows or [["child", "source", "source_id"]]
        self.diffs_applied: list[tuple[str, RawDiff]] = []
        self.created: list[str] = []

    def create_sheet(self, title: str, *, share_with=None) -> str:
        self.created.append(title)
        return "fake-sheet-id"

    def read_raw_rows(self, spreadsheet_id: str) -> list[list[str]]:
        return self.existing_rows

    def apply_diff(self, spreadsheet_id: str, diff: RawDiff) -> None:
        self.diffs_applied.append((spreadsheet_id, diff))


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _task(
    child: str = "james",
    source: SourceEnum = SourceEnum.CLASSROOM,
    source_id: str = "abc",
    status: Status = Status.NOT_STARTED,
    due: datetime | None = None,
) -> Task:
    return Task(
        source=source,
        source_id=source_id,
        child=child,
        title="Maths Q1",
        subject="Maths",
        status=status,
        due_at=due or datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )


def _children_config(*names: str, sheet_id: str | None = "fake-sheet-id") -> ChildrenConfig:
    return ChildrenConfig(
        children={n: ChildConfig(display_name=n.title(), sheet_id=sheet_id) for n in names}
    )


@pytest.fixture
def state(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


# --------------------------------------------------------------------------- #
# Happy paths
# --------------------------------------------------------------------------- #


class TestHappyPath:
    def test_single_child_single_source(self, state: StateStore):
        src = FakeSource("classroom", tasks=[_task(source_id="a")])
        sheets = FakeSheetsBackend()
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [src]},
            sheets=sheets,
            state=state,
        )
        report = orch.run()

        assert len(report.children) == 1
        ch = report.children[0]
        assert ch.child == "james"
        assert len(ch.source_results) == 1
        assert ch.source_results[0].ok
        assert ch.source_results[0].task_count == 1
        assert ch.rows_appended == 1
        assert ch.rows_updated == 0
        assert len(ch.new_tasks) == 1

    def test_only_child_filter(self, state: StateStore):
        src_j = FakeSource("classroom", tasks=[_task(child="james", source_id="a")])
        src_t = FakeSource("classroom", tasks=[_task(child="tahlia", source_id="b")])
        sheets = FakeSheetsBackend()
        orch = Orchestrator(
            children_config=_children_config("james", "tahlia"),
            sources_for_child={"james": [src_j], "tahlia": [src_t]},
            sheets=sheets,
            state=state,
        )
        report = orch.run(only_child="james")

        assert [c.child for c in report.children] == ["james"]
        assert src_j.calls == ["james"]
        assert src_t.calls == []

    def test_unknown_only_child_raises(self, state: StateStore):
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": []},
            sheets=FakeSheetsBackend(),
            state=state,
        )
        with pytest.raises(KeyError, match="nobody"):
            orch.run(only_child="nobody")

    def test_multiple_sources_aggregated_per_child(self, state: StateStore):
        cl = FakeSource("classroom", tasks=[_task(source=SourceEnum.CLASSROOM, source_id="a")])
        cm = FakeSource("compass", tasks=[_task(source=SourceEnum.COMPASS, source_id="b")])
        sheets = FakeSheetsBackend()
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [cl, cm]},
            sheets=sheets,
            state=state,
        )
        report = orch.run()

        ch = report.children[0]
        assert {r.source for r in ch.source_results} == {"classroom", "compass"}
        assert ch.rows_appended == 2

    def test_overdue_recomputed_after_fetch(self, state: StateStore):
        # Task with past due date and not-submitted status should land as overdue.
        past_due = datetime(2020, 1, 1, tzinfo=UTC)
        src = FakeSource(
            "classroom",
            tasks=[_task(source_id="a", due=past_due, status=Status.NOT_STARTED)],
        )
        sheets = FakeSheetsBackend()
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [src]},
            sheets=sheets,
            state=state,
        )
        orch.run()

        # The applied diff should carry the overdue status.
        _, diff = sheets.diffs_applied[0]
        assert len(diff.appends) == 1
        # status column index is 8 in RAW_HEADERS
        assert diff.appends[0][8] == "overdue"


# --------------------------------------------------------------------------- #
# Failure handling
# --------------------------------------------------------------------------- #


class TestFailureHandling:
    def test_auth_expired_recorded_and_other_sources_continue(self, state: StateStore):
        cl = FakeSource("classroom", raises=AuthExpiredError("token rotted"))
        cm = FakeSource("compass", tasks=[_task(source_id="b")])
        sheets = FakeSheetsBackend()
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [cl, cm]},
            sheets=sheets,
            state=state,
        )
        report = orch.run()
        ch = report.children[0]

        results = {r.source: r for r in ch.source_results}
        assert results["classroom"].ok is False
        assert results["classroom"].failure_kind == "auth_expired"
        assert results["compass"].ok is True
        # Compass tasks still made it to the sheet.
        assert ch.rows_appended == 1

        auth = state.get_auth("james", "classroom")
        assert auth is not None
        assert auth.last_failure_kind == "auth_expired"

    def test_schema_break_classified_separately(self, state: StateStore):
        src = FakeSource("compass", raises=SchemaBreakError("payload changed"))
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [src]},
            sheets=FakeSheetsBackend(),
            state=state,
        )
        report = orch.run()
        assert report.children[0].source_results[0].failure_kind == "schema_break"

    def test_transient_classified_separately(self, state: StateStore):
        src = FakeSource("edrolo", raises=TransientError("timeout"))
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [src]},
            sheets=FakeSheetsBackend(),
            state=state,
        )
        report = orch.run()
        assert report.children[0].source_results[0].failure_kind == "transient"

    def test_all_sources_failed_skips_sheet_write(self, state: StateStore):
        cl = FakeSource("classroom", raises=AuthExpiredError("x"))
        cm = FakeSource("compass", raises=AuthExpiredError("y"))
        sheets = FakeSheetsBackend()
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [cl, cm]},
            sheets=sheets,
            state=state,
        )
        report = orch.run()

        assert sheets.diffs_applied == []  # no writes
        assert "All sources failed" in (report.children[0].sheet_skipped_reason or "")

    def test_partial_success_writes_to_sheet(self, state: StateStore):
        cl = FakeSource("classroom", tasks=[_task(source_id="a")])
        cm = FakeSource("compass", raises=AuthExpiredError("y"))
        sheets = FakeSheetsBackend()
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [cl, cm]},
            sheets=sheets,
            state=state,
        )
        orch.run()
        # Sheet write happens because at least one source produced tasks.
        assert len(sheets.diffs_applied) == 1


# --------------------------------------------------------------------------- #
# Sheet skipping when not bootstrapped
# --------------------------------------------------------------------------- #


class TestSheetBootstrapping:
    def test_no_sheet_id_skips_sheet_write_but_still_polls_sources(self, state: StateStore):
        src = FakeSource("classroom", tasks=[_task(source_id="a")])
        sheets = FakeSheetsBackend()
        orch = Orchestrator(
            children_config=_children_config("james", sheet_id=None),
            sources_for_child={"james": [src]},
            sheets=sheets,
            state=state,
        )
        report = orch.run()

        assert sheets.diffs_applied == []
        assert "bootstrap-sheet" in (report.children[0].sheet_skipped_reason or "")
        # Source was still polled and its success recorded.
        assert src.calls == ["james"]
        auth = state.get_auth("james", "classroom")
        assert auth is not None
        assert auth.last_success_at is not None


# --------------------------------------------------------------------------- #
# State integration
# --------------------------------------------------------------------------- #


class TestStateIntegration:
    def test_first_sync_marks_all_new(self, state: StateStore):
        src = FakeSource(
            "classroom",
            tasks=[_task(source_id="a"), _task(source_id="b")],
        )
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [src]},
            sheets=FakeSheetsBackend(),
            state=state,
        )
        report = orch.run()
        assert len(report.children[0].new_tasks) == 2
        assert len(report.children[0].changed_tasks) == 0

    def test_second_sync_with_status_change_marks_changed(self, state: StateStore):
        # First sync: not started.
        src1 = FakeSource("classroom", tasks=[_task(source_id="a", status=Status.NOT_STARTED)])
        orch1 = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [src1]},
            sheets=FakeSheetsBackend(),
            state=state,
        )
        orch1.run()

        # Second sync: now submitted.
        src2 = FakeSource("classroom", tasks=[_task(source_id="a", status=Status.SUBMITTED)])
        orch2 = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [src2]},
            sheets=FakeSheetsBackend(),
            state=state,
        )
        report = orch2.run()
        assert len(report.children[0].new_tasks) == 0
        assert len(report.children[0].changed_tasks) == 1


# --------------------------------------------------------------------------- #
# Human summary
# --------------------------------------------------------------------------- #


class TestSummarise:
    def test_summary_includes_each_child_and_source(self, state: StateStore):
        cl = FakeSource("classroom", tasks=[_task(source_id="a")])
        cm = FakeSource("compass", raises=AuthExpiredError("expired"))
        orch = Orchestrator(
            children_config=_children_config("james"),
            sources_for_child={"james": [cl, cm]},
            sheets=FakeSheetsBackend(),
            state=state,
        )
        report = orch.run()
        text = summarise_for_humans(report)
        assert "james" in text
        assert "classroom" in text
        assert "compass" in text
        assert "auth_expired" in text

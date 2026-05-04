"""Unit tests for ``MedallionOrchestrator`` (M6).

These exercise the orchestration glue with fake ``Source``s and a fake
``GoldSink``. The pipeline components themselves (BronzeWriter, SilverWriter,
LinkDetector, publish_for_child) are real, hitting an in-memory state.db so
we get true end-to-end behaviour for the medallion path without external
network calls.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from homework_hub.config import (
    ChildConfig,
    ChildrenConfig,
    ChildSources,
    SimpleSourceConfig,
)
from homework_hub.medallion_orchestrator import (
    MedallionOrchestrator,
    replay_silver_from_bronze,
    summarise_medallion,
)
from homework_hub.models import Task
from homework_hub.pipeline.ingest import RawRecord
from homework_hub.sources.base import AuthExpiredError, Source
from homework_hub.state.store import StateStore

# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class _FakeSource(Source):
    def __init__(self, name: str, records: list[RawRecord]):
        self.name = name
        self._records = records

    def fetch(self, child: str) -> list[Task]:  # pragma: no cover - unused path
        return []

    def fetch_raw(self, child: str) -> list[RawRecord]:
        return list(self._records)


class _AuthExpiredSource(Source):
    name = "compass"

    def fetch(self, child: str) -> list[Task]:  # pragma: no cover
        return []

    def fetch_raw(self, child: str) -> list[RawRecord]:
        raise AuthExpiredError("cookie expired")


class _SilentAuthExpiredSource(Source):
    """Simulates EP: silence_repeated_auth_expired=True, always raises AuthExpiredError."""

    name = "eduperfect"
    silence_repeated_auth_expired = True

    def fetch(self, child: str) -> list[Task]:  # pragma: no cover
        return []

    def fetch_raw(self, child: str) -> list[RawRecord]:
        raise AuthExpiredError("token expired")


class _RecordingSink:
    """Minimal GoldSink stub that records calls."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str, int]] = []
        self.hidden: dict[str, bool] = {}

    def read_user_edits(self, spreadsheet_id: str) -> list[Any]:
        return []

    def read_duplicate_checkboxes(self, spreadsheet_id: str) -> list[Any]:
        return []

    def read_tab_raw(self, spreadsheet_id: str, tab_name: str) -> list[list[str]]:
        return []

    def write_tab(self, spreadsheet_id: str, tab: Any, rows: list[Any]) -> None:
        self.writes.append((spreadsheet_id, tab.name, len(rows)))

    def set_tab_hidden(self, spreadsheet_id: str, tab: Any, hidden: bool) -> None:
        self.hidden[tab.name] = hidden


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _children_cfg(*, sheet_id: str | None = None) -> ChildrenConfig:
    return ChildrenConfig(
        children={
            "james": ChildConfig(
                display_name="James",
                sheet_id=sheet_id,
                sources=ChildSources(
                    classroom=SimpleSourceConfig(enabled=True),
                    edrolo=SimpleSourceConfig(enabled=True),
                ),
            )
        }
    )


def _classroom_payload(source_id: str = "abc1") -> dict[str, Any]:
    """Bronze-shaped Classroom payload accepted by ``bronze_to_silver_classroom``."""
    return {
        "view": "assigned",
        "base_url": "https://classroom.google.com",
        "card": {
            "course_id": "course-9maths",
            "stream_item_id": source_id,
            "title": "Algebra worksheet",
            "subject": "9 Maths",
            "href": f"/c/course-9maths/a/{source_id}/details",
            "due_or_status": "Due Fri, 1 May",
        },
    }


@pytest.fixture
def state(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


# --------------------------------------------------------------------------- #
# run() — full pipeline
# --------------------------------------------------------------------------- #


class TestRunFullPipeline:
    def test_run_no_sink_skips_publish_with_clear_reason(self, state: StateStore):
        rec = RawRecord(
            child="james",
            source="classroom",
            source_id="abc1",
            payload=_classroom_payload(),
        )
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={"james": [_FakeSource("classroom", [rec])]},
            state=state,
            sink=None,
        )
        report = orch.run()

        assert len(report.children) == 1
        c = report.children[0]
        assert c.child == "james"
        assert len(c.ingest) == 1
        assert c.ingest[0].ok is True
        assert c.ingest[0].bronze_inserted == 1
        assert c.transform is not None and c.transform.ok
        assert c.transform.inserted == 1
        assert c.detect is not None and c.detect.ok
        assert c.publish is not None and c.publish.ok
        assert c.publish.skipped_reason is not None
        assert "M5c" in c.publish.skipped_reason
        assert report.any_failures is False

    def test_run_no_sheet_id_skips_publish_with_bootstrap_hint(self, state: StateStore):
        rec = RawRecord(
            child="james",
            source="classroom",
            source_id="abc1",
            payload=_classroom_payload(),
        )
        sink = _RecordingSink()
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id=None),
            sources_for_child={"james": [_FakeSource("classroom", [rec])]},
            state=state,
            sink=sink,  # type: ignore[arg-type]
        )
        report = orch.run()
        c = report.children[0]
        assert c.publish is not None and c.publish.ok
        assert "bootstrap-sheet" in (c.publish.skipped_reason or "")
        assert sink.writes == []  # never called

    def test_run_with_sink_publishes_and_writes_tabs(self, state: StateStore):
        rec = RawRecord(
            child="james",
            source="classroom",
            source_id="abc1",
            payload=_classroom_payload(),
        )
        sink = _RecordingSink()
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={"james": [_FakeSource("classroom", [rec])]},
            state=state,
            sink=sink,  # type: ignore[arg-type]
        )
        report = orch.run()
        c = report.children[0]
        assert c.publish is not None and c.publish.ok
        assert c.publish.skipped_reason is None
        assert c.publish.tasks_written >= 1
        # Tabs written by publish_for_child: Tasks, Possible Duplicates,
        # Settings, UserEdits. (Today is formula-only, no rows.)
        titles_written = {w[1] for w in sink.writes}
        assert {"Tasks", "Settings"}.issubset(titles_written)

    def test_run_records_sync_rows_per_stage(self, state: StateStore):
        rec = RawRecord(
            child="james",
            source="classroom",
            source_id="abc1",
            payload=_classroom_payload(),
        )
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={"james": [_FakeSource("classroom", [rec])]},
            state=state,
            sink=None,
        )
        orch.run()
        rows = state.recent_sync_runs(child="james", limit=50)
        sources = [r["source"] for r in rows]
        # Ingest row + *transform + *detect + *publish (skipped)
        assert "classroom" in sources
        assert "*transform" in sources
        assert "*detect" in sources
        assert "*publish" in sources
        publish_row = next(r for r in rows if r["source"] == "*publish")
        assert publish_row["outcome"] == "skipped_no_sink"


# --------------------------------------------------------------------------- #
# Ingest failure handling
# --------------------------------------------------------------------------- #


class TestIngestFailures:
    def test_auth_expired_records_failure_and_continues(self, state: StateStore):
        ok_rec = RawRecord(
            child="james",
            source="classroom",
            source_id="abc1",
            payload=_classroom_payload(),
        )
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={
                "james": [
                    _AuthExpiredSource(),
                    _FakeSource("classroom", [ok_rec]),
                ]
            },
            state=state,
            sink=None,
        )
        report = orch.run()
        c = report.children[0]
        assert len(c.ingest) == 2
        compass_r = next(r for r in c.ingest if r.source == "compass")
        classroom_r = next(r for r in c.ingest if r.source == "classroom")
        assert compass_r.ok is False
        assert compass_r.failure_kind == "auth_expired"
        assert classroom_r.ok is True
        # Report.any_failures because one ingest failed
        assert report.any_failures is True
        # But transform still ran successfully against the good source
        assert c.transform is not None and c.transform.ok

    def test_silence_repeated_auth_expired_skips_on_second_run(self, state: StateStore):
        """After the first auth_expired failure, sources with silence_repeated_auth_expired
        are silently skipped on subsequent syncs — ok=True, skipped=True, no [FAIL]."""
        ok_rec = RawRecord(
            child="james",
            source="classroom",
            source_id="abc1",
            payload=_classroom_payload(),
        )
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={
                "james": [
                    _SilentAuthExpiredSource(),
                    _FakeSource("classroom", [ok_rec]),
                ]
            },
            state=state,
            sink=None,
        )

        # First run — records the failure as normal.
        report1 = orch.run()
        ep_r1 = next(r for r in report1.children[0].ingest if r.source == "eduperfect")
        assert ep_r1.ok is False
        assert ep_r1.failure_kind == "auth_expired"
        assert report1.any_failures is True

        # Second run — silently skipped, not a failure.
        report2 = orch.run()
        ep_r2 = next(r for r in report2.children[0].ingest if r.source == "eduperfect")
        assert ep_r2.ok is True
        assert ep_r2.skipped is True
        assert ep_r2.skip_reason is not None
        assert "refresh-ep" in ep_r2.skip_reason
        # Other sources still run normally.
        classroom_r = next(r for r in report2.children[0].ingest if r.source == "classroom")
        assert classroom_r.ok is True
        # Skipped ingest does NOT count as a failure.
        assert report2.any_failures is False

    def test_silence_skipped_when_success_resets_clock(self, state: StateStore):
        """After a successful ingest, the silence window resets and the source
        is attempted again (falls through to raise AuthExpiredError → [FAIL])."""
        ok_rec = RawRecord(
            child="james", source="classroom", source_id="a1", payload=_classroom_payload()
        )

        # Seed a prior success for eduperfect so last_success_at is populated,
        # then record a failure *after* it — same condition as a real refresh.
        state.record_success("james", "eduperfect")

        # Record a failure after the success — the source is now in "broken" state.
        state.record_failure("james", "eduperfect", kind="auth_expired", message="expired")

        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={
                "james": [_SilentAuthExpiredSource(), _FakeSource("classroom", [ok_rec])]
            },
            state=state,
            sink=None,
        )
        # Failure is newer than success → skipped.
        report = orch.run()
        ep_r = next(r for r in report.children[0].ingest if r.source == "eduperfect")
        assert ep_r.skipped is True

        # Now record a success (simulates token refresh).
        state.record_success("james", "eduperfect")

        # Next run: success is now newer than failure → not skipped → raises → [FAIL].
        report2 = orch.run()
        ep_r2 = next(r for r in report2.children[0].ingest if r.source == "eduperfect")
        assert ep_r2.skipped is False
        assert ep_r2.ok is False
        assert ep_r2.failure_kind == "auth_expired"

    def test_non_silent_source_auth_expired_still_fails_on_repeat(self, state: StateStore):
        """Sources without silence_repeated_auth_expired=True always show [FAIL]."""
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={"james": [_AuthExpiredSource()]},
            state=state,
            sink=None,
        )
        for _ in range(3):
            report = orch.run()
            compass_r = next(r for r in report.children[0].ingest if r.source == "compass")
            assert compass_r.ok is False
            assert compass_r.skipped is False
            assert compass_r.failure_kind == "auth_expired"
            assert report.any_failures is True


# --------------------------------------------------------------------------- #
# Stage-only entry points
# --------------------------------------------------------------------------- #


class TestStageEntryPoints:
    def test_ingest_only_skips_transform_detect_publish(self, state: StateStore):
        rec = RawRecord(
            child="james",
            source="classroom",
            source_id="abc1",
            payload=_classroom_payload(),
        )
        orch = MedallionOrchestrator(
            children_config=_children_cfg(),
            sources_for_child={"james": [_FakeSource("classroom", [rec])]},
            state=state,
        )
        report = orch.ingest_only()
        c = report.children[0]
        assert len(c.ingest) == 1 and c.ingest[0].ok
        assert c.transform is None
        assert c.detect is None
        assert c.publish is None

    def test_transform_only_runs_against_existing_bronze(self, state: StateStore):
        # Pre-populate bronze
        rec = RawRecord(
            child="james",
            source="classroom",
            source_id="abc1",
            payload=_classroom_payload(),
        )
        from homework_hub.pipeline.ingest import BronzeWriter

        BronzeWriter(state).write_many([rec])

        orch = MedallionOrchestrator(
            children_config=_children_cfg(),
            sources_for_child={"james": []},  # no sources fetched
            state=state,
        )
        report = orch.transform_only()
        c = report.children[0]
        assert c.ingest == []
        assert c.transform is not None and c.transform.ok
        assert c.transform.inserted == 1

    def test_publish_only_runs_detect_and_publish(self, state: StateStore):
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={"james": []},
            state=state,
            sink=None,
        )
        report = orch.publish_only()
        c = report.children[0]
        assert c.ingest == []
        assert c.transform is None
        assert c.detect is not None and c.detect.ok
        assert c.publish is not None
        assert c.publish.skipped_reason is not None


# --------------------------------------------------------------------------- #
# only_child filtering
# --------------------------------------------------------------------------- #


class TestOnlyChildFilter:
    def test_only_child_unknown_raises(self, state: StateStore):
        orch = MedallionOrchestrator(
            children_config=_children_cfg(),
            sources_for_child={"james": []},
            state=state,
        )
        with pytest.raises(KeyError):
            orch.run(only_child="ghost")

    def test_only_child_filters_to_one(self, state: StateStore):
        cfg = ChildrenConfig(
            children={
                "james": ChildConfig(display_name="James"),
                "tahlia": ChildConfig(display_name="Tahlia"),
            }
        )
        orch = MedallionOrchestrator(
            children_config=cfg,
            sources_for_child={"james": [], "tahlia": []},
            state=state,
        )
        report = orch.run(only_child="tahlia")
        assert [c.child for c in report.children] == ["tahlia"]


# --------------------------------------------------------------------------- #
# replay_silver_from_bronze
# --------------------------------------------------------------------------- #


class TestReplay:
    def test_replay_discovers_children_from_bronze(self, state: StateStore):
        from homework_hub.pipeline.ingest import BronzeWriter

        BronzeWriter(state).write_many(
            [
                RawRecord(
                    child="james",
                    source="classroom",
                    source_id="abc1",
                    payload=_classroom_payload("abc1"),
                ),
                RawRecord(
                    child="tahlia",
                    source="classroom",
                    source_id="xyz9",
                    payload=_classroom_payload("xyz9"),
                ),
            ]
        )
        results = replay_silver_from_bronze(state)
        assert set(results.keys()) == {"james", "tahlia"}
        assert all(r.ok for r in results.values())
        assert all(r.inserted == 1 for r in results.values())

    def test_replay_filters_by_only_child(self, state: StateStore):
        from homework_hub.pipeline.ingest import BronzeWriter

        BronzeWriter(state).write_many(
            [
                RawRecord(
                    child="james",
                    source="classroom",
                    source_id="abc1",
                    payload=_classroom_payload("abc1"),
                ),
                RawRecord(
                    child="tahlia",
                    source="classroom",
                    source_id="xyz9",
                    payload=_classroom_payload("xyz9"),
                ),
            ]
        )
        results = replay_silver_from_bronze(state, only_child="james")
        assert set(results.keys()) == {"james"}

    def test_replay_writes_sync_run_row(self, state: StateStore):
        from homework_hub.pipeline.ingest import BronzeWriter

        BronzeWriter(state).write_many(
            [
                RawRecord(
                    child="james",
                    source="classroom",
                    source_id="abc1",
                    payload=_classroom_payload(),
                ),
            ]
        )
        replay_silver_from_bronze(state)
        rows = state.recent_sync_runs(child="james")
        replay_rows = [r for r in rows if r["source"] == "*replay"]
        assert len(replay_rows) == 1
        assert replay_rows[0]["outcome"] == "ok"


# --------------------------------------------------------------------------- #
# summarise_medallion
# --------------------------------------------------------------------------- #


class TestSummariseMedallion:
    def test_summary_lists_all_stages(self, state: StateStore):
        rec = RawRecord(
            child="james",
            source="classroom",
            source_id="abc1",
            payload=_classroom_payload(),
        )
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={"james": [_FakeSource("classroom", [rec])]},
            state=state,
            sink=None,
        )
        report = orch.run()
        text = summarise_medallion(report)
        assert "Medallion sync completed" in text
        assert "ingest classroom" in text
        assert "transform" in text
        assert "detect" in text
        assert "publish" in text
        assert "[OK]" in text

    def test_summary_marks_failures(self, state: StateStore):
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={"james": [_AuthExpiredSource()]},
            state=state,
            sink=None,
        )
        report = orch.run()
        text = summarise_medallion(report)
        assert "[FAIL/auth_expired]" in text

    def test_summary_renders_skip_for_silenced_source(self, state: StateStore):
        """Silenced sources show [skip] ingest ... in the summary, not [FAIL]."""
        ok_rec = RawRecord(
            child="james", source="classroom", source_id="a1", payload=_classroom_payload()
        )
        orch = MedallionOrchestrator(
            children_config=_children_cfg(sheet_id="sheet-x"),
            sources_for_child={
                "james": [_SilentAuthExpiredSource(), _FakeSource("classroom", [ok_rec])]
            },
            state=state,
            sink=None,
        )
        orch.run()  # first run — records failure
        report2 = orch.run()  # second run — should be silenced
        text = summarise_medallion(report2)
        assert "[skip] ingest eduperfect" in text
        assert "[FAIL" not in text
        assert "failures: no" in text


# --------------------------------------------------------------------------- #
# EduPerfectSource silence flag
# --------------------------------------------------------------------------- #


class TestEduPerfectSilenceFlag:
    def test_eduperfect_source_opts_in_to_silence(self):
        from homework_hub.sources.eduperfect import EduPerfectSource

        assert EduPerfectSource.silence_repeated_auth_expired is True

    def test_other_sources_do_not_silence(self):
        from homework_hub.sources.classroom import ClassroomSource
        from homework_hub.sources.compass import CompassSource

        assert CompassSource.silence_repeated_auth_expired is False
        assert ClassroomSource.silence_repeated_auth_expired is False

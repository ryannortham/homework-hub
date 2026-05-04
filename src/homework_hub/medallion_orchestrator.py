"""Medallion orchestrator — wires bronze → silver → links → gold.

The legacy ``Orchestrator`` (in ``orchestrator.py``) writes through the
old ``apply_diff`` Raw-tab path; this module replaces it with the
medallion flow:

1. **Ingest**     — call ``source.fetch_raw(child)`` per enabled source,
                    write to ``bronze_records``.
2. **Transform**  — read latest bronze rows for the child, project to
                    canonical ``Task`` rows, upsert to ``silver_tasks``.
3. **Detect**     — re-run :class:`LinkDetector` against the now-fresh
                    silver layer.
4. **Publish**    — project silver into per-tab gold rows and write
                    through a :class:`GoldSink`. Skipped (with a clear
                    ``sync_runs`` row) when no sink is configured \u2014
                    M5c provides the real implementation.

Each step records one row per ``(child, source)`` to ``sync_runs`` so
the Settings tab and ``/health`` can surface operational status.

Failures isolate per source: an Edrolo auth-expired error must not
prevent the Compass + Classroom layers from publishing. Stage-level
failures (transform, detect, publish) record a single row with
``source='*'`` and an error string.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from homework_hub.config import ChildrenConfig
from homework_hub.models import Source as SourceEnum
from homework_hub.models import Task
from homework_hub.pipeline.ingest import BronzeWriter, IngestResult
from homework_hub.pipeline.link_detector import DetectionResult, LinkDetector
from homework_hub.pipeline.publish import GoldSink, PublishResult, publish_for_child
from homework_hub.pipeline.transform import (
    SilverWriter,
    TransformResult,
    bronze_to_silver_classroom,
    bronze_to_silver_compass,
    bronze_to_silver_eduperfect,
    bronze_to_silver_edrolo,
)
from homework_hub.sources.base import (
    AuthExpiredError,
    SchemaBreakError,
    Source,
    SourceError,
    TransientError,
)
from homework_hub.state.store import StateStore

log = logging.getLogger(__name__)

_BRONZE_TO_SILVER = {
    SourceEnum.COMPASS.value: bronze_to_silver_compass,
    SourceEnum.CLASSROOM.value: bronze_to_silver_classroom,
    SourceEnum.EDUPERFECT.value: bronze_to_silver_eduperfect,
    SourceEnum.EDROLO.value: bronze_to_silver_edrolo,
}


# --------------------------------------------------------------------------- #
# Result types
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class IngestStageResult:
    child: str
    source: str
    ok: bool
    skipped: bool = False
    skip_reason: str | None = None
    bronze_inserted: int = 0
    bronze_skipped: int = 0
    failure_kind: str | None = None
    failure_message: str | None = None


@dataclass(frozen=True)
class TransformStageResult:
    child: str
    ok: bool
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    error: str | None = None


@dataclass(frozen=True)
class DetectStageResult:
    child: str
    ok: bool
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    error: str | None = None


@dataclass(frozen=True)
class PublishStageResult:
    child: str
    ok: bool
    skipped_reason: str | None = None
    tasks_written: int = 0
    duplicates_written: int = 0
    duplicates_state_updates: int = 0
    user_edits_written: int = 0
    error: str | None = None


@dataclass
class MedallionChildReport:
    child: str
    ingest: list[IngestStageResult] = field(default_factory=list)
    transform: TransformStageResult | None = None
    detect: DetectStageResult | None = None
    publish: PublishStageResult | None = None

    @property
    def ok(self) -> bool:
        if self.transform and not self.transform.ok:
            return False
        if self.detect and not self.detect.ok:
            return False
        if self.publish and not self.publish.ok:
            return False
        # An ingest source failing isn't fatal for the run \u2014 the others
        # still publish \u2014 but it does mark the report as having failures.
        return all(r.ok for r in self.ingest)


@dataclass
class MedallionSyncReport:
    started_at: datetime
    finished_at: datetime
    children: list[MedallionChildReport] = field(default_factory=list)

    @property
    def any_failures(self) -> bool:
        return any(not c.ok for c in self.children)


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #


class MedallionOrchestrator:
    """Runs the medallion sync pipeline end to end."""

    def __init__(
        self,
        *,
        children_config: ChildrenConfig,
        sources_for_child: dict[str, list[Source]],
        state: StateStore,
        sink: GoldSink | None = None,
    ):
        self.children_config = children_config
        self.sources_for_child = sources_for_child
        self.state = state
        self.sink = sink
        self._bronze = BronzeWriter(state)
        self._silver = SilverWriter(state)
        self._link_detector = LinkDetector(state)

    # ------------------------------------------------------------------ #
    # Entry points
    # ------------------------------------------------------------------ #

    def run(self, *, only_child: str | None = None) -> MedallionSyncReport:
        started = datetime.now(UTC)
        children: list[MedallionChildReport] = []
        for child in self._resolve_targets(only_child):
            children.append(self._run_for_child(child))
        return MedallionSyncReport(
            started_at=started,
            finished_at=datetime.now(UTC),
            children=children,
        )

    def ingest_only(self, *, only_child: str | None = None) -> MedallionSyncReport:
        """Run just the ingest stage. Useful for the ``ingest`` CLI verb."""
        started = datetime.now(UTC)
        children: list[MedallionChildReport] = []
        for child in self._resolve_targets(only_child):
            report = MedallionChildReport(child=child)
            report.ingest = self._stage_ingest(child)
            children.append(report)
        return MedallionSyncReport(started, datetime.now(UTC), children)

    def transform_only(self, *, only_child: str | None = None) -> MedallionSyncReport:
        """Run just the transform stage \u2014 reads existing bronze rows."""
        started = datetime.now(UTC)
        children: list[MedallionChildReport] = []
        for child in self._resolve_targets(only_child):
            report = MedallionChildReport(child=child)
            report.transform = self._stage_transform(child)
            children.append(report)
        return MedallionSyncReport(started, datetime.now(UTC), children)

    def publish_only(self, *, only_child: str | None = None) -> MedallionSyncReport:
        """Run just detect + publish stages."""
        started = datetime.now(UTC)
        children: list[MedallionChildReport] = []
        for child in self._resolve_targets(only_child):
            report = MedallionChildReport(child=child)
            report.detect = self._stage_detect(child)
            report.publish = self._stage_publish(child)
            children.append(report)
        return MedallionSyncReport(started, datetime.now(UTC), children)

    # ------------------------------------------------------------------ #
    # Per-child run
    # ------------------------------------------------------------------ #

    def _run_for_child(self, child: str) -> MedallionChildReport:
        report = MedallionChildReport(child=child)
        report.ingest = self._stage_ingest(child)
        report.transform = self._stage_transform(child)
        report.detect = self._stage_detect(child)
        report.publish = self._stage_publish(child)
        return report

    # ------------------------------------------------------------------ #
    # Stage: ingest
    # ------------------------------------------------------------------ #

    def _stage_ingest(self, child: str) -> list[IngestStageResult]:
        results: list[IngestStageResult] = []
        for source in self.sources_for_child.get(child, []):
            results.append(self._ingest_one(child, source))
        return results

    def _ingest_one(self, child: str, source: Source) -> IngestStageResult:
        started = datetime.now(UTC)

        # Sources with structurally short-lived tokens (e.g. EP ~30 min JWTs)
        # opt in to silence_repeated_auth_expired. After the first auth_expired
        # failure, skip silently until a successful ingest resets the clock.
        # This prevents hourly [FAIL] noise for an expected condition while still
        # preserving last known silver data in the sheet.
        if source.silence_repeated_auth_expired:
            auth = self.state.get_auth(child, source.name)
            if auth is not None and auth.last_failure_kind == "auth_expired":
                last_fail = auth.last_failure_at
                last_ok = auth.last_success_at
                if last_fail is not None and (last_ok is None or last_fail > last_ok):
                    return IngestStageResult(
                        child=child,
                        source=source.name,
                        ok=True,
                        skipped=True,
                        skip_reason=(
                            f"token expired — run "
                            f"`homework-hub refresh-ep --child {child}` to refresh"
                        ),
                    )

        try:
            records = source.fetch_raw(child)
        except AuthExpiredError as exc:
            return self._record_ingest_failure(
                child, source.name, "auth_expired", str(exc), started
            )
        except SchemaBreakError as exc:
            return self._record_ingest_failure(
                child, source.name, "schema_break", str(exc), started
            )
        except TransientError as exc:
            return self._record_ingest_failure(child, source.name, "transient", str(exc), started)
        except SourceError as exc:
            log.exception("unhandled source error for %s/%s", child, source.name)
            return self._record_ingest_failure(
                child, source.name, "schema_break", str(exc), started
            )
        except NotImplementedError as exc:
            # A source hasn't implemented fetch_raw() yet \u2014 surface clearly
            # rather than crashing the whole run.
            return self._record_ingest_failure(
                child, source.name, "not_implemented", str(exc), started
            )

        result: IngestResult = self._bronze.write_many(records)
        self.state.record_sync_run(
            child=child,
            source=source.name,
            outcome="ok",
            started_at=started,
            finished_at=datetime.now(UTC),
            bronze_inserted=result.inserted,
        )
        self.state.record_success(child, source.name)
        return IngestStageResult(
            child=child,
            source=source.name,
            ok=True,
            bronze_inserted=result.inserted,
            bronze_skipped=result.skipped,
        )

    def _record_ingest_failure(
        self,
        child: str,
        source: str,
        kind: str,
        message: str,
        started: datetime,
    ) -> IngestStageResult:
        log.warning("ingest %s for %s/%s: %s", kind, child, source, message)
        self.state.record_failure(child, source, kind=kind, message=message)
        self.state.record_sync_run(
            child=child,
            source=source,
            outcome=kind,
            started_at=started,
            finished_at=datetime.now(UTC),
            error=message,
        )
        return IngestStageResult(
            child=child,
            source=source,
            ok=False,
            failure_kind=kind,
            failure_message=message,
        )

    # ------------------------------------------------------------------ #
    # Stage: transform
    # ------------------------------------------------------------------ #

    def _stage_transform(self, child: str) -> TransformStageResult:
        started = datetime.now(UTC)
        try:
            rows: list[tuple[Task, int | None]] = []
            for source_value in _BRONZE_TO_SILVER:
                latest = self._bronze.latest_for(child, source_value)
                adapter = _BRONZE_TO_SILVER[source_value]
                for bronze_id, _source_id, payload, _fetched_at in latest:
                    try:
                        task = adapter(child=child, payload=payload)
                    except Exception as exc:
                        # One bad bronze row mustn't halt the whole transform.
                        log.warning(
                            "skip bronze id=%s for %s/%s: %s",
                            bronze_id,
                            child,
                            source_value,
                            exc,
                        )
                        continue
                    rows.append((task, bronze_id))
            tr: TransformResult = self._silver.upsert_many(rows)
        except Exception as exc:
            log.exception("transform failed for %s", child)
            self.state.record_sync_run(
                child=child,
                source="*transform",
                outcome="error",
                started_at=started,
                finished_at=datetime.now(UTC),
                error=str(exc),
            )
            return TransformStageResult(child=child, ok=False, error=str(exc))

        self.state.record_sync_run(
            child=child,
            source="*transform",
            outcome="ok",
            started_at=started,
            finished_at=datetime.now(UTC),
            silver_upserted=tr.inserted + tr.updated,
        )
        return TransformStageResult(
            child=child,
            ok=True,
            inserted=tr.inserted,
            updated=tr.updated,
            unchanged=tr.unchanged,
        )

    # ------------------------------------------------------------------ #
    # Stage: detect
    # ------------------------------------------------------------------ #

    def _stage_detect(self, child: str) -> DetectStageResult:
        started = datetime.now(UTC)
        try:
            dr: DetectionResult = self._link_detector.detect(child)
        except Exception as exc:
            log.exception("link detection failed for %s", child)
            self.state.record_sync_run(
                child=child,
                source="*detect",
                outcome="error",
                started_at=started,
                finished_at=datetime.now(UTC),
                error=str(exc),
            )
            return DetectStageResult(child=child, ok=False, error=str(exc))

        self.state.record_sync_run(
            child=child,
            source="*detect",
            outcome="ok",
            started_at=started,
            finished_at=datetime.now(UTC),
        )
        return DetectStageResult(
            child=child,
            ok=True,
            inserted=dr.inserted,
            updated=dr.updated,
            unchanged=dr.unchanged,
        )

    # ------------------------------------------------------------------ #
    # Stage: publish
    # ------------------------------------------------------------------ #

    def _stage_publish(self, child: str) -> PublishStageResult:
        started = datetime.now(UTC)

        if self.sink is None:
            self.state.record_sync_run(
                child=child,
                source="*publish",
                outcome="skipped_no_sink",
                started_at=started,
                finished_at=datetime.now(UTC),
            )
            return PublishStageResult(
                child=child,
                ok=True,
                skipped_reason="no GoldSink configured (M5c pending)",
            )

        cfg = self.children_config.children[child]
        if cfg.sheet_id is None:
            self.state.record_sync_run(
                child=child,
                source="*publish",
                outcome="skipped_no_sheet_id",
                started_at=started,
                finished_at=datetime.now(UTC),
            )
            return PublishStageResult(
                child=child,
                ok=True,
                skipped_reason=(
                    f"No sheet_id in children.yaml \u2014 run "
                    f"`homework-hub bootstrap-sheet --child {child}`"
                ),
            )

        try:
            tasks = self._silver.all_for_child(child)
            last_synced = datetime.now(UTC)
            pr: PublishResult = publish_for_child(
                self.state,
                self.sink,
                child=child,
                spreadsheet_id=cfg.sheet_id,
                tasks=tasks,
                last_synced=last_synced,
            )
        except Exception as exc:
            log.exception("publish failed for %s", child)
            self.state.record_sync_run(
                child=child,
                source="*publish",
                outcome="error",
                started_at=started,
                finished_at=datetime.now(UTC),
                error=str(exc),
            )
            return PublishStageResult(child=child, ok=False, error=str(exc))

        self.state.record_sync_run(
            child=child,
            source="*publish",
            outcome="ok",
            started_at=started,
            finished_at=datetime.now(UTC),
        )
        return PublishStageResult(
            child=child,
            ok=True,
            tasks_written=pr.tasks_written,
            duplicates_written=pr.duplicates_written,
            duplicates_state_updates=pr.duplicates_state_updates,
            user_edits_written=pr.user_edits_written,
        )

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _resolve_targets(self, only_child: str | None) -> list[str]:
        all_children = list(self.children_config.children.keys())
        if only_child is None:
            return all_children
        if only_child not in self.children_config.children:
            raise KeyError(f"Unknown child '{only_child}'. Known: {all_children}")
        return [only_child]


# --------------------------------------------------------------------------- #
# Replay helpers
# --------------------------------------------------------------------------- #


def replay_silver_from_bronze(state: StateStore, *, only_child: str | None = None) -> dict:
    """Re-run the transform stage against current bronze for a child.

    Returns ``{child: TransformStageResult}``. Used by the ``replay`` CLI
    verb to re-canonicalise after a subject-rule change without re-fetching.
    """
    bronze = BronzeWriter(state)
    silver = SilverWriter(state)

    # Discover children currently in bronze (the children.yaml may not be
    # the source of truth for replay if a child has been removed).
    import sqlite3
    from contextlib import closing

    with closing(sqlite3.connect(state.db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT DISTINCT child FROM bronze_records").fetchall()
    targets: Iterable[str] = [only_child] if only_child else [r["child"] for r in rows]

    out: dict[str, TransformStageResult] = {}
    for child in targets:
        started = datetime.now(UTC)
        try:
            rows_to_upsert: list[tuple[Task, int | None]] = []
            for source_value, adapter in _BRONZE_TO_SILVER.items():
                for bronze_id, _sid, payload, _fa in bronze.latest_for(child, source_value):
                    try:
                        rows_to_upsert.append((adapter(child=child, payload=payload), bronze_id))
                    except Exception as exc:
                        log.warning(
                            "replay: skip bronze %s/%s id=%s: %s",
                            child,
                            source_value,
                            bronze_id,
                            exc,
                        )
            tr = silver.upsert_many(rows_to_upsert)
        except Exception as exc:
            log.exception("replay failed for %s", child)
            state.record_sync_run(
                child=child,
                source="*replay",
                outcome="error",
                started_at=started,
                finished_at=datetime.now(UTC),
                error=str(exc),
            )
            out[child] = TransformStageResult(child=child, ok=False, error=str(exc))
            continue

        state.record_sync_run(
            child=child,
            source="*replay",
            outcome="ok",
            started_at=started,
            finished_at=datetime.now(UTC),
            silver_upserted=tr.inserted + tr.updated,
        )
        out[child] = TransformStageResult(
            child=child,
            ok=True,
            inserted=tr.inserted,
            updated=tr.updated,
            unchanged=tr.unchanged,
        )
    return out


# --------------------------------------------------------------------------- #
# Human-readable summary
# --------------------------------------------------------------------------- #


def summarise_medallion(report: MedallionSyncReport) -> str:
    duration = (report.finished_at - report.started_at).total_seconds()
    lines = [
        f"Medallion sync completed in {duration:.1f}s "
        f"(failures: {'yes' if report.any_failures else 'no'})"
    ]
    for c in report.children:
        lines.append(f"  {c.child}:")
        for r in c.ingest:
            if r.skipped:
                lines.append(
                    f"    [skip] ingest {r.source}: {r.skip_reason}"
                )
            elif r.ok:
                lines.append(
                    f"    [OK]   ingest {r.source}: "
                    f"+{r.bronze_inserted} bronze ({r.bronze_skipped} skipped)"
                )
            else:
                lines.append(
                    f"    [FAIL/{r.failure_kind}] ingest {r.source}: " f"{r.failure_message}"
                )
        if c.transform:
            t = c.transform
            if t.ok:
                lines.append(
                    f"    [OK]   transform: +{t.inserted} new, "
                    f"~{t.updated} changed, ={t.unchanged} unchanged"
                )
            else:
                lines.append(f"    [FAIL] transform: {t.error}")
        if c.detect:
            d = c.detect
            if d.ok:
                lines.append(
                    f"    [OK]   detect: +{d.inserted} new link(s), "
                    f"~{d.updated}, ={d.unchanged}"
                )
            else:
                lines.append(f"    [FAIL] detect: {d.error}")
        if c.publish:
            p = c.publish
            if p.skipped_reason:
                lines.append(f"    [skip] publish: {p.skipped_reason}")
            elif p.ok:
                lines.append(
                    f"    [OK]   publish: {p.tasks_written} task(s), "
                    f"{p.duplicates_written} dup(s), "
                    f"{p.duplicates_state_updates} state update(s)"
                )
            else:
                lines.append(f"    [FAIL] publish: {p.error}")
    return "\n".join(lines)


__all__ = [
    "DetectStageResult",
    "IngestStageResult",
    "MedallionChildReport",
    "MedallionOrchestrator",
    "MedallionSyncReport",
    "PublishStageResult",
    "TransformStageResult",
    "replay_silver_from_bronze",
    "summarise_medallion",
]

"""Medallion orchestrator — wires bronze → silver → gold.

The legacy ``Orchestrator`` (in ``orchestrator.py``) writes through the
old ``apply_diff`` Raw-tab path; this module replaces it with the
medallion flow:

1. **Ingest**     — call ``source.fetch_raw(child)`` per enabled source,
                    write to ``bronze_records``.
2. **Transform**  — read latest bronze rows for the child, project to
                    canonical ``Task`` rows, upsert to ``silver_tasks``.
3. **Publish**    — project silver into per-tab gold rows and write
                    through a :class:`GoldSink`. Skipped (with a clear
                    ``sync_runs`` row) when no sink is configured —
                    M5c provides the real implementation.

Each step records one row per ``(child, source)`` to ``sync_runs`` so
the Settings tab and ``/health`` can surface operational status.

Failures isolate per source: an Edrolo auth-expired error must not
prevent the Compass + Classroom layers from publishing. Stage-level
failures (transform, publish) record a single row with
``source='*'`` and an error string.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from homework_hub.config import ChildrenConfig, Settings
from homework_hub.models import Source as SourceEnum
from homework_hub.models import Task
from homework_hub.pipeline.auth_status import build_source_auth_rows
from homework_hub.pipeline.ingest import BronzeWriter, IngestResult
from homework_hub.pipeline.publish import GoldSink, PublishResult, publish_for_child
from homework_hub.pipeline.reconcile import apply_age_cap, reconcile_stale
from homework_hub.pipeline.transform import (
    SilverWriter,
    TransformResult,
    bronze_to_silver_classroom,
    bronze_to_silver_compass,
    bronze_to_silver_edrolo,
    bronze_to_silver_eduperfect,
)
from homework_hub.sources.base import (
    AuthExpiredError,
    SchemaBreakError,
    Source,
    SourceError,
    TransientError,
)
from homework_hub.state.store import StateStore

if TYPE_CHECKING:
    from homework_hub.sources.workplan import WorkplanFetcher

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
class PublishStageResult:
    child: str
    ok: bool
    skipped_reason: str | None = None
    tasks_written: int = 0
    history_written: int = 0
    user_edits_written: int = 0
    error: str | None = None


@dataclass
class MedallionChildReport:
    child: str
    ingest: list[IngestStageResult] = field(default_factory=list)
    transform: TransformStageResult | None = None
    publish: PublishStageResult | None = None

    @property
    def ok(self) -> bool:
        if self.transform and not self.transform.ok:
            return False
        if self.publish and not self.publish.ok:
            return False
        # An ingest source failing isn't fatal for the run — the others
        # still publish — but it does mark the report as having failures.
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
        history_cutoff_days: int = 30,
        active_cutoff_days: int = 60,
        stale_grace_syncs: int = 2,
        future_date_cap_days: int = 365,
        settings: Settings | None = None,
        workplan_fetcher: WorkplanFetcher | None = None,
    ):
        self.children_config = children_config
        self.sources_for_child = sources_for_child
        self.state = state
        self.sink = sink
        self.history_cutoff_days = history_cutoff_days
        self.settings = settings
        # Cleansing knobs. When ``settings`` is supplied, prefer its values
        # so the wiring layer is the single source of truth.
        self._active_cutoff_days = (
            settings.active_cutoff_days if settings is not None else active_cutoff_days
        )
        self._stale_grace_syncs = (
            settings.stale_grace_syncs if settings is not None else stale_grace_syncs
        )
        self._future_date_cap_days = (
            settings.future_date_cap_days if settings is not None else future_date_cap_days
        )
        self._bronze = BronzeWriter(state)
        self._silver = SilverWriter(state)
        # Optional Student Workplan Tracker fetcher — runs after transform so
        # its rows survive the bronze-replay reset; failures are isolated by
        # the orchestrator hook below.
        self._workplan_fetcher = workplan_fetcher
        # Per-run scratchpad mapping ``source.name -> list[silver_source_id]``
        # for the currently-running child. Populated by ``_ingest_one`` on
        # successful ingest; consumed by ``_run_for_child`` to drive
        # ``reconcile_stale`` after the transform stage (which would otherwise
        # mask upstream disappearance by re-writing silver from the bronze
        # back-catalogue and resetting ``missing_streak`` to 0).
        self._current_run_seen: dict[str, list[str]] = {}

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
        """Run just the transform stage — reads existing bronze rows."""
        started = datetime.now(UTC)
        children: list[MedallionChildReport] = []
        for child in self._resolve_targets(only_child):
            report = MedallionChildReport(child=child)
            report.transform = self._stage_transform(child)
            children.append(report)
        return MedallionSyncReport(started, datetime.now(UTC), children)

    def publish_only(self, *, only_child: str | None = None) -> MedallionSyncReport:
        """Run just the publish stage."""
        started = datetime.now(UTC)
        children: list[MedallionChildReport] = []
        for child in self._resolve_targets(only_child):
            report = MedallionChildReport(child=child)
            report.publish = self._stage_publish(child)
            children.append(report)
        return MedallionSyncReport(started, datetime.now(UTC), children)

    # ------------------------------------------------------------------ #
    # Per-child run
    # ------------------------------------------------------------------ #

    def _run_for_child(self, child: str) -> MedallionChildReport:
        report = MedallionChildReport(child=child)
        # Reset per-run scratchpad: ``_ingest_one`` will populate it for
        # each source that succeeds, and reconcile reads it below.
        self._current_run_seen = {}
        report.ingest = self._stage_ingest(child)
        report.transform = self._stage_transform(child)
        # Upstream-disappearance reconciliation runs AFTER transform — the
        # transform stage re-writes silver from bronze's back-catalogue and
        # resets ``missing_streak`` to 0, so we have to bump streaks here
        # using the seen-ids stashed by ``_ingest_one`` for sources whose
        # ingest succeeded this run. Best-effort; never aborts publish.
        for source_name, seen_ids in self._current_run_seen.items():
            try:
                reconcile_stale(
                    self.state,
                    child=child,
                    source=source_name,
                    seen_ids=seen_ids,
                    grace_syncs=self._stale_grace_syncs,
                )
            except Exception:
                log.exception("reconcile_stale failed for %s/%s", child, source_name)
        # Age-cap sweep — archives stale Overdue rows and date-less zombies
        # whose anchor (due_at or first_seen_at) is older than the cutoff.
        # Best-effort; failure here doesn't abort publish.
        try:
            apply_age_cap(
                self.state,
                child=child,
                cutoff_days=self._active_cutoff_days,
            )
        except Exception:
            log.exception("apply_age_cap failed for %s", child)
        # Workplan hook — fetches Student Workplan Tracker Forms and writes
        # directly to silver. Wrapped so any failure stays isolated from the
        # regular sync stages.
        if self._workplan_fetcher is not None:
            try:
                self._workplan_fetcher.fetch_one_child(child)
            except Exception:
                log.exception("workplan fetch failed for %s", child)
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
        #
        # If the source reports a token refresh newer than the last failure
        # (via ``token_refreshed_at``), bypass the silence and attempt the
        # ingest — the operator has just refreshed credentials.
        if source.silence_repeated_auth_expired:
            auth = self.state.get_auth(child, source.name)
            if auth is not None and auth.last_failure_kind == "auth_expired":
                last_fail = auth.last_failure_at
                last_ok = auth.last_success_at
                if last_fail is not None and (last_ok is None or last_fail > last_ok):
                    refreshed = source.token_refreshed_at(child)
                    if refreshed is None or refreshed <= last_fail:
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
            # A source hasn't implemented fetch_raw() yet — surface clearly
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

        # Stash the silver source_ids that came back THIS sync so the
        # per-child driver can run ``reconcile_stale`` after the transform
        # stage. We can't reconcile here because the transform stage also
        # re-writes silver from the bronze back-catalogue, which would reset
        # ``missing_streak`` and mask any disappearance.
        #
        # ``RawRecord.source_id`` is the upstream's bronze key — which is
        # NOT always the silver source_id (e.g. Classroom silver_id is
        # ``course_id:stream_item_id``, derived only by the bronze→silver
        # mapper). Run the mapper here so the seen-set matches silver.
        adapter = _BRONZE_TO_SILVER.get(source.name)
        seen_ids: list[str] = []
        if adapter is not None:
            for rec in records:
                try:
                    task = adapter(child=child, payload=rec.payload)
                except Exception:
                    # A single bad payload mustn't poison the seen set —
                    # those rows will simply have streaks incremented and
                    # eventually archive themselves if they stay broken.
                    continue
                seen_ids.append(task.source_id)
        self._current_run_seen[source.name] = sorted(set(seen_ids))

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
                    f"No sheet_id in children.yaml — run "
                    f"`homework-hub bootstrap-sheet --child {child}`"
                ),
            )

        try:
            tasks = self._silver.all_for_child(child)
            last_synced = datetime.now(UTC)
            source_auth_rows = None
            if self.settings is not None:
                source_auth_rows = build_source_auth_rows(
                    child=child,
                    child_cfg=cfg,
                    settings=self.settings,
                    state=self.state,
                    now=last_synced,
                )
            pr: PublishResult = publish_for_child(
                self.state,
                self.sink,
                child=child,
                spreadsheet_id=cfg.sheet_id,
                tasks=tasks,
                last_synced=last_synced,
                cutoff_days=self.history_cutoff_days,
                future_date_cap_days=self._future_date_cap_days,
                source_auth_rows=source_auth_rows,
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
            history_written=pr.history_written,
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
                lines.append(f"    [skip] ingest {r.source}: {r.skip_reason}")
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
        if c.publish:
            p = c.publish
            if p.skipped_reason:
                lines.append(f"    [skip] publish: {p.skipped_reason}")
            elif p.ok:
                lines.append(
                    f"    [OK]   publish: {p.tasks_written} task(s), "
                    f"{p.history_written} history, "
                    f"{p.user_edits_written} edit(s)"
                )
            else:
                lines.append(f"    [FAIL] publish: {p.error}")
    return "\n".join(lines)


__all__ = [
    "IngestStageResult",
    "MedallionChildReport",
    "MedallionOrchestrator",
    "MedallionSyncReport",
    "PublishStageResult",
    "TransformStageResult",
    "replay_silver_from_bronze",
    "summarise_medallion",
]

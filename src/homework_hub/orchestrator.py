"""Sync orchestrator — wires sources, state DB, and the Sheets sink together.

For each enabled (child, source) pair:

1. Run ``source.fetch(child)`` to get tasks.
2. On AuthExpiredError / SchemaBreakError / TransientError, record the
   failure to the state DB and continue (one source failing must not block
   the others).
3. On success, record the success in the state DB.

After every source has been polled for a given child:

4. Read the current Raw tab rows from that child's sheet.
5. Compute a positional RawDiff (preserving row order so kids' Notes stay
   anchored).
6. Apply the diff via the SheetsBackend.
7. Update ``seen_tasks`` so we know which tasks are new for any future
   notification phase.

Returns a structured ``SyncReport`` summarising what happened — used by the
``sync`` and ``status`` CLI commands and (later) the daemon's logs.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from homework_hub.config import ChildrenConfig
from homework_hub.models import Task
from homework_hub.sinks.sheets_client import SheetsBackend
from homework_hub.sinks.sheets_diff import compute_raw_diff
from homework_hub.sources.base import (
    AuthExpiredError,
    SchemaBreakError,
    Source,
    SourceError,
    TransientError,
)
from homework_hub.state.store import StateStore, UpsertResult

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Result types
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SourceResult:
    child: str
    source: str
    ok: bool
    failure_kind: str | None = None
    failure_message: str | None = None
    task_count: int = 0


@dataclass
class ChildReport:
    child: str
    source_results: list[SourceResult] = field(default_factory=list)
    sheet_id: str | None = None
    sheet_skipped_reason: str | None = None
    rows_updated: int = 0
    rows_appended: int = 0
    rows_unchanged: int = 0
    new_tasks: list[Task] = field(default_factory=list)
    changed_tasks: list[Task] = field(default_factory=list)


@dataclass
class SyncReport:
    started_at: datetime
    finished_at: datetime
    children: list[ChildReport] = field(default_factory=list)

    @property
    def any_failures(self) -> bool:
        return any(not r.ok for c in self.children for r in c.source_results)


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #


class Orchestrator:
    """Runs a single sync pass across one or more children."""

    def __init__(
        self,
        *,
        children_config: ChildrenConfig,
        sources_for_child: dict[str, list[Source]],
        sheets: SheetsBackend,
        state: StateStore,
    ):
        self.children_config = children_config
        self.sources_for_child = sources_for_child
        self.sheets = sheets
        self.state = state

    def run(self, *, only_child: str | None = None) -> SyncReport:
        started = datetime.now(UTC)
        children: list[ChildReport] = []

        target_children = self._resolve_targets(only_child)

        for child in target_children:
            children.append(self._run_for_child(child))

        return SyncReport(started_at=started, finished_at=datetime.now(UTC), children=children)

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _resolve_targets(self, only_child: str | None) -> list[str]:
        all_children = list(self.children_config.children.keys())
        if only_child is None:
            return all_children
        if only_child not in self.children_config.children:
            raise KeyError(f"Unknown child '{only_child}'. Known: {all_children}")
        return [only_child]

    def _run_for_child(self, child: str) -> ChildReport:
        report = ChildReport(child=child)
        all_tasks: list[Task] = []

        for source in self.sources_for_child.get(child, []):
            tasks, result = self._run_source(child, source)
            report.source_results.append(result)
            all_tasks.extend(tasks)

        cfg = self.children_config.children[child]
        report.sheet_id = cfg.sheet_id

        if cfg.sheet_id is None:
            report.sheet_skipped_reason = (
                "No sheet_id in children.yaml — run "
                f"`homework-hub bootstrap-sheet --child {child}`"
            )
            log.warning("Skipping sheet write for %s: %s", child, report.sheet_skipped_reason)
        elif not all_tasks and self._all_sources_failed(report):
            report.sheet_skipped_reason = (
                "All sources failed; skipping sheet write to avoid clobbering."
            )
            log.warning("Skipping sheet write for %s: all sources failed", child)
        else:
            self._write_to_sheet(cfg.sheet_id, all_tasks, report)

        # Record dedup state regardless of sheet outcome — we still saw the
        # tasks even if the write failed; future syncs will retry the write.
        upsert = self.state.upsert_seen(all_tasks)
        report.new_tasks = upsert.new
        report.changed_tasks = upsert.changed

        return report

    def _run_source(self, child: str, source: Source) -> tuple[list[Task], SourceResult]:
        try:
            tasks = source.fetch(child)
        except AuthExpiredError as exc:
            log.warning("auth_expired for %s/%s: %s", child, source.name, exc)
            self.state.record_failure(child, source.name, kind="auth_expired", message=str(exc))
            return [], SourceResult(
                child=child,
                source=source.name,
                ok=False,
                failure_kind="auth_expired",
                failure_message=str(exc),
            )
        except SchemaBreakError as exc:
            log.error("schema_break for %s/%s: %s", child, source.name, exc)
            self.state.record_failure(child, source.name, kind="schema_break", message=str(exc))
            return [], SourceResult(
                child=child,
                source=source.name,
                ok=False,
                failure_kind="schema_break",
                failure_message=str(exc),
            )
        except TransientError as exc:
            log.info("transient for %s/%s: %s", child, source.name, exc)
            self.state.record_failure(child, source.name, kind="transient", message=str(exc))
            return [], SourceResult(
                child=child,
                source=source.name,
                ok=False,
                failure_kind="transient",
                failure_message=str(exc),
            )
        except SourceError as exc:
            # Catch-all for any subclass we forgot to handle explicitly.
            log.exception("unhandled source error for %s/%s", child, source.name)
            self.state.record_failure(child, source.name, kind="schema_break", message=str(exc))
            return [], SourceResult(
                child=child,
                source=source.name,
                ok=False,
                failure_kind="schema_break",
                failure_message=str(exc),
            )

        # Apply the OVERDUE recompute now (after fetch, before sheet write).
        tasks = [t.with_overdue_check() for t in tasks]
        self.state.record_success(child, source.name)
        return tasks, SourceResult(child=child, source=source.name, ok=True, task_count=len(tasks))

    @staticmethod
    def _all_sources_failed(report: ChildReport) -> bool:
        return bool(report.source_results) and all(not r.ok for r in report.source_results)

    def _write_to_sheet(self, sheet_id: str, tasks: Iterable[Task], report: ChildReport) -> None:
        existing = self.sheets.read_raw_rows(sheet_id)
        diff = compute_raw_diff(existing_rows=existing, incoming=list(tasks))
        self.sheets.apply_diff(sheet_id, diff)
        report.rows_updated = len(diff.updates)
        report.rows_appended = len(diff.appends)
        report.rows_unchanged = len(diff.unchanged_keys)


def summarise_for_humans(report: SyncReport) -> str:
    """Render a concise multi-line summary suitable for CLI output / logs."""
    lines: list[str] = []
    duration = (report.finished_at - report.started_at).total_seconds()
    lines.append(
        f"Sync completed in {duration:.1f}s "
        f"(failures: {'yes' if report.any_failures else 'no'})"
    )
    for child in report.children:
        lines.append(f"  {child.child}:")
        for r in child.source_results:
            if r.ok:
                lines.append(f"    [OK] {r.source}: {r.task_count} task(s)")
            else:
                lines.append(f"    [FAIL/{r.failure_kind}] {r.source}: {r.failure_message}")
        if child.sheet_skipped_reason:
            lines.append(f"    sheet: skipped — {child.sheet_skipped_reason}")
        else:
            lines.append(
                f"    sheet: {child.rows_updated} updated, "
                f"{child.rows_appended} appended, "
                f"{child.rows_unchanged} unchanged"
            )
        if child.new_tasks:
            lines.append(f"    new: {len(child.new_tasks)} task(s)")
        if child.changed_tasks:
            lines.append(f"    changed: {len(child.changed_tasks)} task(s)")
    return "\n".join(lines)


# Re-export so callers don't need to import state.store separately.
__all__ = [
    "ChildReport",
    "Orchestrator",
    "SourceResult",
    "SyncReport",
    "UpsertResult",
    "summarise_for_humans",
]

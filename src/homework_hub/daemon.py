"""Long-running daemon: APScheduler hourly cron + FastAPI /health endpoint.

Runs in the container as the default `python -m homework_hub` invocation.
Two concerns live in this module:

* **Scheduler** — APScheduler ``BackgroundScheduler`` configured from
  ``settings.sync_cron`` (default ``7 * * * *`` — hourly at :07). The job
  builds a fresh ``MedallionOrchestrator`` per run so a transient failure inside
  one source can't poison the next tick.

* **Health endpoint** — FastAPI app on ``settings.health_port`` exposing a
  single ``/health`` route. Uptime Kuma polls it; Discord notifications
  remain deferred (phase 12). Returns last-success/last-failure per
  ``(child, source)`` pair from the state DB plus the next scheduled
  fire time.

The scheduler is decoupled from the web layer for testability: pure
functions ``build_scheduler`` and ``build_health_app`` take their
collaborators as arguments, so tests can drive them with fakes without
spinning up a real uvicorn process.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI

from homework_hub.config import Settings
from homework_hub.medallion_orchestrator import (
    MedallionOrchestrator,
    summarise_medallion,
)
from homework_hub.state.store import StateStore
from homework_hub.wiring import build_medallion_orchestrator

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #


def _parse_cron(expr: str) -> CronTrigger:
    """Parse a 5-field cron string ('m h dom mon dow') into a CronTrigger."""
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"sync_cron must be a 5-field cron expression, got {expr!r}")
    minute, hour, day, month, day_of_week = parts
    return CronTrigger(
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=day_of_week,
    )


def build_scheduler(
    *,
    cron_expr: str,
    job: Callable[[], Any],
    job_id: str = "homework_hub_sync",
) -> BackgroundScheduler:
    """Construct (but don't start) a BackgroundScheduler with the sync job."""
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        job,
        trigger=_parse_cron(cron_expr),
        id=job_id,
        replace_existing=True,
        coalesce=True,  # if we miss ticks (clock skew), run only once
        max_instances=1,  # never overlap two syncs
    )
    return scheduler


def make_sync_job(
    orchestrator_factory: Callable[[], MedallionOrchestrator],
) -> Callable[[], None]:
    """Wrap a fresh-orchestrator-per-tick callable with logging.

    Each tick rebuilds the orchestrator so token reloads / config edits
    take effect without restarting the daemon.
    """

    def _tick() -> None:
        try:
            orchestrator = orchestrator_factory()
            report = orchestrator.run()
            log.info("sync tick complete\n%s", summarise_medallion(report))
        except Exception:
            # Never let an exception propagate out of an APScheduler job —
            # it would tear down the scheduler thread.
            log.exception("sync tick crashed")

    return _tick


# --------------------------------------------------------------------------- #
# Health endpoint
# --------------------------------------------------------------------------- #


def build_health_app(
    *,
    state: StateStore,
    scheduler: BackgroundScheduler | None = None,
    job_id: str = "homework_hub_sync",
) -> FastAPI:
    """FastAPI app exposing /health.

    Status semantics:
    * ``"ok"`` — every (child, source) pair has a more recent success than
      failure (or has never failed).
    * ``"degraded"`` — at least one source is currently in a failed state.
    * ``"unknown"`` — no syncs have run yet (fresh container).
    """
    app = FastAPI(title="Homework Hub", docs_url=None, redoc_url=None)

    @app.get("/health")
    def health() -> dict[str, Any]:  # pragma: no cover - exercised via TestClient
        return _health_payload(state=state, scheduler=scheduler, job_id=job_id)

    # Expose the inner function for unit tests that don't want to spin up
    # the TestClient. Attribute access is cheap and explicit.
    app.state.health_payload = lambda: _health_payload(
        state=state, scheduler=scheduler, job_id=job_id
    )

    return app


def _health_payload(
    *,
    state: StateStore,
    scheduler: BackgroundScheduler | None,
    job_id: str,
) -> dict[str, Any]:
    auth_records = list(state.all_auth())

    sources_payload: list[dict[str, Any]] = []
    any_failed = False
    any_seen = False
    for rec in auth_records:
        any_seen = True
        last_success = rec.last_success_at
        last_failure = rec.last_failure_at
        # A source is "failing" if its last failure is newer than its last
        # success (or there is no success at all).
        failing = last_failure is not None and (last_success is None or last_failure > last_success)
        if failing:
            any_failed = True
        sources_payload.append(
            {
                "child": rec.child,
                "source": rec.source,
                "last_success_at": last_success.isoformat() if last_success else None,
                "last_failure_at": last_failure.isoformat() if last_failure else None,
                "last_failure_kind": rec.last_failure_kind,
                "last_failure_message": rec.last_failure_message,
                "failing": failing,
            }
        )

    if not any_seen:
        status = "unknown"
    elif any_failed:
        status = "degraded"
    else:
        status = "ok"

    next_run_at: str | None = None
    if scheduler is not None:
        job = scheduler.get_job(job_id)
        if job is not None and job.next_run_time is not None:
            next_run_at = job.next_run_time.astimezone(UTC).isoformat()

    return {
        "status": status,
        "now": datetime.now(UTC).isoformat(),
        "next_run_at": next_run_at,
        "sources": sources_payload,
    }


# --------------------------------------------------------------------------- #
# Top-level daemon entrypoint
# --------------------------------------------------------------------------- #


def run_daemon(settings: Settings) -> None:  # pragma: no cover - integration
    """Block forever running scheduler + uvicorn.

    Intentionally not unit-tested; ``build_scheduler`` and
    ``build_health_app`` carry the testable surface area. This function is
    exercised end-to-end during manual deployment.
    """
    import uvicorn

    state = StateStore(settings.state_db)
    scheduler = build_scheduler(
        cron_expr=settings.sync_cron,
        job=make_sync_job(lambda: build_medallion_orchestrator(settings)),
    )
    app = build_health_app(state=state, scheduler=scheduler)

    scheduler.start()
    log.info(
        "homework-hub daemon started: cron=%r, health=:%d",
        settings.sync_cron,
        settings.health_port,
    )
    try:
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=settings.health_port,
            log_level="info",
            access_log=False,
        )
    finally:
        scheduler.shutdown(wait=False)


__all__ = [
    "build_health_app",
    "build_scheduler",
    "make_sync_job",
    "run_daemon",
]

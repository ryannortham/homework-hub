"""Tests for the APScheduler + FastAPI daemon module.

We don't start uvicorn or a real scheduler thread; we exercise the pure
factory functions ``build_scheduler``, ``make_sync_job`` and
``build_health_app`` against in-memory collaborators.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from homework_hub.daemon import (
    _parse_cron,
    build_health_app,
    build_scheduler,
    make_sync_job,
)
from homework_hub.state.store import StateStore

# --------------------------------------------------------------------------- #
# Cron parsing
# --------------------------------------------------------------------------- #


def test_parse_cron_accepts_five_field_expression():
    trigger = _parse_cron("7 * * * *")
    # APScheduler's CronTrigger stores fields by name; verify via repr.
    text = repr(trigger)
    assert "minute='7'" in text


@pytest.mark.parametrize("expr", ["", "7", "7 *", "7 * * *", "7 * * * * *"])
def test_parse_cron_rejects_wrong_field_count(expr: str):
    with pytest.raises(ValueError):
        _parse_cron(expr)


# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #


def test_build_scheduler_registers_job_with_cron_trigger():
    calls: list[int] = []

    def _job() -> None:
        calls.append(1)

    scheduler = build_scheduler(cron_expr="7 * * * *", job=_job)
    job = scheduler.get_job("homework_hub_sync")
    assert job is not None
    assert "minute='7'" in repr(job.trigger)
    # Job is registered but not yet running — start would block in tests.
    assert calls == []


def test_make_sync_job_swallows_exceptions(caplog):
    """A crashing orchestrator must NOT propagate out of the job callable.

    APScheduler would otherwise tear down the worker thread and we'd
    silently stop syncing.
    """

    def _factory():  # pragma: no cover - constructor runs, raise is below
        raise RuntimeError("boom")

    job = make_sync_job(_factory)
    # Should not raise.
    with caplog.at_level("ERROR"):
        job()
    assert any("sync tick crashed" in rec.message for rec in caplog.records)


def test_make_sync_job_invokes_orchestrator_run():
    runs: list[str] = []

    class _FakeOrchestrator:
        def run(self):
            runs.append("ran")

            class _R:
                children: list = []  # noqa: RUF012 — test fixture, not real model
                started_at = datetime.now(UTC)
                finished_at = datetime.now(UTC)

                @property
                def any_failures(self) -> bool:
                    return False

            return _R()

    job = make_sync_job(lambda: _FakeOrchestrator())
    job()
    assert runs == ["ran"]


# --------------------------------------------------------------------------- #
# Health endpoint
# --------------------------------------------------------------------------- #


def _state(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


def test_health_unknown_when_no_syncs_yet(tmp_path: Path):
    state = _state(tmp_path)
    app = build_health_app(state=state)
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "unknown"
    assert body["sources"] == []


def test_health_ok_when_all_sources_recently_succeeded(tmp_path: Path):
    state = _state(tmp_path)
    state.record_success("james", "classroom")
    state.record_success("james", "compass")
    app = build_health_app(state=state)
    client = TestClient(app)
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert {s["source"] for s in body["sources"]} == {"classroom", "compass"}
    assert all(s["failing"] is False for s in body["sources"])


def test_health_degraded_when_a_source_is_failing(tmp_path: Path):
    state = _state(tmp_path)
    state.record_success("james", "classroom")
    state.record_failure("james", "compass", kind="auth_expired", message="cookie expired")
    app = build_health_app(state=state)
    body = client_get_health(app)
    assert body["status"] == "degraded"
    failing = [s for s in body["sources"] if s["failing"]]
    assert len(failing) == 1
    assert failing[0]["source"] == "compass"
    assert failing[0]["last_failure_kind"] == "auth_expired"


def test_health_recovers_when_failure_is_older_than_success(tmp_path: Path):
    """A new success after a past failure flips the source back to healthy."""
    state = _state(tmp_path)
    past = datetime.now(UTC) - timedelta(hours=2)
    state.record_failure("james", "compass", kind="transient", message="timeout", now=past)
    state.record_success("james", "compass")  # now = default = utcnow
    app = build_health_app(state=state)
    body = client_get_health(app)
    assert body["status"] == "ok"
    [src] = body["sources"]
    assert src["failing"] is False
    assert src["last_failure_at"] is not None  # still recorded for posterity


def test_health_includes_next_run_time_when_scheduler_attached(tmp_path: Path):
    state = _state(tmp_path)
    scheduler = build_scheduler(cron_expr="7 * * * *", job=lambda: None)
    scheduler.start(paused=True)  # compute next_run_time without firing
    try:
        app = build_health_app(state=state, scheduler=scheduler)
        body = client_get_health(app)
        assert body["next_run_at"] is not None
        # ISO 8601 format check — parse round-trips.
        datetime.fromisoformat(body["next_run_at"])
    finally:
        scheduler.shutdown(wait=False)


def client_get_health(app) -> dict:
    """Helper for the tests above — single TestClient lifecycle per call."""
    with TestClient(app) as client:
        return client.get("/health").json()

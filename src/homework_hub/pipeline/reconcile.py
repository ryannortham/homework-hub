"""Silver-layer task-cleansing sweeps.

Two responsibilities, both invoked from the orchestrator:

1. **Upstream-disappearance reconciliation** — after each successful ingest,
   compare the set of ``source_id``s that came back with what we have in
   silver for that ``(child, source)``. Rows that have been missing for
   ``stale_grace_syncs`` consecutive successful syncs get archived with
   reason ``upstream_removed``. Reappearing rows are un-archived
   automatically by the silver writer.

2. **Age-cap sweep** — once per child per sync, archive non-terminal silver
   rows whose anchor date is older than ``active_cutoff_days``. Catches
   forgotten Overdue tasks and date-less zombies that never partition into
   History on their own. Reason ``age_cap``.

Both functions emit one ``INFO`` log line per task they archive so the
behaviour is observable from container logs without any extra plumbing.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from homework_hub.models import Status
from homework_hub.state.store import StateStore

log = logging.getLogger(__name__)

# Statuses that mean "this task is done with" — never re-archive these.
_TERMINAL = {
    Status.SUBMITTED.value,
    Status.GRADED.value,
    Status.ARCHIVED.value,
}


def reconcile_stale(
    store: StateStore,
    *,
    child: str,
    source: str,
    seen_ids: list[str],
    grace_syncs: int = 2,
    now: datetime | None = None,
) -> list[str]:
    """Reconcile silver against the source_ids that came back in this sync.

    Steps:

    1. ``bump_last_seen`` for ``seen_ids`` (also clears
       ``archived_at`` / ``archived_reason`` when reason was
       ``upstream_removed`` — the recovery path).
    2. Increment ``missing_streak`` for every other silver row in
       ``(child, source)``.
    3. Archive any non-terminal row whose ``missing_streak >= grace_syncs``
       and which isn't already archived.

    Returns the list of ``source_id``s that were newly archived this run.
    """
    ts = now or datetime.now(UTC)

    # Always bump first so reappearances clear their archive flags before we
    # consider re-archiving. ``seen_ids=[]`` is a valid input (everything
    # disappeared) and is handled by the helper.
    store.bump_last_seen(child=child, source=source, source_ids=seen_ids, now=ts)
    rows = store.increment_missing_streak(child=child, source=source, seen_ids=seen_ids)

    archived: list[str] = []
    for row in rows:
        if row["missing_streak"] < grace_syncs:
            continue
        if row["status"] in _TERMINAL:
            continue
        if row["archived_at"] is not None:
            continue
        store.mark_archived(
            child=child,
            source=source,
            source_id=row["source_id"],
            reason="upstream_removed",
            now=ts,
        )
        archived.append(row["source_id"])
        log.info(
            "reconcile_stale: archived child=%s source=%s source_id=%s streak=%d",
            child,
            source,
            row["source_id"],
            row["missing_streak"],
        )

    return archived


def apply_age_cap(
    store: StateStore,
    *,
    child: str,
    cutoff_days: int,
    now: datetime | None = None,
) -> list[tuple[str, str]]:
    """Archive non-terminal silver rows whose anchor date is more than
    ``cutoff_days`` in the past.

    Anchor priority per row: ``due_at`` if set, otherwise ``first_seen_at``.
    Rows already archived, or in a terminal status, are skipped.

    Returns the list of ``(source, source_id)`` tuples that were archived.
    """
    ts = now or datetime.now(UTC)
    cutoff = ts - timedelta(days=cutoff_days)

    archived: list[tuple[str, str]] = []
    for row in store.silver_for_child(child=child):
        if row["status"] in _TERMINAL:
            continue
        if row["archived_at"] is not None:
            continue

        anchor_str = row["due_at"] or row["first_seen_at"]
        if not anchor_str:
            continue
        try:
            anchor = datetime.fromisoformat(anchor_str)
        except ValueError:
            continue
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=UTC)

        if anchor < cutoff:
            store.mark_archived(
                child=child,
                source=row["source"],
                source_id=row["source_id"],
                reason="age_cap",
                now=ts,
            )
            archived.append((row["source"], row["source_id"]))
            log.info(
                "apply_age_cap: archived child=%s source=%s source_id=%s anchor=%s",
                child,
                row["source"],
                row["source_id"],
                anchor.isoformat(),
            )

    return archived

"""Gold publish — project silver rows into per-tab table data.

The publish flow:

1. Read silver rows for the child.
2. Read existing ``UserEdits`` rows from the spreadsheet via the
   :class:`GoldSink` protocol.
3. Capture live kid overrides from the Tasks and History tabs *before*
   they are overwritten.
4. Project silver into task rows using :func:`project_tasks_rows`.
5. Merge ``UserEdits`` over the editable columns.
6. Partition rows into Active (→ Tasks tab) and History (→ History tab)
   using a configurable ``cutoff_days`` window.
7. Write Tasks, History, Settings and UserEdits tabs through the sink.

This module owns ``Source``-display labels (``Compass``/``Classroom``/
``Edrolo``) and ``Status``-display labels (``Not started``/...). Date
columns are converted from UTC datetimes to Melbourne local dates so
the kid sees the date a task is actually due in their timezone.

The Protocol :class:`GoldSink` describes the surface ``publish_for_child``
needs from the sheet client; a fake implementation is used in tests.
"""

from __future__ import annotations

import contextlib
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from homework_hub.dashboard_layout import (
    build_requests as build_dashboard_requests,
)
from homework_hub.dashboard_layout import (
    task_rows_to_dashboard_tasks,
)
from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task, TaskType
from homework_hub.pipeline.auth_status import SourceAuthRow
from homework_hub.schema import (
    HISTORY_TAB,
    SCHEMA,
    SETTINGS_TAB,
    TASKS_TAB,
    ColumnKind,
    TabSpec,
)
from homework_hub.state.store import StateStore

log = logging.getLogger(__name__)

MELBOURNE = ZoneInfo("Australia/Melbourne")

# Display labels for the dropdown columns. Order matches schema vocab.
_SOURCE_DISPLAY: dict[str, str] = {
    SourceEnum.COMPASS.value: "Compass",
    SourceEnum.CLASSROOM.value: "Classroom",
    SourceEnum.EDUPERFECT.value: "EP",
    SourceEnum.EDROLO.value: "Edrolo",
}

_STATUS_DISPLAY: dict[str, str] = {
    Status.NOT_STARTED.value: "Not started",
    Status.IN_PROGRESS.value: "In progress",
    Status.SUBMITTED.value: "Submitted",
    Status.GRADED.value: "Graded",
    Status.OVERDUE.value: "Overdue",
    Status.ARCHIVED.value: "Archived",
}

_TASK_TYPE_DISPLAY: dict[str, str] = {
    TaskType.ASSESSMENT.value: "Assessment",
    TaskType.HOMEWORK.value: "Homework",
    TaskType.GENERAL.value: "General",
}


# --------------------------------------------------------------------------- #
# Pure projection helpers
# --------------------------------------------------------------------------- #


def melbourne_local_date(dt: datetime | None) -> date | None:
    """Convert a UTC datetime to a Melbourne local date.

    DST-aware via ``zoneinfo``. Naive inputs are assumed UTC. ``None``
    in → ``None`` out.
    """
    if dt is None:
        return None
    aware = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    return aware.astimezone(MELBOURNE).date()


def format_melbourne_dmy(dt: datetime | None) -> str:
    """Format a UTC datetime as Melbourne-local ``dd/mm/yyyy HH:MM``.

    Used for the Settings tab where Sheets number-format coercion doesn't
    apply (the tab is a plain text grid, not a Table with a DATE column).
    Returns an em-dash for ``None``.
    """
    if dt is None:
        return "—"
    aware = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    return aware.astimezone(MELBOURNE).strftime("%d/%m/%Y %H:%M")


def format_melbourne_dmy_date(dt: datetime | None) -> str:
    """Format a UTC datetime as Melbourne-local ``dd/mm/yyyy`` (date only)."""
    if dt is None:
        return "—"
    aware = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    return aware.astimezone(MELBOURNE).strftime("%d/%m/%Y")


def task_uid(task: Task) -> str:
    """Stable identifier for the UserEdits merge.

    Format: ``<source>:<source_id>``. Independent of child because each
    child's spreadsheet only ever holds their own tasks.
    """
    return f"{task.source.value}:{task.source_id}"


def checkpoint_uid(task: Task, gi_id: int) -> str:
    """Stable identifier for a sub-task row derived from a Compass gradingItem.

    Format: ``<source>:<source_id>:gi:<gi_id>``.  The ``:gi:`` separator
    lets callers distinguish sub-task uids from parent task uids.
    """
    return f"{task.source.value}:{task.source_id}:gi:{gi_id}"


def parent_uid_from_checkpoint(uid: str) -> str | None:
    """Extract the parent task_uid from a checkpoint uid, or ``None`` if not
    a checkpoint uid."""
    if ":gi:" in uid:
        return uid.split(":gi:")[0]
    return None


@dataclass(frozen=True)
class TaskRow:
    """One projected row for the Tasks or History tab. Cells are tab-column-ordered."""

    task_uid: str
    cells: tuple[object, ...]


def _task_row(
    *,
    uid: str,
    subject: str,
    task_type: str,
    title: str,
    due: date | None,
    status: str,
    source: str,
    link: str,
) -> TaskRow:
    """Build one TaskRow with the 10-column TASKS_TAB column order."""
    days_formula = TASKS_TAB.columns[TASKS_TAB.column_index("days")].formula_template
    cell_by_key = {
        "subject": subject,
        "task_type": task_type,
        "title": title,
        "due": due,
        "days": days_formula,
        "status": status,
        "notes": "",
        "source": source,
        "link": link,
        "task_uid": uid,
    }
    cells = tuple(cell_by_key[c.key] for c in TASKS_TAB.columns)
    return TaskRow(task_uid=uid, cells=cells)


def project_tasks_rows(tasks: list[Task]) -> list[TaskRow]:
    """Project silver tasks into task row data.

    Tasks with Compass ``Checkpoints`` gradingItems are expanded into one
    sub-task row per checkpoint. Sub-task titles are merged as
    ``"<parent title>: <checkpoint name>"`` so the kid sees both at a glance.

    Editable column ``notes`` defaults to blank. The ``Days`` column is
    written as a row-relative formula so Sheets evaluates it on every open.
    """
    rows: list[TaskRow] = []
    for t in tasks:
        uid = task_uid(t)
        src_label = _SOURCE_DISPLAY.get(t.source.value, t.source.value)
        type_label = _TASK_TYPE_DISPLAY.get(t.task_type.value, "Homework")
        status_label = _STATUS_DISPLAY.get(t.status.value, t.status.value)
        due = melbourne_local_date(t.due_at)

        # Tasks with checkpoints are expanded into one row per checkpoint;
        # the parent row is suppressed (the checkpoints carry all the detail).
        if t.checkpoints:
            for cp in t.checkpoints:
                gi_id = cp.get("id")
                gi_name = cp.get("name", "").strip()
                if not gi_id or not gi_name:
                    continue
                rows.append(
                    _task_row(
                        uid=checkpoint_uid(t, gi_id),
                        subject=t.subject or "",
                        task_type=type_label,
                        title=f"{t.title}: {gi_name}",
                        due=due,
                        status=status_label,
                        source=src_label,
                        link=t.url,
                    )
                )
        else:
            rows.append(
                _task_row(
                    uid=uid,
                    subject=t.subject or "",
                    task_type=type_label,
                    title=t.title,
                    due=due,
                    status=status_label,
                    source=src_label,
                    link=t.url,
                )
            )

    return rows


def cap_future_dates(
    rows: list[TaskRow],
    *,
    cap_days: int = 365,
    today: date | None = None,
) -> list[TaskRow]:
    """Blank the Due cell on any row whose due date is implausibly far in the
    future. Belt-and-braces against source-side date-parser drift (e.g. the
    pre-fix Classroom bare-date roll-forward that emitted 2027 dates from
    last-year's missed cards). Emits a WARN log line per affected row."""
    today_local = today or datetime.now(MELBOURNE).date()
    cap = today_local + timedelta(days=cap_days)
    due_idx = TASKS_TAB.column_index("due")
    capped: list[TaskRow] = []
    for row in rows:
        cell = row.cells[due_idx]
        if isinstance(cell, date) and cell > cap:
            log.warning(
                "cap_future_dates: blanking due=%s on uid=%s (cap=%s)",
                cell.isoformat(),
                row.task_uid,
                cap.isoformat(),
            )
            new_cells = list(row.cells)
            new_cells[due_idx] = ""
            capped.append(TaskRow(task_uid=row.task_uid, cells=tuple(new_cells)))
        else:
            capped.append(row)
    return capped


def project_settings_rows(
    *,
    child: str,
    last_synced: datetime | None,
    source_auth_rows: list[SourceAuthRow] | None = None,
) -> list[tuple[str, str, str, str]]:
    """Project the Settings tab as a 4-column table.

    Layout (rows include blank trailing cells so every row has exactly
    four columns; Sheets renders them as a uniform grid):

    1. One row per enabled source: ``Source | Last Synced | Token Expires | Status``.
       Dates are formatted as Melbourne-local ``dd/mm/yyyy HH:MM``.
    2. A spacer row.
    3. A trailer block with the child name, the full-sync timestamp, and
       the list of managed tabs.

    ``source_auth_rows`` is optional for backwards compatibility with the
    older two-column tests; when omitted the per-source block is empty.
    """
    rows: list[tuple[str, str, str, str]] = []

    for row in source_auth_rows or []:
        rows.append(
            (
                row.display_name,
                format_melbourne_dmy(row.last_success_at),
                _format_token_expires(row),
                _STATUS_LABEL.get(row.status, row.status),
            )
        )

    rows.append(("", "", "", ""))

    rows.append(("Child", child, "", ""))
    rows.append(("Last full sync", format_melbourne_dmy(last_synced), "", ""))
    rows.append(
        (
            "Tabs managed",
            ", ".join(t.name for t in SCHEMA.tabs if not t.hidden),
            "",
            "",
        )
    )
    return rows


_STATUS_LABEL: dict[str, str] = {
    "ok": "OK",
    "expiring": "Expiring soon",
    "expired": "Expired",
    "missing": "No token",
    "never_synced": "Never synced",
    "failing": "Failing",
    "unknown": "Unknown",
}


def _format_token_expires(row: SourceAuthRow) -> str:
    """Render the Token Expires cell for one source row.

    * EduPerfect / Classroom expose an absolute expiry — show it.
    * Edrolo's ``sessionid`` is a browser-session cookie (no expiry) —
      show ``Session``.
    * Compass has no published expiry; we fall back to the captured-at
      timestamp returned by the reader so the kid can see roughly how
      stale the cookie is.
    """
    if not row.token_present:
        return "No token"
    if row.token_expires_at is None:
        if row.source == "edrolo":
            return "Session"
        if row.source == "compass":
            return "Unknown"
        return "Session"
    if row.source == "compass":
        return f"Captured {format_melbourne_dmy(row.token_expires_at)}"
    return format_melbourne_dmy(row.token_expires_at)


# --------------------------------------------------------------------------- #
# Task partitioning
# --------------------------------------------------------------------------- #


def partition_tasks(
    rows: list[TaskRow],
    tasks: list[Task],
    user_edits: list[UserEdit],
    *,
    cutoff_days: int = 30,
    now: datetime | None = None,
) -> tuple[list[TaskRow], list[TaskRow]]:
    """Partition task rows into (active, history).

    A row moves to History when:

    * Its effective status is ``Archived`` (immediate — no cutoff wait), OR
    * Its effective status is ``Submitted`` or ``Graded`` AND its effective
      completion date is older than ``cutoff_days`` ago.

    Effective completion date priority:
    1. ``updated_at`` from the kid's status UserEdit (manual override timestamp).
    2. ``submitted_at`` from the silver Task.
    3. ``due_at`` from the silver Task.
    4. ``first_seen_at`` from the silver Task (stable anchor; ``last_synced``
       was deliberately removed here because it refreshes every sync and
       therefore prevented terminal rows lacking an upstream completion
       timestamp from ever ageing into History).

    Rows whose completion date cannot be determined are routed straight to
    History (a terminal status with no temporal anchor is clearly stale and
    should not clutter the Tasks tab).
    """
    ref = now or datetime.now(UTC)
    cutoff = ref - timedelta(days=cutoff_days)

    task_by_uid = {f"{t.source.value}:{t.source_id}": t for t in tasks}

    # Index the latest status UserEdit per uid for completion-date lookup.
    status_edit_by_uid: dict[str, UserEdit] = {}
    for e in user_edits:
        if e.column == "status":
            status_edit_by_uid[e.task_uid] = e

    status_idx = TASKS_TAB.column_index("status")
    _terminal = {"Submitted", "Graded"}

    active: list[TaskRow] = []
    history: list[TaskRow] = []

    for row in rows:
        effective_status = row.cells[status_idx]

        # Archived rows go straight to History regardless of cutoff.
        if effective_status == "Archived":
            history.append(row)
            continue

        if effective_status not in _terminal:
            active.append(row)
            continue

        # Resolve the silver Task (handles checkpoint rows via parent uid).
        uid = row.task_uid
        task = task_by_uid.get(uid)
        if task is None:
            parent_uid = parent_uid_from_checkpoint(uid)
            if parent_uid:
                task = task_by_uid.get(parent_uid)

        # Determine the effective completion datetime.
        completion: datetime | None = None
        status_edit = status_edit_by_uid.get(uid)
        if status_edit is not None:
            with contextlib.suppress(ValueError, TypeError):
                completion = datetime.fromisoformat(status_edit.updated_at)
        if completion is None and task is not None:
            completion = task.submitted_at or task.due_at or task.first_seen_at

        if completion is None:
            # Terminal status with no temporal anchor — treat as stale and
            # route to History rather than letting it linger on Tasks.
            history.append(row)
            continue

        # Ensure tz-aware for comparison.
        if completion.tzinfo is None:
            completion = completion.replace(tzinfo=UTC)

        if completion < cutoff:
            history.append(row)
        else:
            active.append(row)

    return active, history


# --------------------------------------------------------------------------- #
# UserEdits merge
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class UserEdit:
    """One persisted kid override for a (task_uid, column) pair.

    ``original_value`` captures the system-derived (projected silver) value
    at the moment the override is written. Refreshed every publish cycle so
    it always reflects what the system *would* show now if the override
    were cleared. When the system catches up (silver value equals the kid's
    overridden value), the override is automatically dropped by
    :func:`diff_user_edits` because there's no longer a meaningful diff.
    """

    task_uid: str
    column: str
    value: object  # already coerced to bool for checkboxes
    updated_at: str
    original_value: object = ""


def merge_user_edits(
    rows: list[TaskRow],
    edits: list[UserEdit],
) -> list[TaskRow]:
    """Overlay kid overrides on the editable columns of task rows.

    Edits referencing a ``task_uid`` no longer present in silver are
    silently dropped — silver is the source of truth for which tasks
    exist; UserEdits is just the kid's preference layer on top.
    """
    editable_keys = {c.key for c in TASKS_TAB.editable_columns()}
    by_uid: dict[str, dict[str, object]] = {}
    for e in edits:
        if e.column not in editable_keys:
            continue
        by_uid.setdefault(e.task_uid, {})[e.column] = e.value

    merged: list[TaskRow] = []
    for row in rows:
        overrides = by_uid.get(row.task_uid)
        if not overrides:
            merged.append(row)
            continue
        new_cells = list(row.cells)
        for col_idx, col in enumerate(TASKS_TAB.columns):
            if col.key in overrides:
                value = overrides[col.key]
                if col.kind is ColumnKind.DATE and isinstance(value, str):
                    parsed = _parse_tasks_tab_date(value)
                    if parsed is None:
                        continue
                    value = parsed
                new_cells[col_idx] = value
        merged.append(TaskRow(task_uid=row.task_uid, cells=tuple(new_cells)))
    return merged


def diff_user_edits(
    rows: list[TaskRow],
    existing: list[UserEdit],
    projected: list[TaskRow] | None = None,
) -> list[UserEdit]:
    """Compute the canonical UserEdits row-set for the task tabs.

    For each editable cell that differs from its *projected* (system-derived)
    value we emit a ``UserEdit`` — this represents a deliberate kid override.
    Cells that match the projected value are not persisted, so ``UserEdits``
    stays small.

    ``projected`` is the pre-merge output of :func:`project_tasks_rows`.
    When omitted (backwards-compat) a static default of ``""`` is used.
    Callers should always supply it.

    Existing ``updated_at`` timestamps are preserved when the value did not
    change (avoids spurious churn on every publish).
    """
    editable_cols = TASKS_TAB.editable_columns()
    projected_by_uid: dict[str, TaskRow] = {r.task_uid: r for r in projected} if projected else {}
    _static_defaults: dict[str, object] = {"notes": ""}
    existing_by_key = {(e.task_uid, e.column): e for e in existing}

    out: list[UserEdit] = []
    now = datetime.now(UTC).isoformat()
    for row in rows:
        proj_row = projected_by_uid.get(row.task_uid)
        for col in editable_cols:
            idx = TASKS_TAB.column_index(col.key)
            value = row.cells[idx]
            # Use the projected (system-derived) value as the baseline.
            if proj_row is not None:
                default = proj_row.cells[idx]
            else:
                default = _static_defaults.get(col.key, "")
            if value == default:
                # System has caught up to the kid's value (or there never
                # was an override). Skip — this naturally drops the
                # override from UserEdits on the next writeback cycle.
                continue
            prior = existing_by_key.get((row.task_uid, col.key))
            if prior is not None and prior.value == value:
                # Unchanged value — preserve original updated_at but
                # refresh ``original_value`` to the current projected
                # silver value (Option B semantics).
                out.append(
                    UserEdit(
                        task_uid=prior.task_uid,
                        column=prior.column,
                        value=prior.value,
                        updated_at=prior.updated_at,
                        original_value=default,
                    )
                )
            else:
                out.append(
                    UserEdit(
                        task_uid=row.task_uid,
                        column=col.key,
                        value=value,
                        updated_at=now,
                        original_value=default,
                    )
                )
    return out


# Sheets date-serial epoch: days since 30 Dec 1899.
_SHEETS_EPOCH = date(1899, 12, 30)


def _parse_tasks_tab_date(raw: str) -> date | None:
    """Parse a date cell from the Tasks/History tab as returned by ``get_all_values()``.

    Sheets returns the cell's *display* string, so we expect ``dd/MM/yyyy``
    (the format applied at bootstrap).  As a fallback we also handle the raw
    integer serial string that Sheets occasionally returns when the cell was
    written as a number rather than a formatted date. Persisted UserEdits use
    ISO ``yyyy-MM-dd``, which is also accepted so date overrides retain their
    type when reapplied. Any other value (empty, unparseable) returns ``None``
    so the caller can treat it as "no override".
    """
    raw = raw.strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d/%m/%Y").date()
    except ValueError:
        pass
    try:
        return date.fromisoformat(raw)
    except ValueError:
        pass
    if raw.isdigit():
        return _SHEETS_EPOCH + timedelta(days=int(raw))
    return None


def capture_tab_edits(
    raw_rows: list[list[str]],
    projected: list[TaskRow],
    *,
    tab: TabSpec = TASKS_TAB,
) -> list[UserEdit]:
    """Detect kid overrides by comparing a live tab against projected defaults.

    Called with the raw string rows from ``get_all_values()[1:]`` (header
    stripped) before the tab is overwritten.  Joins each raw row to its
    projected counterpart by ``task_uid`` (last column), then for every
    editable column emits a :class:`UserEdit` when the cell value differs
    from the system default.

    ``tab`` selects which tab's editable columns to inspect. Defaults to
    ``TASKS_TAB``; pass ``HISTORY_TAB`` when reading the History tab (editable
    columns are Status and Notes only — Due is locked in History).

    Coercion per column kind:
    - ``CHECKBOX``  — ``"TRUE"`` → ``True``, ``"FALSE"`` → ``False``.
    - ``DATE``      — parsed via :func:`_parse_tasks_tab_date`; failures skipped.
    - ``DROPDOWN`` / ``TEXT`` — raw string; empty string skipped (= no override).
    """
    editable_cols = tab.editable_columns()
    uid_idx = TASKS_TAB.column_index("task_uid")  # same index in both tabs
    projected_by_uid = {r.task_uid: r for r in projected}
    now = datetime.now(UTC).isoformat()

    out: list[UserEdit] = []
    for raw_row in raw_rows:
        if len(raw_row) <= uid_idx:
            continue
        uid = raw_row[uid_idx]
        proj_row = projected_by_uid.get(uid)
        if proj_row is None:
            continue

        for col in editable_cols:
            col_idx = TASKS_TAB.column_index(col.key)  # same index in both tabs
            if col_idx >= len(raw_row):
                continue
            raw_val = raw_row[col_idx]

            # Coerce raw string to the appropriate Python type.
            if col.kind is ColumnKind.CHECKBOX:
                if raw_val not in ("TRUE", "FALSE"):
                    continue
                value: object = raw_val == "TRUE"
            elif col.kind is ColumnKind.DATE:
                value = _parse_tasks_tab_date(raw_val)
                if value is None:
                    continue
            else:
                # DROPDOWN / TEXT — empty string means no override.
                if not raw_val:
                    continue
                value = raw_val

            default = proj_row.cells[col_idx]
            if value == default:
                continue

            out.append(
                UserEdit(
                    task_uid=uid,
                    column=col.key,
                    value=value,
                    updated_at=now,
                    original_value=default,
                )
            )

    return out


def _merge_edit_sources(
    live: list[UserEdit],
    persisted: list[UserEdit],
) -> list[UserEdit]:
    """Combine live (Tasks/History tabs) and persisted (UserEdits tab) edit lists.

    Live edits represent the kid's current state in the sheet and always
    take precedence over what was persisted from a previous sync.
    """
    merged: dict[tuple[str, str], UserEdit] = {(e.task_uid, e.column): e for e in persisted}
    for e in live:
        merged[(e.task_uid, e.column)] = e
    return list(merged.values())


def apply_unarchive_edits(
    edits: list[UserEdit],
    tasks: list[Task],
    store: StateStore,
) -> list[Task]:
    """Honour kid-driven un-archive: any ``status`` edit on an archived task
    that sets a non-Archived value clears the archive flags in silver and
    returns the updated task list so downstream projection sees the change
    within the same publish cycle. Returns ``tasks`` unmodified when no
    un-archive edits are present."""
    task_by_uid = {f"{t.source.value}:{t.source_id}": t for t in tasks}
    updates: dict[str, Task] = {}

    for edit in edits:
        if edit.column != "status":
            continue
        task = task_by_uid.get(edit.task_uid)
        if task is None or task.status != Status.ARCHIVED:
            continue
        new_label = str(edit.value).strip().lower()
        if new_label == "archived":
            continue
        # Reverse the display label back to enum; default to NOT_STARTED if
        # the kid's value doesn't match a known status.
        reverse = {v.lower(): k for k, v in _STATUS_DISPLAY.items()}
        new_status = Status(reverse.get(new_label, Status.NOT_STARTED.value))
        store.clear_archive(child=task.child, source=task.source.value, source_id=task.source_id)
        log.info(
            "apply_unarchive_edits: kid un-archived uid=%s → %s",
            edit.task_uid,
            new_status.value,
        )
        updates[edit.task_uid] = task.model_copy(
            update={
                "status": new_status,
                "archived_at": None,
                "archived_reason": None,
            }
        )

    if not updates:
        return tasks
    return [updates.get(f"{t.source.value}:{t.source_id}", t) for t in tasks]


def apply_archive_edits(
    edits: list[UserEdit],
    tasks: list[Task],
    store: StateStore,
) -> list[Task]:
    """Honour kid-driven archive: any ``status`` edit setting ``Archived`` on
    a non-archived task writes the archive flags through to silver so the row
    persists as archived across syncs. Returns the updated task list so
    downstream projection sees the change within the same publish cycle.

    The mirror of :func:`apply_unarchive_edits`. Silver ``Graded`` /
    ``Overdue`` tasks are skipped (terminal statuses can't be re-archived
    via the sheet; ``filter_superseded_edits`` already drops those edits).
    """
    task_by_uid = {f"{t.source.value}:{t.source_id}": t for t in tasks}
    updates: dict[str, Task] = {}
    _terminal_status = {Status.GRADED, Status.OVERDUE}

    for edit in edits:
        if edit.column != "status":
            continue
        if str(edit.value).strip().lower() != "archived":
            continue
        task = task_by_uid.get(edit.task_uid)
        if task is None or task.status == Status.ARCHIVED:
            continue
        if task.status in _terminal_status:
            continue
        store.mark_archived(
            child=task.child,
            source=task.source.value,
            source_id=task.source_id,
            reason="kid_edit",
        )
        log.info("apply_archive_edits: kid archived uid=%s", edit.task_uid)
        updates[edit.task_uid] = task.model_copy(
            update={
                "status": Status.ARCHIVED,
                "archived_reason": "kid_edit",
            }
        )

    if not updates:
        return tasks
    return [updates.get(f"{t.source.value}:{t.source_id}", t) for t in tasks]


def filter_superseded_edits(
    edits: list[UserEdit],
    tasks: list[Task],
) -> list[UserEdit]:
    """Drop kid overrides that silver's current state has superseded.

    Precedence rules:
    - ``status`` — silver ``Graded``, ``Overdue`` or ``Submitted`` locks
      the status column against kid *downgrades*: the source system is
      the authority on whether a task is done / past-due, and a kid
      edit that pulls the row back to ``Not started`` / ``In progress``
      would otherwise stick forever (the original_value on the edit
      matches the source's current Submitted state, so the diff layer
      can't detect drift). Kids CAN still set ``Archived`` from any
      state — handled by :func:`apply_archive_edits` — and can also
      edit ``Archived`` → anything else to un-archive a task (handled
      by :func:`apply_unarchive_edits`).
    - ``due``    — kid always wins; they may set or override any due date.
    - ``notes`` — kid always wins; no silver equivalent.
    """
    task_by_uid = {f"{t.source.value}:{t.source_id}": t for t in tasks}
    _terminal_status = {Status.GRADED, Status.OVERDUE, Status.SUBMITTED}

    out: list[UserEdit] = []
    for edit in edits:
        task = task_by_uid.get(edit.task_uid)
        if task is None:
            # Task no longer in silver — orphan, will be pruned by diff_user_edits.
            out.append(edit)
            continue

        # Status column has a terminal-state lock against downgrades:
        # silver Graded / Overdue / Submitted supersede any kid edit
        # EXCEPT a move to ``Archived`` (kids may always shelve work
        # they consider done). Kid-driven Archive is handled by
        # apply_archive_edits which writes the archive flags through
        # to silver so the row stays archived across syncs and
        # partitions to History.
        if (
            edit.column == "status"
            and task.status in _terminal_status
            and str(edit.value).strip().lower() != "archived"
        ):
            continue

        out.append(edit)

    return out


# --------------------------------------------------------------------------- #
# Sink protocol + publish entry point
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DashboardMeta:
    """Live metadata about the Dashboard tab needed for an idempotent
    publish-time relayout.

    ``sheet_id`` is the Dashboard tab's sheetId. ``table_ids`` is the
    list of any pre-existing Dashboard Tables (typically the three
    ``tbl_dash_*`` ids, but tolerant of zero on first publish or one if
    a previous publish was interrupted). ``banded_range_ids`` and
    ``conditional_format_rule_count`` are the counts/ids needed to drain
    those artefacts before re-emitting fresh ones.

    ``protected_range_ids`` carries the ids of any *whole-sheet*
    protected ranges already installed on the Dashboard. Used by publish
    to decide whether to emit a fresh ``addProtectedRange`` request
    (idempotent install on first sync after deploy).

    ``theme_accent`` is the spreadsheet theme's ``ACCENT1`` colour
    resolved to an ``{"red","green","blue"}`` float dict (0..1).
    ``None`` when the spreadsheet has no readable theme — callers
    fall back to a hard-coded default. Used to paint every Sheets-Table
    header chip (Dashboard + Tasks + History + UserEdits + Settings)
    and to tint the Dashboard's banded alt-rows so the whole sheet
    tracks the kid's chosen ``Format → Theme``.
    """

    sheet_id: int
    table_ids: list[str]
    banded_range_ids: list[int]
    conditional_format_rule_count: int
    protected_range_ids: list[int] = field(default_factory=list)
    theme_accent: dict[str, float] | None = None


class GoldSink(Protocol):
    """Surface the publish step needs from the spreadsheet backend."""

    def read_user_edits(self, spreadsheet_id: str) -> list[UserEdit]: ...

    def read_tab_raw(self, spreadsheet_id: str, tab_name: str) -> list[list[str]]: ...

    def write_tab(
        self,
        spreadsheet_id: str,
        tab: TabSpec,
        rows: list[tuple[object, ...]],
    ) -> None: ...

    def set_tab_hidden(self, spreadsheet_id: str, tab: TabSpec, hidden: bool) -> None: ...

    def read_dashboard_meta(self, spreadsheet_id: str) -> DashboardMeta: ...

    def write_dashboard_layout(
        self, spreadsheet_id: str, requests: list[dict[str, Any]]
    ) -> None: ...

    def write_dashboard_protection(
        self,
        spreadsheet_id: str,
        dashboard_sheet_id: int,
    ) -> None: ...


@dataclass(frozen=True)
class PublishResult:
    child: str
    tasks_written: int
    history_written: int
    user_edits_written: int


def publish_for_child(
    store: StateStore,
    sink: GoldSink,
    *,
    child: str,
    spreadsheet_id: str,
    tasks: list[Task],
    last_synced: datetime | None,
    cutoff_days: int = 30,
    future_date_cap_days: int = 365,
    source_auth_rows: list[SourceAuthRow] | None = None,
) -> PublishResult:
    """End-to-end publish for one child.

    Idempotent: re-running with the same silver state and the same
    sheet contents produces zero net changes.
    """
    # 1. Project silver into task rows, then blank out implausibly-future
    #    due dates (defence against source-side parser drift).
    task_rows = project_tasks_rows(tasks)
    task_rows = cap_future_dates(task_rows, cap_days=future_date_cap_days)

    # 2. Capture live kid overrides from both tabs *before* overwriting.
    #
    # Each capture call is scoped to the projection rows that BELONG in the
    # tab being read. If we passed the full ``task_rows`` to both calls, a
    # row that moved from Tasks→History this cycle (e.g. a newly-archived
    # row) would have its stale Tasks-tab cell compared against its new
    # History-tab projection, generating a phantom kid-edit that then
    # triggers ``apply_unarchive_edits`` and clears the archive flags every
    # sync. Pre-partition with an empty edit list to derive the silver-only
    # destination per row, then scope each capture accordingly.
    persisted_edits = sink.read_user_edits(spreadsheet_id)
    raw_tasks_rows = sink.read_tab_raw(spreadsheet_id, TASKS_TAB.name)
    raw_history_rows = sink.read_tab_raw(spreadsheet_id, HISTORY_TAB.name)
    silver_active_rows, silver_history_rows = partition_tasks(
        task_rows, tasks, [], cutoff_days=cutoff_days
    )
    live_task_edits = capture_tab_edits(raw_tasks_rows, silver_active_rows, tab=TASKS_TAB)
    live_history_edits = capture_tab_edits(raw_history_rows, silver_history_rows, tab=HISTORY_TAB)

    # 3. Merge all edit sources and apply superseded filter.
    user_edits = _merge_edit_sources([*live_task_edits, *live_history_edits], persisted_edits)
    user_edits = filter_superseded_edits(user_edits, tasks)

    # 3a. Honour kid-driven un-archive: a status edit moving a row from
    #     "Archived" to anything else clears the archive flags in silver and
    #     reprojects the row so it lands on the Tasks tab this cycle.
    tasks = apply_unarchive_edits(user_edits, tasks, store)
    # 3b. Honour kid-driven archive: a status edit setting "Archived" on
    #     a non-archived task writes the archive flags into silver so the
    #     row persists as archived across syncs and partitions to History.
    tasks = apply_archive_edits(user_edits, tasks, store)
    task_rows = project_tasks_rows(tasks)
    task_rows = cap_future_dates(task_rows, cap_days=future_date_cap_days)

    # 4. Apply edits to rows, then partition into active / history.
    merged_rows = merge_user_edits(task_rows, user_edits)
    active_rows, history_rows = partition_tasks(
        merged_rows, tasks, user_edits, cutoff_days=cutoff_days
    )

    # 5. Compute UserEdits writeback (canonical row-set against all rows).
    edits_writeback = diff_user_edits(merged_rows, user_edits, projected=task_rows)
    settings_rows = project_settings_rows(
        child=child,
        last_synced=last_synced,
        source_auth_rows=source_auth_rows,
    )

    # 6. Write tabs.
    sink.write_tab(spreadsheet_id, TASKS_TAB, [r.cells for r in active_rows])
    sink.write_tab(spreadsheet_id, HISTORY_TAB, [r.cells for r in history_rows])
    sink.write_tab(spreadsheet_id, SETTINGS_TAB, [tuple(p) for p in settings_rows])

    user_edits_tab = SCHEMA.by_name("UserEdits")
    sink.write_tab(
        spreadsheet_id,
        user_edits_tab,
        [
            (
                e.task_uid,
                e.column,
                _coerce_user_edit_value(e.original_value),
                _coerce_user_edit_value(e.value),
                e.updated_at,
            )
            for e in edits_writeback
        ],
    )

    # 7. Refresh the Dashboard's dynamic task-list Tables. The Dashboard
    # frame (greeting / floating KPI scorecards / donut) is template-owned
    # and stays put; this step re-emits the lists region sized to the
    # current active row count. Failures here are logged but don't fail
    # publish — the Tasks/History/Settings/UserEdits writes above are the
    # canonical state of the world, and the Dashboard lists will be
    # picked up on the next publish.
    try:
        meta = sink.read_dashboard_meta(spreadsheet_id)
        today = melbourne_local_date(datetime.now(UTC))
        assert today is not None  # datetime.now(UTC) is never None
        dash_tasks = task_rows_to_dashboard_tasks(active_rows, tasks=tasks)
        dash_requests = build_dashboard_requests(
            dash_sheet_id=meta.sheet_id,
            tasks=dash_tasks,
            today=today,
            existing_table_ids=meta.table_ids,
            existing_banded_range_ids=meta.banded_range_ids,
            existing_conditional_format_rule_count=meta.conditional_format_rule_count,
            theme_accent=meta.theme_accent,
            source_auth_rows=source_auth_rows,
        )
        sink.write_dashboard_layout(spreadsheet_id, dash_requests)
        # One-shot install of the whole-sheet Dashboard protection. The
        # ``addProtectedRange`` resource lives independently of the Tables
        # we tear down each sync, so a single install survives indefinitely.
        # Idempotent: only emitted when ``read_dashboard_meta`` reports no
        # existing whole-sheet protected range.
        if not meta.protected_range_ids:
            try:
                sink.write_dashboard_protection(spreadsheet_id, meta.sheet_id)
                log.info("dashboard protection installed for child=%s", child)
            except Exception:
                log.exception("dashboard protection install failed for child=%s", child)
    except Exception:
        log.exception("dashboard layout refresh failed for child=%s", child)

    return PublishResult(
        child=child,
        tasks_written=len(active_rows),
        history_written=len(history_rows),
        user_edits_written=len(edits_writeback),
    )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _coerce_user_edit_value(value: object) -> str:
    """Stringify a user-edit value for the hidden UserEdits tab."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if value is None:
        return ""
    return str(value)


def _connect(store: StateStore) -> sqlite3.Connection:
    conn = sqlite3.connect(store.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# Re-export internal types intentionally kept private at module level.
__all__ = [
    "MELBOURNE",
    "DashboardMeta",
    "GoldSink",
    "PublishResult",
    "TaskRow",
    "UserEdit",
    "apply_archive_edits",
    "apply_unarchive_edits",
    "capture_tab_edits",
    "checkpoint_uid",
    "diff_user_edits",
    "filter_superseded_edits",
    "format_melbourne_dmy",
    "format_melbourne_dmy_date",
    "melbourne_local_date",
    "merge_user_edits",
    "parent_uid_from_checkpoint",
    "partition_tasks",
    "project_settings_rows",
    "project_tasks_rows",
    "publish_for_child",
    "task_uid",
]

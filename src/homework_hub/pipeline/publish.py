"""Gold publish — project silver rows into per-tab table data.

The publish flow:

1. Read silver rows + auto-detected links for the child.
2. Read existing ``UserEdits`` rows + ``Possible Duplicates`` checkbox
   state from the spreadsheet via the :class:`GoldSink` protocol.
3. Apply checkbox state back to ``silver_task_links`` (kid confirmations
   from the previous sync are persisted before the next publish).
4. Project silver into per-tab row data using
   :func:`project_tasks_rows` / :func:`project_duplicates_rows` /
   :func:`project_settings_rows`.
5. Merge ``UserEdits`` over the editable columns on the Tasks tab.
6. Write Tasks, Possible Duplicates, Settings tabs through the sink.

This module owns ``Source``-display labels (``Compass``/``Classroom``/
``Edrolo``) and ``Status``-display labels (``Not started``/...). Date
columns are converted from UTC datetimes to Melbourne local dates so
the kid sees the date a task is actually due in their timezone.

The Protocol :class:`GoldSink` describes the surface ``publish_for_child``
needs from the sheet client; a fake implementation is used in tests.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from homework_hub.models import Source as SourceEnum
from homework_hub.models import Status, Task
from homework_hub.schema import (
    DUPLICATES_TAB,
    SCHEMA,
    SETTINGS_TAB,
    TASKS_TAB,
    TabSpec,
)
from homework_hub.state.store import StateStore

MELBOURNE = ZoneInfo("Australia/Melbourne")

# Display labels for the dropdown columns. Order matches schema vocab.
_SOURCE_DISPLAY: dict[str, str] = {
    SourceEnum.COMPASS.value: "Compass",
    SourceEnum.CLASSROOM.value: "Classroom",
    SourceEnum.EDROLO.value: "Edrolo",
}

_STATUS_DISPLAY: dict[str, str] = {
    Status.NOT_STARTED.value: "Not started",
    Status.IN_PROGRESS.value: "In progress",
    Status.SUBMITTED.value: "Submitted",
    Status.GRADED.value: "Graded",
    Status.OVERDUE.value: "Overdue",
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


def task_uid(task: Task) -> str:
    """Stable identifier for the UserEdits merge.

    Format: ``<source>:<source_id>``. Independent of child because each
    child's spreadsheet only ever holds their own tasks.
    """
    return f"{task.source.value}:{task.source_id}"


@dataclass(frozen=True)
class TaskRow:
    """One projected row for the Tasks tab. Cells are tab-column-ordered."""

    task_uid: str
    cells: tuple[object, ...]


@dataclass(frozen=True)
class DuplicateRow:
    """One projected row for the Possible Duplicates tab."""

    link_id: int
    cells: tuple[object, ...]


def project_tasks_rows(tasks: list[Task]) -> list[TaskRow]:
    """Project silver tasks into Tasks-tab row data.

    Editable columns ``priority`` and ``notes`` default to blank.
    ``done`` is derived from the task's status: ``True`` when the upstream
    LMS reports the task as Submitted or Graded, ``False`` otherwise.
    Kids can override it via the sheet; :func:`merge_user_edits` overlays
    those overrides afterwards.

    The ``Days`` column is written as a row-relative formula
    (``=C{row}-TODAY()``) so Sheets evaluates it as a number on every open.
    """
    rows: list[TaskRow] = []
    for t in tasks:
        cell_by_key = {
            "subject": t.subject or "",
            "title": t.title,
            "due": melbourne_local_date(t.due_at),
            "days": TASKS_TAB.columns[TASKS_TAB.column_index("days")].formula_template,
            "status": _STATUS_DISPLAY.get(t.status.value, t.status.value),
            "priority": "",
            "done": t.status in (Status.SUBMITTED, Status.GRADED),
            "notes": "",
            "source": _SOURCE_DISPLAY.get(t.source.value, t.source.value),
            "link": t.url,
            "task_uid": task_uid(t),
        }
        cells = tuple(cell_by_key[c.key] for c in TASKS_TAB.columns)
        rows.append(TaskRow(task_uid=task_uid(t), cells=cells))
    return rows


@dataclass(frozen=True)
class LinkProjectionInput:
    """Bundle of silver data needed to project one duplicate row."""

    link_id: int
    confidence: str  # auto_high | auto_medium | manual
    state: str  # pending | confirmed | dismissed
    subject: str
    compass_title: str
    compass_due: datetime | None
    classroom_title: str
    classroom_due: datetime | None


def project_duplicates_rows(links: list[LinkProjectionInput]) -> list[DuplicateRow]:
    """Project duplicate-link rows for the Possible Duplicates tab.

    Only ``state == 'pending'`` rows are surfaced; kids' confirm/dismiss
    decisions are persisted on silver_task_links so the row simply drops
    off the next publish.
    """
    rows: list[DuplicateRow] = []
    for link in links:
        if link.state != "pending":
            continue
        confidence_label = (
            "High"
            if link.confidence == "auto_high"
            else "Medium" if link.confidence == "auto_medium" else "Manual"
        )
        cell_by_key = {
            "link_id": str(link.link_id),
            "confidence": confidence_label,
            "subject": link.subject,
            "compass_title": link.compass_title,
            "compass_due": melbourne_local_date(link.compass_due),
            "classroom_title": link.classroom_title,
            "classroom_due": melbourne_local_date(link.classroom_due),
            "confirm": False,
            "dismiss": False,
        }
        cells = tuple(cell_by_key[c.key] for c in DUPLICATES_TAB.columns)
        rows.append(DuplicateRow(link_id=link.link_id, cells=cells))
    return rows


def project_settings_rows(*, child: str, last_synced: datetime | None) -> list[tuple[str, str]]:
    """Project the Settings tab key/value pairs."""
    last = melbourne_local_date(last_synced).isoformat() if last_synced is not None else "—"
    return [
        ("Child", child),
        ("Last synced (Melbourne date)", last),
        (
            "Last synced (UTC)",
            last_synced.isoformat() if last_synced is not None else "—",
        ),
        ("Tabs managed", ", ".join(t.name for t in SCHEMA.tabs if not t.hidden)),
    ]


# --------------------------------------------------------------------------- #
# UserEdits merge
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class UserEdit:
    """One persisted kid override for a (task_uid, column) pair."""

    task_uid: str
    column: str
    value: object  # already coerced to bool for checkboxes
    updated_at: str


def merge_user_edits(
    rows: list[TaskRow],
    edits: list[UserEdit],
) -> list[TaskRow]:
    """Overlay kid overrides on the editable columns of Tasks rows.

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
                new_cells[col_idx] = overrides[col.key]
        merged.append(TaskRow(task_uid=row.task_uid, cells=tuple(new_cells)))
    return merged


def diff_user_edits(
    rows: list[TaskRow],
    existing: list[UserEdit],
    projected: list[TaskRow] | None = None,
) -> list[UserEdit]:
    """Compute the canonical UserEdits row-set for the Tasks tab.

    For each editable cell that differs from its *projected* (system-derived)
    value we emit a ``UserEdit`` — this represents a deliberate kid override.
    Cells that match the projected value are not persisted, so ``UserEdits``
    stays small.

    ``projected`` is the pre-merge output of :func:`project_tasks_rows`.
    When omitted (backwards-compat) a static default of ``""`` / ``False``
    is used, which was the original behaviour before ``done`` became
    status-derived.  Callers should always supply it.

    Existing ``updated_at`` timestamps are preserved when the value did not
    change (avoids spurious churn on every publish).
    """
    editable_cols = TASKS_TAB.editable_columns()
    projected_by_uid: dict[str, TaskRow] = (
        {r.task_uid: r for r in projected} if projected else {}
    )
    _static_defaults: dict[str, object] = {"priority": "", "done": False, "notes": ""}
    existing_by_key = {(e.task_uid, e.column): e for e in existing}

    out: list[UserEdit] = []
    now = datetime.now(UTC).isoformat()
    for row in rows:
        proj_row = projected_by_uid.get(row.task_uid)
        for col in editable_cols:
            idx = TASKS_TAB.column_index(col.key)
            value = row.cells[idx]
            # Use the projected (system-derived) value as the baseline so a
            # done=True on a Submitted task is not treated as a kid override.
            if proj_row is not None:
                default = proj_row.cells[idx]
            else:
                default = _static_defaults.get(col.key, "")
            if value == default:
                continue
            prior = existing_by_key.get((row.task_uid, col.key))
            if prior is not None and prior.value == value:
                # Unchanged — preserve original updated_at.
                out.append(prior)
            else:
                out.append(
                    UserEdit(
                        task_uid=row.task_uid,
                        column=col.key,
                        value=value,
                        updated_at=now,
                    )
                )
    return out


# Sheets date-serial epoch: days since 30 Dec 1899.
_SHEETS_EPOCH = date(1899, 12, 30)


def _parse_tasks_tab_date(raw: str) -> date | None:
    """Parse a date cell from the Tasks tab as returned by ``get_all_values()``.

    Sheets returns the cell's *display* string, so we expect ``dd/MM/yyyy``
    (the format applied at bootstrap).  As a fallback we also handle the raw
    integer serial string that Sheets occasionally returns when the cell was
    written as a number rather than a formatted date.  Any other value (empty,
    unparseable) returns ``None`` so the caller can treat it as "no override".
    """
    raw = raw.strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d/%m/%Y").date()
    except ValueError:
        pass
    if raw.isdigit():
        return _SHEETS_EPOCH + timedelta(days=int(raw))
    return None


def capture_tasks_tab_edits(
    raw_rows: list[list[str]],
    projected: list[TaskRow],
) -> list[UserEdit]:
    """Detect kid overrides by comparing the live Tasks tab against projected defaults.

    Called with the raw string rows from ``get_all_values()[1:]`` (header
    stripped) before the tab is overwritten.  Joins each raw row to its
    projected counterpart by ``task_uid`` (last column), then for every
    editable column emits a :class:`UserEdit` when the cell value differs
    from the system default.

    Coercion per column kind:
    - ``CHECKBOX``  — ``"TRUE"`` → ``True``, ``"FALSE"`` → ``False``.
    - ``DATE``      — parsed via :func:`_parse_tasks_tab_date`; failures skipped.
    - ``DROPDOWN`` / ``TEXT`` — raw string; empty string skipped (= no override).
    """
    from homework_hub.schema import ColumnKind

    editable_cols = TASKS_TAB.editable_columns()
    uid_idx = TASKS_TAB.column_index("task_uid")
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
            col_idx = TASKS_TAB.column_index(col.key)
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

            out.append(UserEdit(task_uid=uid, column=col.key, value=value, updated_at=now))

    return out


def _merge_edit_sources(
    live: list[UserEdit],
    persisted: list[UserEdit],
) -> list[UserEdit]:
    """Combine live (Tasks tab) and persisted (UserEdits tab) edit lists.

    Live edits represent the kid's current state in the sheet and always
    take precedence over what was persisted from a previous sync.
    """
    merged: dict[tuple[str, str], UserEdit] = {
        (e.task_uid, e.column): e for e in persisted
    }
    for e in live:
        merged[(e.task_uid, e.column)] = e
    return list(merged.values())


def filter_superseded_edits(
    edits: list[UserEdit],
    tasks: list[Task],
) -> list[UserEdit]:
    """Drop kid overrides that silver's current state has superseded.

    Precedence rules:
    - ``status`` — silver ``Graded`` or ``Overdue`` locks the status column;
      kid cannot override these terminal states.
    - ``done``   — silver ``Graded`` locks ``done=True``; kid cannot un-tick.
    - ``due``    — silver wins whenever ``task.due_at`` is not ``None``; the
      kid override is only meaningful as a placeholder for a missing date.
    - ``priority`` / ``notes`` — kid always wins; no silver equivalent.
    """
    task_by_uid = {f"{t.source.value}:{t.source_id}": t for t in tasks}
    _terminal_status = {Status.GRADED, Status.OVERDUE}

    out: list[UserEdit] = []
    for edit in edits:
        task = task_by_uid.get(edit.task_uid)
        if task is None:
            # Task no longer in silver — orphan, will be pruned by diff_user_edits.
            out.append(edit)
            continue

        if edit.column == "status" and task.status in _terminal_status:
            continue  # Graded / Overdue lock the status column.

        if edit.column == "done" and task.status is Status.GRADED:
            continue  # Graded locks done=True; kid cannot un-tick.

        if edit.column == "due" and task.due_at is not None:
            continue  # Silver has a real date; kid placeholder is no longer needed.

        out.append(edit)

    return out


# --------------------------------------------------------------------------- #
# Possible-Duplicates checkbox readback
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DuplicateCheckboxState:
    """Checkbox state read from a Possible Duplicates row."""

    link_id: int
    confirm: bool
    dismiss: bool


def reconcile_link_state(state: DuplicateCheckboxState) -> str | None:
    """Map (confirm, dismiss) checkboxes to a silver_task_links state.

    Returns:
        ``'confirmed'`` if Confirm is ticked,
        ``'dismissed'`` if only Dismiss is ticked,
        ``None`` if neither ticked (no change).

    Both ticked → Confirm wins (kid clearly meant to merge).
    """
    if state.confirm:
        return "confirmed"
    if state.dismiss:
        return "dismissed"
    return None


# --------------------------------------------------------------------------- #
# State-store readers
# --------------------------------------------------------------------------- #


def load_links_for_publish(store: StateStore, child: str) -> list[LinkProjectionInput]:
    """Read silver_task_links + matching silver_tasks for projection.

    Joins each link to both silver rows so the publish layer has the
    titles and due dates needed without a second query.
    """
    with closing(_connect(store)) as conn:
        rows = conn.execute(
            "SELECT l.id, l.confidence, l.state, "
            "  ps.subject_canonical AS subject, "
            "  ps.title AS compass_title, ps.due_at AS compass_due, "
            "  ks.title AS classroom_title, ks.due_at AS classroom_due "
            "FROM silver_task_links l "
            "JOIN silver_tasks ps "
            "  ON ps.child = l.child AND ps.source = l.primary_source "
            " AND ps.source_id = l.primary_source_id "
            "JOIN silver_tasks ks "
            "  ON ks.child = l.child AND ks.source = l.secondary_source "
            " AND ks.source_id = l.secondary_source_id "
            "WHERE l.child = ? "
            "ORDER BY l.confidence DESC, l.id ASC",
            (child,),
        ).fetchall()

    return [
        LinkProjectionInput(
            link_id=int(r["id"]),
            confidence=r["confidence"],
            state=r["state"],
            subject=r["subject"] or "",
            compass_title=r["compass_title"] or "",
            compass_due=(datetime.fromisoformat(r["compass_due"]) if r["compass_due"] else None),
            classroom_title=r["classroom_title"] or "",
            classroom_due=(
                datetime.fromisoformat(r["classroom_due"]) if r["classroom_due"] else None
            ),
        )
        for r in rows
    ]


def apply_link_state_writebacks(
    store: StateStore,
    states: list[DuplicateCheckboxState],
) -> int:
    """Persist checkbox-driven state changes to silver_task_links.

    Returns the count of rows updated.
    """
    if not states:
        return 0
    updated = 0
    with closing(_connect(store)) as conn, conn:
        for s in states:
            new_state = reconcile_link_state(s)
            if new_state is None:
                continue
            cur = conn.execute(
                "UPDATE silver_task_links SET state = ? WHERE id = ?",
                (new_state, s.link_id),
            )
            updated += cur.rowcount
    return updated


# --------------------------------------------------------------------------- #
# Sink protocol + publish entry point
# --------------------------------------------------------------------------- #


class GoldSink(Protocol):
    """Surface the publish step needs from the spreadsheet backend."""

    def read_user_edits(self, spreadsheet_id: str) -> list[UserEdit]: ...

    def read_duplicate_checkboxes(self, spreadsheet_id: str) -> list[DuplicateCheckboxState]: ...

    def read_tab_raw(self, spreadsheet_id: str, tab_name: str) -> list[list[str]]: ...

    def write_tab(
        self,
        spreadsheet_id: str,
        tab: TabSpec,
        rows: list[tuple[object, ...]],
    ) -> None: ...

    def set_tab_hidden(self, spreadsheet_id: str, tab: TabSpec, hidden: bool) -> None: ...


@dataclass(frozen=True)
class PublishResult:
    child: str
    tasks_written: int
    duplicates_written: int
    duplicates_state_updates: int
    user_edits_written: int


def publish_for_child(
    store: StateStore,
    sink: GoldSink,
    *,
    child: str,
    spreadsheet_id: str,
    tasks: list[Task],
    last_synced: datetime | None,
) -> PublishResult:
    """End-to-end publish for one child.

    Idempotent: re-running with the same silver state and the same
    sheet contents produces zero net changes.
    """
    # 1. Persist last sync's checkbox decisions BEFORE re-reading links,
    #    so confirmed/dismissed pairs drop out of this publish.
    checkbox_states = sink.read_duplicate_checkboxes(spreadsheet_id)
    state_updates = apply_link_state_writebacks(store, checkbox_states)

    # 2. Read silver-derived link projections for the kid.
    link_inputs = load_links_for_publish(store, child)

    # 3. Project rows and capture kid overrides from the live Tasks tab
    #    *before* it is overwritten.
    task_rows = project_tasks_rows(tasks)
    persisted_edits = sink.read_user_edits(spreadsheet_id)
    raw_tasks_rows = sink.read_tab_raw(spreadsheet_id, TASKS_TAB.name)
    live_edits = capture_tasks_tab_edits(raw_tasks_rows, task_rows)
    user_edits = _merge_edit_sources(live_edits, persisted_edits)
    user_edits = filter_superseded_edits(user_edits, tasks)
    merged_rows = merge_user_edits(task_rows, user_edits)
    duplicate_rows = project_duplicates_rows(link_inputs)
    settings_rows = project_settings_rows(child=child, last_synced=last_synced)

    # 4. Compute UserEdits writeback (canonical row-set).
    edits_writeback = diff_user_edits(merged_rows, user_edits, projected=task_rows)

    # 5. Write tabs.
    sink.write_tab(spreadsheet_id, TASKS_TAB, [r.cells for r in merged_rows])
    sink.write_tab(spreadsheet_id, DUPLICATES_TAB, [r.cells for r in duplicate_rows])
    sink.write_tab(spreadsheet_id, SETTINGS_TAB, [tuple(p) for p in settings_rows])

    user_edits_tab = SCHEMA.by_name("UserEdits")
    sink.write_tab(
        spreadsheet_id,
        user_edits_tab,
        [
            (e.task_uid, e.column, _coerce_user_edit_value(e.value), e.updated_at)
            for e in edits_writeback
        ],
    )

    # 6. Auto-hide the Possible Duplicates tab when there's nothing to confirm.
    sink.set_tab_hidden(spreadsheet_id, DUPLICATES_TAB, hidden=not duplicate_rows)

    return PublishResult(
        child=child,
        tasks_written=len(merged_rows),
        duplicates_written=len(duplicate_rows),
        duplicates_state_updates=state_updates,
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
    "DuplicateCheckboxState",
    "DuplicateRow",
    "GoldSink",
    "LinkProjectionInput",
    "PublishResult",
    "TaskRow",
    "UserEdit",
    "apply_link_state_writebacks",
    "capture_tasks_tab_edits",
    "diff_user_edits",
    "filter_superseded_edits",
    "load_links_for_publish",
    "melbourne_local_date",
    "merge_user_edits",
    "project_duplicates_rows",
    "project_settings_rows",
    "project_tasks_rows",
    "publish_for_child",
    "reconcile_link_state",
    "task_uid",
]

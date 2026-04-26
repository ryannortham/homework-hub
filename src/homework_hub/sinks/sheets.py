"""Pure logic for the Sheets sink.

Splits cleanly into two layers:

- This module: pure functions that build request payloads / row matrices.
  Fully unit-testable, no Google API calls.
- A thin API client (added in phase 9) that submits those payloads via gspread.
"""

from __future__ import annotations

from datetime import UTC, datetime

from homework_hub.models import Task

# Column order for the Raw tab. The script writes only this tab; everything
# else in the sheet is formula-driven so kids' manual edits survive.
RAW_HEADERS: list[str] = [
    "child",
    "source",
    "source_id",
    "subject",
    "title",
    "description",
    "assigned_at",
    "due_at",
    "status",
    "status_raw",
    "url",
    "last_synced",
]


def task_to_row(task: Task) -> list[str]:
    """Project a Task to the Raw-tab row order.

    Datetimes are emitted as ISO-8601 strings in UTC with trailing 'Z'.
    None values become empty strings.
    """
    return [
        task.child,
        task.source.value,
        task.source_id,
        task.subject,
        task.title,
        task.description,
        _fmt_dt(task.assigned_at),
        _fmt_dt(task.due_at),
        task.status.value,
        task.status_raw,
        task.url,
        _fmt_dt(task.last_synced),
    ]


def tasks_to_matrix(tasks: list[Task]) -> list[list[str]]:
    """Build the full Raw-tab matrix including the header row."""
    return [RAW_HEADERS, *(task_to_row(t) for t in tasks)]


def _fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

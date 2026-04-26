"""Pure positional upsert logic for the Raw tab.

The Tasks tab on each child's sheet uses ``ARRAYFORMULA`` to mirror Raw rows
into columns A-L, while kids' editable columns (Notes, Priority, Manual
Status) live in adjacent columns indexed *positionally* by row.

That means the row order on Raw must be **stable across syncs**:

* A task already on Raw must stay on the same row, even when the source
  reorders/refreshes its task list. Kids' notes stay anchored.
* A new task is appended at the bottom.
* A task that's no longer in the source's response stays on Raw forever
  (per design: kids may still want their notes; the row may also reappear
  on a future sync if the source had a transient gap).

This module owns just that diff-and-merge logic; the gspread API layer
(``sheets_client.py``) calls into it.
"""

from __future__ import annotations

from dataclasses import dataclass

from homework_hub.models import Task
from homework_hub.sinks.sheets import RAW_HEADERS, task_to_row


def _row_key(row: list[str]) -> tuple[str, str, str] | None:
    """Extract the (child, source, source_id) dedup key from a Raw row.

    Returns ``None`` for rows that can't be keyed (e.g. blank trailing rows
    or partially-populated junk left by a bad migration).
    """
    if len(row) < 3:
        return None
    child, source, source_id = (
        (row[0] or "").strip(),
        (row[1] or "").strip(),
        (row[2] or "").strip(),
    )
    if not child or not source or not source_id:
        return None
    return (child, source, source_id)


@dataclass(frozen=True)
class RawDiff:
    """Result of computing the Raw-tab diff for one sync.

    The Sheets API client uses ``updates`` to issue per-row updates and
    ``appends`` for a single batch append at the bottom. ``unchanged_keys``
    is informational (also handy in tests).
    """

    # 1-based row indices on Raw (header is row 1, first data row is row 2)
    # → list of new cell values, in RAW_HEADERS order. Used for in-place
    # row replacement.
    updates: dict[int, list[str]]
    # New rows to append below the last currently-occupied row, in order.
    appends: list[list[str]]
    # source_id keys for tasks that were already on Raw with identical values
    # (no API write needed). Surfaced for tests + observability.
    unchanged_keys: list[tuple[str, str, str]]
    # The row index immediately *after* the last data row currently on Raw.
    # Sheets writes use this for the append range.
    next_append_row: int


def compute_raw_diff(
    *,
    existing_rows: list[list[str]],
    incoming: list[Task],
) -> RawDiff:
    """Diff incoming tasks against the current Raw-tab state.

    Args:
        existing_rows: Every row currently on Raw, *including* the header at
            index 0. Each inner list may be of any length (Sheets returns
            short lists when trailing cells are blank); we treat missing
            cells as "".
        incoming: The freshly-fetched task set (any combination of children
            and sources).

    Returns:
        A ``RawDiff`` describing the minimal write the API client needs to
        perform.
    """
    # Build an index of currently-occupied rows by their dedup key. Row
    # numbers are 1-based to match the Sheets API; row 1 is the header so
    # data starts at row 2.
    key_to_row: dict[tuple[str, str, str], int] = {}
    for idx, row in enumerate(existing_rows):
        if idx == 0:
            continue  # header
        key = _row_key(row)
        if key is None:
            continue
        # First occurrence wins; later duplicates (shouldn't happen) are
        # left alone — we'll overwrite the first one and leave the second
        # in place rather than risk shifting positions.
        key_to_row.setdefault(key, idx + 1)  # +1 because Sheets is 1-based

    # The next free row for appends is one past the largest existing index.
    # Sheets rows are 1-based; existing_rows[i] corresponds to sheet row i+1.
    # If only the header is present (len 1), the next free row is 2.
    next_append_row = max(2, len(existing_rows) + 1)

    updates: dict[int, list[str]] = {}
    appends: list[list[str]] = []
    unchanged: list[tuple[str, str, str]] = []

    # Track keys we've already processed in this batch to avoid double-writing
    # if the same task is yielded twice by an upstream bug.
    seen: set[tuple[str, str, str]] = set()

    for task in incoming:
        key = task.dedup_key
        if key in seen:
            continue
        seen.add(key)

        new_row = task_to_row(task)
        if key in key_to_row:
            row_num = key_to_row[key]
            current = existing_rows[row_num - 1]
            if _rows_equal(current, new_row):
                unchanged.append(key)
            else:
                updates[row_num] = new_row
        else:
            appends.append(new_row)

    return RawDiff(
        updates=updates,
        appends=appends,
        unchanged_keys=unchanged,
        next_append_row=next_append_row,
    )


def _rows_equal(current: list[str], new_row: list[str]) -> bool:
    """Compare two Raw rows column-wise, treating short rows as ``""``-padded.

    The ``last_synced`` column (last in RAW_HEADERS) changes every sync, so
    it's excluded from equality — otherwise every task would always look
    'changed' and we'd write the entire sheet on every sync.
    """
    sync_col = len(RAW_HEADERS) - 1  # index of last_synced
    for i in range(len(RAW_HEADERS)):
        if i == sync_col:
            continue
        a = current[i] if i < len(current) else ""
        b = new_row[i] if i < len(new_row) else ""
        if (a or "") != (b or ""):
            return False
    return True

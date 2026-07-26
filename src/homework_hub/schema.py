"""Per-column schema for the Gold publish layer.

This module is the single source of truth for what columns appear on
which tab, in what order, with what type. Both the bootstrap-sheet
(M5c) and the publish step (M5) consume these specs to keep tab layout
and write-time formatting in lockstep — change a column here and both
sides pick it up automatically.

No I/O, no Google API imports — pure data classes so tests stay cheap
and the module is safe to import from anywhere.

Tab layout
----------

* **Dashboard** — read-only formula-driven landing page. Greeting + last-sync
  pulled from Settings via ``VLOOKUP``, KPI counters via ``COUNTIFS`` against
  ``tbl_tasks``, three 10-row task lists (Overdue / Due this week / Upcoming)
  built from per-row ``IFERROR(INDEX(SORT(FILTER(...))))`` slabs, and a "By
  subject" block (spilled ``UNIQUE`` + ``COUNTIFS`` + ``SPARKLINE``). No kid
  edits, no publish writes — purely formula-owned.
* **Tasks** — the kid-facing Table for active homework. Some columns mirror
  silver (read-only after publish), others are kid-editable and persisted
  via ``UserEdits`` merge. Native Sheets Table ``tbl_tasks``.
* **History** — submitted/graded tasks older than ``history_cutoff_days``.
  Only Status and Notes are editable here. Native Sheets Table
  ``tbl_history``.
* **Settings** — key/value display tab; kids never edit it.
* **UserEdits** — hidden tab, script-managed. Merge target for kid
  edits to the editable columns on the Tasks and History tabs. Native
  Sheets Table ``tbl_user_edits``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

# --------------------------------------------------------------------------- #
# Column primitives
# --------------------------------------------------------------------------- #


class ColumnKind(StrEnum):
    """Sheets-level cell type for a column.

    The publish layer translates each ``ColumnKind`` into the appropriate
    ``CellFormat`` + ``DataValidationRule`` batchUpdate request.
    """

    TEXT = "text"
    DATE = "date"  # Melbourne local date; written as a Sheets DATE serial
    NUMBER = "number"
    CHECKBOX = "checkbox"
    DROPDOWN = "dropdown"
    FORMULA = "formula"  # value supplied by a per-row formula template


@dataclass(frozen=True)
class ColumnSpec:
    """A single column on a tab.

    Attributes
    ----------
    key:
        Stable machine identifier used by ``publish`` to locate the
        column when projecting silver rows.
    header:
        The literal string written into row 1 of the tab.
    kind:
        Sheets-level cell type (see :class:`ColumnKind`).
    editable:
        ``True`` if kids can edit the cell after publish. The
        ``UserEdits`` tab persists overrides for these columns only.
    dropdown_values:
        For ``DROPDOWN`` columns, the fixed list of allowed values.
        Empty for non-dropdown columns.
    formula_template:
        For ``FORMULA`` columns, the per-row template. ``{row}`` is
        substituted with the 1-based row number at write time, e.g.
        ``"=D{row}-TODAY()"`` where D is the absolute column letter for Due.
    width_px:
        Optional column-width hint applied at bootstrap time. ``None``
        leaves Sheets' default (100px).
    """

    key: str
    header: str
    kind: ColumnKind
    editable: bool = False
    dropdown_values: tuple[str, ...] = ()
    formula_template: str = ""
    width_px: int | None = None

    def __post_init__(self) -> None:
        if self.kind is ColumnKind.DROPDOWN and not self.dropdown_values:
            raise ValueError(f"ColumnSpec {self.key!r}: DROPDOWN columns require dropdown_values")
        if self.kind is ColumnKind.FORMULA and not self.formula_template:
            raise ValueError(f"ColumnSpec {self.key!r}: FORMULA columns require formula_template")
        if self.kind is not ColumnKind.DROPDOWN and self.dropdown_values:
            raise ValueError(f"ColumnSpec {self.key!r}: dropdown_values only valid for DROPDOWN")


@dataclass(frozen=True)
class TabSpec:
    """A single tab/worksheet in the kid-facing spreadsheet."""

    name: str
    columns: tuple[ColumnSpec, ...]
    table_id: str = ""  # native Sheets Table id; empty = no Table
    hidden: bool = False
    frozen_rows: int = 1
    description: str = ""

    @property
    def header_row(self) -> tuple[str, ...]:
        return tuple(c.header for c in self.columns)

    def column_index(self, key: str) -> int:
        """0-based index of a column by key. Raises if not found."""
        for i, c in enumerate(self.columns):
            if c.key == key:
                return i
        raise KeyError(f"Tab {self.name!r}: no column with key {key!r}")

    def editable_columns(self) -> tuple[ColumnSpec, ...]:
        return tuple(c for c in self.columns if c.editable)


# --------------------------------------------------------------------------- #
# Fixed dropdown vocabularies
# --------------------------------------------------------------------------- #

# Native Sheets Table ids for the Dashboard lists. The Tables are
# emitted at publish time (sized to actual data) rather than at template
# time, but the ids are declared here so callers can refer to them by
# stable name when tearing down / re-creating.
DASHBOARD_OVERDUE_TABLE_ID = "tbl_dash_overdue"
DASHBOARD_WEEK_TABLE_ID = "tbl_dash_week"
DASHBOARD_NO_DUE_DATE_TABLE_ID = "tbl_dash_no_due_date"
DASHBOARD_UPCOMING_TABLE_ID = "tbl_dash_upcoming"
DASHBOARD_DONE_TABLE_ID = "tbl_dash_done"

# Human-visible section names — used as the Table's ``name`` so the
# section identity is conveyed via the Table label itself.
DASHBOARD_OVERDUE_TABLE_NAME = "Overdue"
DASHBOARD_WEEK_TABLE_NAME = "DueThisWeek"
DASHBOARD_NO_DUE_DATE_TABLE_NAME = "NoDueDate"
DASHBOARD_UPCOMING_TABLE_NAME = "Upcoming"
DASHBOARD_DONE_TABLE_NAME = "DoneThisWeek"

DASHBOARD_TABLE_IDS: tuple[str, ...] = (
    DASHBOARD_OVERDUE_TABLE_ID,
    DASHBOARD_WEEK_TABLE_ID,
    DASHBOARD_NO_DUE_DATE_TABLE_ID,
    DASHBOARD_UPCOMING_TABLE_ID,
    DASHBOARD_DONE_TABLE_ID,
)


SOURCE_VALUES: tuple[str, ...] = ("Compass", "Classroom", "EP", "Edrolo")

# Status mirrors homework_hub.models.Status display labels. Editable on
# both Tasks and History tabs so kids can manually update completion state.
STATUS_VALUES: tuple[str, ...] = (
    "Not started",
    "In progress",
    "Submitted",
    "Graded",
    "Overdue",
    "Archived",
)

# Task type mirrors homework_hub.models.TaskType display labels. Read-only —
# derived from the upstream LMS badge; kids cannot change it.
TYPE_VALUES: tuple[str, ...] = ("Assessment", "Homework", "General")


# --------------------------------------------------------------------------- #
# Shared column tuple for Tasks and History (identical structure)
# --------------------------------------------------------------------------- #
#
# Both tabs share the same 10-column layout:
#   Subject(A), Type(B), Title(C), Due(D), Days(E), Status(F),
#   Notes(G), Source(H), Link(I), task_uid(J)
#
# The Days formula references Due=D (col 4) and Status=F (col 6).

_DAYS_FORMULA = (
    '=IF(OR(D{row}="",F{row}="Submitted",F{row}="Graded",F{row}="Archived"),' '"",D{row}-TODAY())'
)


def _task_columns(*, due_editable: bool) -> tuple[ColumnSpec, ...]:
    """Build the 10-column task column tuple.

    ``due_editable`` controls whether the Due column accepts kid overrides.
    Tasks tab: True. History tab: False (date is locked once in History).
    """
    return (
        ColumnSpec(key="subject", header="Subject", kind=ColumnKind.TEXT, width_px=120),
        ColumnSpec(
            key="task_type",
            header="Type",
            kind=ColumnKind.DROPDOWN,
            dropdown_values=TYPE_VALUES,
            width_px=110,
        ),
        ColumnSpec(key="title", header="Title", kind=ColumnKind.TEXT, width_px=300),
        ColumnSpec(
            key="due",
            header="Due",
            kind=ColumnKind.DATE,
            editable=due_editable,
            width_px=110,
        ),
        ColumnSpec(
            key="days",
            header="Days",
            kind=ColumnKind.FORMULA,
            # Blank when no due date or already submitted/graded — only
            # relevant for unsubmitted work with a due date.
            # Due=D(col 4), Status=F(col 6).
            formula_template=_DAYS_FORMULA,
            width_px=70,
        ),
        ColumnSpec(
            key="status",
            header="Status",
            kind=ColumnKind.DROPDOWN,
            dropdown_values=STATUS_VALUES,
            editable=True,
            width_px=120,
        ),
        ColumnSpec(
            key="notes",
            header="Notes",
            kind=ColumnKind.TEXT,
            editable=True,
            width_px=280,
        ),
        ColumnSpec(
            key="source",
            header="Source",
            kind=ColumnKind.DROPDOWN,
            dropdown_values=SOURCE_VALUES,
            width_px=110,
        ),
        ColumnSpec(key="link", header="Link", kind=ColumnKind.TEXT, width_px=80),
        ColumnSpec(key="task_uid", header="task_uid", kind=ColumnKind.TEXT, width_px=0),
    )


# --------------------------------------------------------------------------- #
# Tab definitions
# --------------------------------------------------------------------------- #

# Tasks — active homework kid-facing Table.
TASKS_TAB = TabSpec(
    name="Tasks",
    table_id="tbl_tasks",
    description="Active homework. Kids can edit Due, Status and Notes.",
    columns=_task_columns(due_editable=True),
)


# History — submitted/graded tasks beyond the cutoff window.
HISTORY_TAB = TabSpec(
    name="History",
    table_id="tbl_history",
    description="Submitted/graded tasks older than the history cutoff. Status and Notes editable.",
    columns=_task_columns(due_editable=False),
)


# Dashboard — kid-facing landing page. Pure formula tab: every cell is
# written by ``sheet_template._write_dashboard_layout`` via ``updateCells``
# (greeting, KPI counters, list slabs, by-subject block). No headers at
# the schema level — the layout owns row 1 too.
#
# v4.3 bordered-canvas layout: 6 columns A..F. Columns A and F are 8px
# border columns (left + right gutter). Columns B-E host the content:
#
#   A=8 border │ B=256 Subject │ C=512 Title │ D=128 Due │ E=128 Status │ F=8 border
#
# Border columns also frame the greeting / section headers / footer by
# being skipped over via ``startColumnIndex=1, endColumnIndex=5`` in the
# layout writer.
DASHBOARD_TAB = TabSpec(
    name="Dashboard",
    description="Auto-built landing page: greeting, KPIs, task lists. Read-only.",
    columns=(
        ColumnSpec(key="col_a", header="", kind=ColumnKind.TEXT, width_px=8),
        ColumnSpec(key="col_b", header="", kind=ColumnKind.TEXT, width_px=256),
        ColumnSpec(key="col_c", header="", kind=ColumnKind.TEXT, width_px=512),
        ColumnSpec(key="col_d", header="", kind=ColumnKind.TEXT, width_px=128),
        ColumnSpec(key="col_e", header="", kind=ColumnKind.TEXT, width_px=128),
        ColumnSpec(key="col_f", header="", kind=ColumnKind.TEXT, width_px=8),
    ),
    frozen_rows=0,
)


# Settings — per-source auth/sync status table.
#
# Header row + N source rows + trailer rows (Child, Last full sync, Tabs
# managed). Plain (non-Table) tab so we can mix the header row with the
# child-info trailer without forcing every row to share the same schema.
SETTINGS_TAB = TabSpec(
    name="Settings",
    description="Per-source auth + sync status. Read-only.",
    columns=(
        ColumnSpec(key="source", header="Source", kind=ColumnKind.TEXT, width_px=140),
        ColumnSpec(key="last_synced", header="Last Synced", kind=ColumnKind.TEXT, width_px=180),
        ColumnSpec(key="token_expires", header="Token Expires", kind=ColumnKind.TEXT, width_px=180),
        ColumnSpec(key="status", header="Status", kind=ColumnKind.TEXT, width_px=120),
    ),
)


# UserEdits — hidden, script-managed. One row per (task_uid, column) edit.
# Publish reads this tab before writing Tasks/History so kid edits survive a resync.
USER_EDITS_TAB = TabSpec(
    name="UserEdits",
    table_id="tbl_user_edits",
    description="Persisted kid overrides; merge target for editable columns.",
    hidden=True,
    columns=(
        ColumnSpec(key="task_uid", header="task_uid", kind=ColumnKind.TEXT),
        ColumnSpec(key="column", header="column", kind=ColumnKind.TEXT),
        ColumnSpec(key="original_value", header="original_value", kind=ColumnKind.TEXT),
        ColumnSpec(key="value", header="value", kind=ColumnKind.TEXT),
        ColumnSpec(key="updated_at", header="updated_at", kind=ColumnKind.TEXT),
    ),
)


# DashboardData — hidden helper tab. Holds the 4 KPI label+value pairs
# (label in col A, COUNTIF formula in col B) consumed by the Dashboard's
# floating scorecard and donut charts. Charts require their data to live
# in actual cells; this tab keeps that plumbing out of the visible
# Dashboard grid and away from any user-managed tab.
DASHBOARD_DATA_TAB = TabSpec(
    name="DashboardData",
    description="Hidden helper cells sourcing Dashboard charts.",
    hidden=True,
    columns=(
        ColumnSpec(key="label", header="", kind=ColumnKind.TEXT, width_px=160),
        ColumnSpec(key="value", header="", kind=ColumnKind.TEXT, width_px=80),
    ),
    frozen_rows=0,
)


# --------------------------------------------------------------------------- #
# Aggregate
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SheetSchema:
    """The full set of tabs in a kid spreadsheet, in display order."""

    tabs: tuple[TabSpec, ...] = field(
        default=(
            DASHBOARD_TAB,
            TASKS_TAB,
            HISTORY_TAB,
            SETTINGS_TAB,
            USER_EDITS_TAB,
            DASHBOARD_DATA_TAB,
        )
    )

    def by_name(self, name: str) -> TabSpec:
        for t in self.tabs:
            if t.name == name:
                return t
        raise KeyError(f"No tab named {name!r}")


SCHEMA = SheetSchema()

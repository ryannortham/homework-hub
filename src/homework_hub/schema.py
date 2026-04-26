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

* **Today** — read-only ``QUERY()`` view of ``tbl_tasks`` filtered to
  due-today/overdue rows. Owned by formula; no kid edits.
* **Tasks** — the kid-facing Table. Some columns mirror silver
  (read-only after publish), others are kid-editable and persisted via
  ``UserEdits`` merge. Native Sheets Table ``tbl_tasks``.
* **Possible Duplicates** — Compass↔Classroom auto-link rows. Two
  ``Confirm`` / ``Dismiss`` checkboxes per row drive ``state`` writeback
  on the next sync. Auto-hidden when empty. Native Sheets Table
  ``tbl_duplicates``.
* **Settings** — key/value display tab; kids never edit it.
* **UserEdits** — hidden tab, script-managed. Merge target for kid
  edits to the editable columns on the Tasks tab. Native Sheets Table
  ``tbl_user_edits``.
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
        ``"=C{row}-TODAY()"`` where C is the absolute column letter for Due.
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

SOURCE_VALUES: tuple[str, ...] = ("Compass", "Classroom", "Edrolo")

# Status mirrors homework_hub.models.Status display labels. Read-only on
# the Tasks tab — the dropdown is informational + visual only.
STATUS_VALUES: tuple[str, ...] = (
    "Not started",
    "In progress",
    "Submitted",
    "Graded",
    "Overdue",
)

PRIORITY_VALUES: tuple[str, ...] = ("", "Low", "Med", "High")

CONFIRM_DISMISS_VALUES: tuple[str, ...] = ("", "Confirm", "Dismiss")  # unused; we use checkbox


# --------------------------------------------------------------------------- #
# Tab definitions
# --------------------------------------------------------------------------- #

# Tasks — the kid-facing Table.
TASKS_TAB = TabSpec(
    name="Tasks",
    table_id="tbl_tasks",
    description="All current homework. Kids can edit Priority and Notes.",
    columns=(
        ColumnSpec(key="subject", header="Subject", kind=ColumnKind.TEXT, width_px=120),
        ColumnSpec(key="title", header="Title", kind=ColumnKind.TEXT, width_px=320),
        ColumnSpec(key="due", header="Due", kind=ColumnKind.DATE, width_px=110),
        ColumnSpec(
            key="days",
            header="Days",
            kind=ColumnKind.FORMULA,
            formula_template="=C{row}-TODAY()",
            width_px=70,
        ),
        ColumnSpec(
            key="status",
            header="Status",
            kind=ColumnKind.DROPDOWN,
            dropdown_values=STATUS_VALUES,
            width_px=120,
        ),
        ColumnSpec(
            key="priority",
            header="Priority",
            kind=ColumnKind.DROPDOWN,
            dropdown_values=PRIORITY_VALUES,
            editable=True,
            width_px=90,
        ),
        ColumnSpec(
            key="done",
            header="Done",
            kind=ColumnKind.CHECKBOX,
            editable=True,
            width_px=60,
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
        # Stable identity for UserEdits merge — hidden in the UI but
        # required so we can join silver writes against kid overrides.
        ColumnSpec(key="task_uid", header="task_uid", kind=ColumnKind.TEXT, width_px=0),
    ),
)


# Today — pure formula tab. The single QUERY() formula in A1 builds the
# whole view from tbl_tasks; no per-column data writes ever land here.
TODAY_TAB = TabSpec(
    name="Today",
    description="Auto-built view: due today + overdue. Read-only.",
    columns=(
        ColumnSpec(
            key="query",
            header="",
            kind=ColumnKind.FORMULA,
            # Filter Tasks Table by Days <= 0 (today + overdue), excluding
            # rows already marked Done. Sort by Days ascending so most-
            # overdue items float to the top.
            formula_template=(
                '=QUERY(tbl_tasks, "select Subject, Title, Due, Days, Status, '
                'Priority, Source where Days <= 0 and Done = false order by Days asc", 1)'
            ),
        ),
    ),
    frozen_rows=0,
)


# Possible Duplicates — Compass↔Classroom auto-links.
DUPLICATES_TAB = TabSpec(
    name="Possible Duplicates",
    table_id="tbl_duplicates",
    description="Confirm/Dismiss to merge or keep apart. Auto-hidden when empty.",
    columns=(
        ColumnSpec(key="link_id", header="link_id", kind=ColumnKind.TEXT, width_px=0),
        ColumnSpec(
            key="confidence",
            header="Confidence",
            kind=ColumnKind.TEXT,
            width_px=110,
        ),
        ColumnSpec(key="subject", header="Subject", kind=ColumnKind.TEXT, width_px=130),
        ColumnSpec(
            key="compass_title",
            header="Compass Title",
            kind=ColumnKind.TEXT,
            width_px=260,
        ),
        ColumnSpec(
            key="compass_due",
            header="Compass Due",
            kind=ColumnKind.DATE,
            width_px=110,
        ),
        ColumnSpec(
            key="classroom_title",
            header="Classroom Title",
            kind=ColumnKind.TEXT,
            width_px=260,
        ),
        ColumnSpec(
            key="classroom_due",
            header="Classroom Due",
            kind=ColumnKind.DATE,
            width_px=110,
        ),
        ColumnSpec(
            key="confirm",
            header="Confirm",
            kind=ColumnKind.CHECKBOX,
            editable=True,
            width_px=80,
        ),
        ColumnSpec(
            key="dismiss",
            header="Dismiss",
            kind=ColumnKind.CHECKBOX,
            editable=True,
            width_px=80,
        ),
    ),
)


# Settings — simple key/value display.
SETTINGS_TAB = TabSpec(
    name="Settings",
    description="Sync metadata. Read-only.",
    columns=(
        ColumnSpec(key="key", header="Key", kind=ColumnKind.TEXT, width_px=180),
        ColumnSpec(key="value", header="Value", kind=ColumnKind.TEXT, width_px=420),
    ),
)


# UserEdits — hidden, script-managed. One row per (task_uid, column) edit.
# Publish reads this tab before writing Tasks so kid edits survive a resync.
USER_EDITS_TAB = TabSpec(
    name="UserEdits",
    table_id="tbl_user_edits",
    description="Persisted kid overrides; merge target for editable columns.",
    hidden=True,
    columns=(
        ColumnSpec(key="task_uid", header="task_uid", kind=ColumnKind.TEXT),
        ColumnSpec(key="column", header="column", kind=ColumnKind.TEXT),
        ColumnSpec(key="value", header="value", kind=ColumnKind.TEXT),
        ColumnSpec(key="updated_at", header="updated_at", kind=ColumnKind.TEXT),
    ),
)


# --------------------------------------------------------------------------- #
# Aggregate
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SheetSchema:
    """The full set of tabs in a kid spreadsheet, in display order."""

    tabs: tuple[TabSpec, ...] = field(
        default=(TODAY_TAB, TASKS_TAB, DUPLICATES_TAB, SETTINGS_TAB, USER_EDITS_TAB)
    )

    def by_name(self, name: str) -> TabSpec:
        for t in self.tabs:
            if t.name == name:
                return t
        raise KeyError(f"No tab named {name!r}")


SCHEMA = SheetSchema()

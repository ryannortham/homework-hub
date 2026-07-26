"""Tests for the Gold-publish ColumnSpec schema (M5b)."""

from __future__ import annotations

import pytest

from homework_hub.schema import (
    DASHBOARD_TAB,
    SCHEMA,
    SETTINGS_TAB,
    SOURCE_VALUES,
    STATUS_VALUES,
    TASKS_TAB,
    USER_EDITS_TAB,
    ColumnKind,
    ColumnSpec,
)


class TestColumnSpecValidation:
    def test_dropdown_requires_values(self):
        with pytest.raises(ValueError, match="DROPDOWN"):
            ColumnSpec(key="x", header="X", kind=ColumnKind.DROPDOWN)

    def test_formula_requires_template(self):
        with pytest.raises(ValueError, match="FORMULA"):
            ColumnSpec(key="x", header="X", kind=ColumnKind.FORMULA)

    def test_dropdown_values_only_for_dropdown(self):
        with pytest.raises(ValueError, match="dropdown_values"):
            ColumnSpec(
                key="x",
                header="X",
                kind=ColumnKind.TEXT,
                dropdown_values=("a", "b"),
            )

    def test_text_column_ok(self):
        c = ColumnSpec(key="x", header="X", kind=ColumnKind.TEXT)
        assert c.editable is False
        assert c.width_px is None


class TestTabSpec:
    def test_header_row(self):
        assert "Subject" in TASKS_TAB.header_row
        assert "Title" in TASKS_TAB.header_row
        assert "Due" in TASKS_TAB.header_row

    def test_column_index(self):
        assert TASKS_TAB.column_index("subject") == 0
        assert TASKS_TAB.column_index("task_type") == 1
        assert TASKS_TAB.column_index("title") == 2

    def test_column_index_missing(self):
        with pytest.raises(KeyError):
            TASKS_TAB.column_index("does_not_exist")

    def test_editable_columns(self):
        keys = {c.key for c in TASKS_TAB.editable_columns()}
        assert keys == {"due", "status", "notes"}


class TestTasksTab:
    def test_type_dropdown(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("task_type")]
        assert col.kind is ColumnKind.DROPDOWN
        assert {"Assessment", "Homework", "General"}.issubset(set(col.dropdown_values))

    def test_subject_is_text_not_dropdown(self):
        # Subject column intentionally has NO dropdown — there are too many
        # subjects across both kids and the resolver handles canonicalisation.
        col = TASKS_TAB.columns[TASKS_TAB.column_index("subject")]
        assert col.kind is ColumnKind.TEXT
        assert col.dropdown_values == ()

    def test_days_is_relative_formula(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("days")]
        assert col.kind is ColumnKind.FORMULA
        assert "TODAY()" in col.formula_template
        assert "{row}" in col.formula_template
        # Blank when no due date or already submitted/graded.
        assert "Submitted" in col.formula_template
        assert "Graded" in col.formula_template

    def test_days_formula_blanks_archived_rows(self):
        """The Days formula must blank archived rows so they don't render a
        negative number when they appear on the History tab."""
        col = TASKS_TAB.columns[TASKS_TAB.column_index("days")]
        assert "Archived" in col.formula_template

    def test_status_dropdown_includes_archived(self):
        """The Status dropdown must include 'Archived' so the cell renders
        correctly when the silver writer / age-cap sweep sets it."""
        assert "Archived" in STATUS_VALUES

    def test_due_is_date(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("due")]
        assert col.kind is ColumnKind.DATE

    def test_source_dropdown_values(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("source")]
        assert col.kind is ColumnKind.DROPDOWN
        assert col.dropdown_values == SOURCE_VALUES

    def test_status_dropdown_editable(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("status")]
        assert col.kind is ColumnKind.DROPDOWN
        assert col.editable is True
        assert col.dropdown_values == STATUS_VALUES

    def test_task_uid_present_for_useredits_merge(self):
        # Hidden identity column required to join Tasks ↔ UserEdits.
        idx = TASKS_TAB.column_index("task_uid")
        assert TASKS_TAB.columns[idx].header == "task_uid"

    def test_table_id(self):
        assert TASKS_TAB.table_id == "tbl_tasks"


class TestDashboardTab:
    def test_no_table_id(self):
        # Dashboard is pure formula-driven, NOT a Sheets Table.
        assert DASHBOARD_TAB.table_id == ""

    def test_six_columns(self):
        # v4.3 bordered-canvas Dashboard layout uses 6 columns A..F.
        # Columns A and F are 8px border columns framing content in B..E.
        assert len(DASHBOARD_TAB.columns) == 6

    def test_no_headers(self):
        # The layout owns row 1 — no schema-level header strings.
        assert all(c.header == "" for c in DASHBOARD_TAB.columns)

    def test_no_frozen_rows(self):
        # Dashboard has no header row to freeze.
        assert DASHBOARD_TAB.frozen_rows == 0

    def test_column_widths(self):
        widths = [c.width_px for c in DASHBOARD_TAB.columns]
        # 8px borders on A/F; B(Subject)=256, C(Title)=512, D(Due)=128,
        # E(Status)=128. Inner canvas = 1024px; total = 1040px.
        assert widths == [8, 256, 512, 128, 128, 8]


class TestSettingsTab:
    def test_per_source_layout(self):
        assert tuple(c.key for c in SETTINGS_TAB.columns) == (
            "source",
            "last_synced",
            "token_expires",
            "status",
        )
        assert SETTINGS_TAB.table_id == ""


class TestUserEditsTab:
    def test_hidden(self):
        assert USER_EDITS_TAB.hidden is True

    def test_columns(self):
        assert tuple(c.key for c in USER_EDITS_TAB.columns) == (
            "task_uid",
            "column",
            "original_value",
            "value",
            "updated_at",
        )

    def test_table_id(self):
        assert USER_EDITS_TAB.table_id == "tbl_user_edits"


class TestSheetSchema:
    def test_default_tab_order(self):
        names = tuple(t.name for t in SCHEMA.tabs)
        assert names == (
            "Dashboard",
            "Tasks",
            "History",
            "Settings",
            "UserEdits",
            "DashboardData",
        )

    def test_by_name(self):
        assert SCHEMA.by_name("Tasks") is TASKS_TAB

    def test_by_name_missing(self):
        with pytest.raises(KeyError):
            SCHEMA.by_name("Nope")


class TestDashboardTableIds:
    """v5.0: Dashboard lists are real Sheets Tables; each section's
    Table is identified by a stable opaque id + human-readable name."""

    def test_table_ids_and_names_are_distinct_and_stable(self):
        from homework_hub.schema import (
            DASHBOARD_DONE_TABLE_ID,
            DASHBOARD_DONE_TABLE_NAME,
            DASHBOARD_NO_DUE_DATE_TABLE_ID,
            DASHBOARD_NO_DUE_DATE_TABLE_NAME,
            DASHBOARD_OVERDUE_TABLE_ID,
            DASHBOARD_OVERDUE_TABLE_NAME,
            DASHBOARD_TABLE_IDS,
            DASHBOARD_UPCOMING_TABLE_ID,
            DASHBOARD_UPCOMING_TABLE_NAME,
            DASHBOARD_WEEK_TABLE_ID,
            DASHBOARD_WEEK_TABLE_NAME,
        )

        # IDs are non-empty strings (Sheets accepts caller-supplied ids).
        ids = (
            DASHBOARD_OVERDUE_TABLE_ID,
            DASHBOARD_WEEK_TABLE_ID,
            DASHBOARD_NO_DUE_DATE_TABLE_ID,
            DASHBOARD_UPCOMING_TABLE_ID,
            DASHBOARD_DONE_TABLE_ID,
        )
        for tid in ids:
            assert isinstance(tid, str)
            assert tid
        # Distinct.
        assert len(set(ids)) == 5
        # Aggregated tuple matches.
        assert ids == DASHBOARD_TABLE_IDS
        # Names are the user-visible section labels.
        assert DASHBOARD_OVERDUE_TABLE_NAME == "Overdue"
        assert DASHBOARD_WEEK_TABLE_NAME == "DueThisWeek"
        assert DASHBOARD_NO_DUE_DATE_TABLE_NAME == "NoDueDate"
        assert DASHBOARD_UPCOMING_TABLE_NAME == "Upcoming"
        assert DASHBOARD_DONE_TABLE_NAME == "DoneThisWeek"

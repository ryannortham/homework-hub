"""Tests for the Gold-publish ColumnSpec schema (M5b)."""

from __future__ import annotations

import pytest

from homework_hub.schema import (
    DUPLICATES_TAB,
    PRIORITY_VALUES,
    SCHEMA,
    SETTINGS_TAB,
    SOURCE_VALUES,
    STATUS_VALUES,
    TASKS_TAB,
    TODAY_TAB,
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
        assert TASKS_TAB.column_index("title") == 1

    def test_column_index_missing(self):
        with pytest.raises(KeyError):
            TASKS_TAB.column_index("does_not_exist")

    def test_editable_columns(self):
        keys = {c.key for c in TASKS_TAB.editable_columns()}
        assert keys == {"priority", "done", "notes"}


class TestTasksTab:
    def test_priority_dropdown_includes_blank(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("priority")]
        assert col.kind is ColumnKind.DROPDOWN
        assert "" in col.dropdown_values
        assert {"Low", "Med", "High"}.issubset(set(col.dropdown_values))

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
        assert "[@Due]" in col.formula_template

    def test_done_is_checkbox_and_editable(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("done")]
        assert col.kind is ColumnKind.CHECKBOX
        assert col.editable is True

    def test_due_is_date(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("due")]
        assert col.kind is ColumnKind.DATE

    def test_source_dropdown_values(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("source")]
        assert col.kind is ColumnKind.DROPDOWN
        assert col.dropdown_values == SOURCE_VALUES

    def test_status_dropdown_read_only(self):
        col = TASKS_TAB.columns[TASKS_TAB.column_index("status")]
        assert col.kind is ColumnKind.DROPDOWN
        assert col.editable is False
        assert col.dropdown_values == STATUS_VALUES

    def test_task_uid_present_for_useredits_merge(self):
        # Hidden identity column required to join Tasks ↔ UserEdits.
        idx = TASKS_TAB.column_index("task_uid")
        assert TASKS_TAB.columns[idx].header == "task_uid"

    def test_table_id(self):
        assert TASKS_TAB.table_id == "tbl_tasks"


class TestTodayTab:
    def test_query_formula(self):
        col = TODAY_TAB.columns[0]
        assert col.kind is ColumnKind.FORMULA
        assert "QUERY(tbl_tasks" in col.formula_template
        assert "Days <= 0" in col.formula_template
        assert "Done = false" in col.formula_template

    def test_no_table_id(self):
        # Today is a pure formula view, NOT a Sheets Table.
        assert TODAY_TAB.table_id == ""


class TestDuplicatesTab:
    def test_confirm_dismiss_checkboxes(self):
        confirm = DUPLICATES_TAB.columns[DUPLICATES_TAB.column_index("confirm")]
        dismiss = DUPLICATES_TAB.columns[DUPLICATES_TAB.column_index("dismiss")]
        assert confirm.kind is ColumnKind.CHECKBOX
        assert confirm.editable is True
        assert dismiss.kind is ColumnKind.CHECKBOX
        assert dismiss.editable is True

    def test_compass_and_classroom_titles_present(self):
        assert "compass_title" in {c.key for c in DUPLICATES_TAB.columns}
        assert "classroom_title" in {c.key for c in DUPLICATES_TAB.columns}

    def test_link_id_hidden_first(self):
        # link_id is required for state writeback; placed first as a stable key.
        assert DUPLICATES_TAB.columns[0].key == "link_id"


class TestSettingsTab:
    def test_key_value_layout(self):
        assert tuple(c.key for c in SETTINGS_TAB.columns) == ("key", "value")
        assert SETTINGS_TAB.table_id == ""


class TestUserEditsTab:
    def test_hidden(self):
        assert USER_EDITS_TAB.hidden is True

    def test_columns(self):
        assert tuple(c.key for c in USER_EDITS_TAB.columns) == (
            "task_uid",
            "column",
            "value",
            "updated_at",
        )

    def test_table_id(self):
        assert USER_EDITS_TAB.table_id == "tbl_user_edits"


class TestSheetSchema:
    def test_default_tab_order(self):
        names = tuple(t.name for t in SCHEMA.tabs)
        assert names == ("Today", "Tasks", "Possible Duplicates", "Settings", "UserEdits")

    def test_by_name(self):
        assert SCHEMA.by_name("Tasks") is TASKS_TAB

    def test_by_name_missing(self):
        with pytest.raises(KeyError):
            SCHEMA.by_name("Nope")


class TestPriorityVocabulary:
    def test_priority_values(self):
        assert PRIORITY_VALUES == ("", "Low", "Med", "High")

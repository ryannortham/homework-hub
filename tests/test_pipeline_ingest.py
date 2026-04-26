"""Tests for the bronze ingest layer (M2)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from homework_hub.pipeline.ingest import BronzeWriter, IngestResult, RawRecord
from homework_hub.state.store import StateStore


@pytest.fixture
def store(tmp_path: Path) -> StateStore:
    return StateStore(tmp_path / "state.db")


@pytest.fixture
def writer(store: StateStore) -> BronzeWriter:
    return BronzeWriter(store)


def _record(
    child: str = "james",
    source: str = "compass",
    source_id: str = "1",
    payload: dict | None = None,
    fetched_at: datetime | None = None,
) -> RawRecord:
    return RawRecord(
        child=child,
        source=source,
        source_id=source_id,
        payload=payload or {"name": "WW1 Benchmark", "due": "2026-05-01"},
        fetched_at=fetched_at or datetime(2026, 4, 26, 10, 0, tzinfo=UTC),
    )


# --------------------------------------------------------------------------- #
# RawRecord — pure-data behaviour
# --------------------------------------------------------------------------- #


class TestRawRecordHash:
    def test_canonical_json_is_sorted(self):
        rec = RawRecord(
            child="james",
            source="compass",
            source_id="1",
            payload={"b": 2, "a": 1},
        )
        assert rec.canonical_json() == '{"a":1,"b":2}'

    def test_payload_hash_stable_across_dict_orderings(self):
        a = RawRecord(child="x", source="y", source_id="z", payload={"a": 1, "b": 2})
        b = RawRecord(child="x", source="y", source_id="z", payload={"b": 2, "a": 1})
        assert a.payload_hash() == b.payload_hash()

    def test_payload_hash_changes_when_payload_changes(self):
        a = _record(payload={"name": "v1"})
        b = _record(payload={"name": "v2"})
        assert a.payload_hash() != b.payload_hash()

    def test_payload_hash_is_64_hex(self):
        h = _record().payload_hash()
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


# --------------------------------------------------------------------------- #
# BronzeWriter.write_many — insert, dedup, ids
# --------------------------------------------------------------------------- #


class TestWriteMany:
    def test_first_write_inserts_all(self, writer: BronzeWriter):
        result = writer.write_many([_record(source_id="1"), _record(source_id="2")])
        assert isinstance(result, IngestResult)
        assert result.inserted == 2
        assert result.skipped == 0
        assert len(result.ids) == 2
        assert all(i > 0 for i in result.ids)

    def test_identical_payload_skipped_on_second_write(self, writer: BronzeWriter):
        first = writer.write_many([_record(source_id="1")])
        second = writer.write_many([_record(source_id="1")])
        assert first.inserted == 1
        assert second.inserted == 0
        assert second.skipped == 1
        # Skipped records still report the original bronze id.
        assert second.ids == first.ids

    def test_changed_payload_creates_new_row(self, writer: BronzeWriter):
        writer.write_many([_record(source_id="1", payload={"v": 1})])
        result = writer.write_many([_record(source_id="1", payload={"v": 2})])
        assert result.inserted == 1
        assert result.skipped == 0

    def test_ids_in_input_order(self, writer: BronzeWriter):
        result = writer.write_many(
            [
                _record(source_id="a"),
                _record(source_id="b"),
                _record(source_id="c"),
            ]
        )
        assert len(result.ids) == 3
        assert result.ids == sorted(result.ids)  # autoincrement → ascending

    def test_mixed_batch_inserts_and_skips(self, writer: BronzeWriter):
        writer.write_many([_record(source_id="a", payload={"v": 1})])
        result = writer.write_many(
            [
                _record(source_id="a", payload={"v": 1}),  # exact dup -> skip
                _record(source_id="a", payload={"v": 2}),  # changed -> insert
                _record(source_id="b", payload={"v": 1}),  # new id  -> insert
            ]
        )
        assert result.inserted == 2
        assert result.skipped == 1

    def test_separate_children_dont_collide(self, writer: BronzeWriter):
        writer.write_many([_record(child="james", source_id="1")])
        result = writer.write_many([_record(child="tahlia", source_id="1")])
        assert result.inserted == 1
        assert result.skipped == 0

    def test_separate_sources_dont_collide(self, writer: BronzeWriter):
        writer.write_many([_record(source="compass", source_id="1")])
        result = writer.write_many([_record(source="classroom", source_id="1")])
        assert result.inserted == 1

    def test_fetched_at_persisted_as_utc_iso(self, writer: BronzeWriter, store: StateStore):
        ts = datetime(2026, 4, 26, 14, 30, tzinfo=UTC)
        writer.write_many([_record(source_id="1", fetched_at=ts)])
        import sqlite3

        conn = sqlite3.connect(store.db_path)
        row = conn.execute("SELECT fetched_at FROM bronze_records").fetchone()
        conn.close()
        assert row[0].startswith("2026-04-26T14:30:00")
        assert "+00:00" in row[0] or row[0].endswith("Z")

    def test_payload_round_trips_through_db(self, writer: BronzeWriter, store: StateStore):
        payload = {
            "id": 1,
            "name": "WW1 Benchmark",
            "students": [{"submissionStatus": 4}],
        }
        writer.write_many([_record(source_id="1", payload=payload)])
        import sqlite3

        conn = sqlite3.connect(store.db_path)
        row = conn.execute("SELECT payload_json FROM bronze_records").fetchone()
        conn.close()
        assert json.loads(row[0]) == payload


# --------------------------------------------------------------------------- #
# BronzeWriter.latest_for — most recent payload per source_id
# --------------------------------------------------------------------------- #


class TestLatestFor:
    def test_empty_when_no_records(self, writer: BronzeWriter):
        assert writer.latest_for("james", "compass") == []

    def test_single_record_returned(self, writer: BronzeWriter):
        writer.write_many([_record(source_id="1", payload={"v": 1})])
        rows = writer.latest_for("james", "compass")
        assert len(rows) == 1
        bronze_id, source_id, payload, fetched_at = rows[0]
        assert source_id == "1"
        assert payload == {"v": 1}
        assert isinstance(bronze_id, int)
        assert isinstance(fetched_at, datetime)

    def test_returns_only_most_recent_per_source_id(self, writer: BronzeWriter):
        t0 = datetime(2026, 4, 26, 10, 0, tzinfo=UTC)
        t1 = t0 + timedelta(hours=1)
        writer.write_many([_record(source_id="1", payload={"v": 1}, fetched_at=t0)])
        writer.write_many([_record(source_id="1", payload={"v": 2}, fetched_at=t1)])
        rows = writer.latest_for("james", "compass")
        assert len(rows) == 1
        assert rows[0][2] == {"v": 2}

    def test_filters_by_child_and_source(self, writer: BronzeWriter):
        writer.write_many(
            [
                _record(child="james", source="compass", source_id="1"),
                _record(child="james", source="classroom", source_id="2"),
                _record(child="tahlia", source="compass", source_id="3"),
            ]
        )
        rows = writer.latest_for("james", "compass")
        assert len(rows) == 1
        assert rows[0][1] == "1"

    def test_multiple_source_ids_returned_in_id_order(self, writer: BronzeWriter):
        writer.write_many(
            [
                _record(source_id="a"),
                _record(source_id="b"),
                _record(source_id="c"),
            ]
        )
        rows = writer.latest_for("james", "compass")
        ids = [r[1] for r in rows]
        assert ids == ["a", "b", "c"]

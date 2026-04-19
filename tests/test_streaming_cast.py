"""Regression tests for the streaming .dat -> Parquet pipeline.

The bug we're guarding against: ``pyarrow.RecordBatch.cast`` only exists in
PyArrow >= 14. Earlier code called ``batch.cast(table_def.schema)`` directly,
which silently broke streaming on PyArrow 11 (the version shipping with
several conda/Spark images). The fix replaces it with ``_cast_to_schema``,
which casts column-by-column and works back to PyArrow 4.

These tests don't need the ``dsdgen`` binary — they synthesise a tiny
pipe-delimited file matching the dbgen_version schema (string + date + time +
string), which exercises both the time-column cast path and the generic
schema-cast path in one shot.
"""
from __future__ import annotations

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tpcds_fast_datagen.schema import DBGEN_VERSION, REASON
from tpcds_fast_datagen.worker import _cast_to_schema, _streaming_dat_to_parquet


def _write_dat(path: pathlib.Path, rows: list[str]) -> None:
    """Write a dsdgen-style pipe-delimited file (trailing pipe per row)."""
    path.write_text("\n".join(r + "|" for r in rows) + "\n")


def test_cast_to_schema_handles_int_string_time(tmp_path):
    """The helper must coerce a batch with mixed types without RecordBatch.cast."""
    schema = pa.schema([
        ("a", pa.int32()),
        ("b", pa.string()),
        ("c", pa.time32("s")),
    ])
    batch = pa.record_batch(
        [pa.array([1, 2]), pa.array(["x", "y"]), pa.array([0, 3600], type=pa.time32("s"))],
        names=["a", "b", "c"],
    )
    out = _cast_to_schema(batch, schema)
    assert out.schema == schema
    assert out.num_rows == 2
    # time32('s') values: hour 0 (midnight) and hour 1 (3600s)
    times = out.column("c").to_pylist()
    assert times[0].hour == 0 and times[0].minute == 0
    assert times[1].hour == 1 and times[1].minute == 0


def test_streaming_dat_to_parquet_dbgen_version(tmp_path):
    """Full streaming path on dbgen_version (has a time32 column).

    Reproduces the SF=1..200 bench failure: this would raise
    ``AttributeError: 'pyarrow.lib.RecordBatch' object has no attribute 'cast'``
    on PyArrow < 14 if the regression returned.
    """
    dat = tmp_path / "dbgen_version.dat"
    _write_dat(dat, ["TPC-DS 3.2.0|2024-01-15|12:34:56|--scale 1"])

    out = tmp_path / "dbgen_version.parquet"
    n = _streaming_dat_to_parquet(str(dat), DBGEN_VERSION, str(out))
    assert n == 1

    table = pq.read_table(str(out))
    # Column names + value content (Parquet may widen time32('s') -> time32('ms'),
    # so we don't compare the full schema; we check semantics).
    assert table.schema.names == [c[0] for c in DBGEN_VERSION.columns]
    row = table.to_pylist()[0]
    assert row["dv_version"] == "TPC-DS 3.2.0"
    assert str(row["dv_create_date"]) == "2024-01-15"
    assert row["dv_create_time"].hour == 12
    assert row["dv_create_time"].minute == 34
    assert row["dv_create_time"].second == 56


def test_streaming_dat_to_parquet_no_time_column(tmp_path):
    """Exercises the ``_cast_to_schema`` branch (no time columns).

    REASON has only int + string columns, so it goes through the else-branch
    that previously called ``batch.cast(schema)`` and broke on PyArrow < 14.
    """
    dat = tmp_path / "reason.dat"
    _write_dat(dat, ["1|AAAA|because", "2|BBBB|why not"])
    # _write_dat appends the trailing pipe each row already.

    out = tmp_path / "reason.parquet"
    n = _streaming_dat_to_parquet(str(dat), REASON, str(out))
    assert n == 2

    table = pq.read_table(str(out))
    assert table.schema.names == [c[0] for c in REASON.columns]
    assert table.column("r_reason_sk").to_pylist() == [1, 2]

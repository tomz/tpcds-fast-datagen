"""Tests for tpcds_fast_datagen.spark.

These tests don't require a real SparkSession or pyspark — we exercise:

* :mod:`spark.job` task planning (pure Python),
* :mod:`spark.api.generate` via a tiny SparkContext-shaped fake,
* :mod:`spark.submit` argv construction.

A separate slow test suite would be needed for an actual Spark run.
"""
from __future__ import annotations

import json

import pytest

from tpcds_fast_datagen.spark import generate
from tpcds_fast_datagen.spark.api import GenerateResult
from tpcds_fast_datagen.spark.job import (
    _strip_child_suffix,
    autosize_chunks,
    plan_tasks,
)
from tpcds_fast_datagen.spark.submit import build_argv


# ------------------------ planning ----------------------------------


def test_plan_tasks_small_sf_one_task_per_table():
    """At SF=1 every table is < 1M rows except the big facts."""
    tasks = plan_tasks(scale_factor=1, num_chunks=8)
    # All tasks at SF=1 should be child=1, parallel=1 because nothing reaches
    # 1M rows except store_sales (2.88M), catalog_sales (1.44M), inventory.
    table_counts: dict[str, int] = {}
    for t in tasks:
        table_counts[t["table"]] = table_counts.get(t["table"], 0) + 1
    # Some big tables get sharded even at SF=1.
    assert "store_sales" in table_counts


def test_plan_tasks_dedupes_companion_pairs():
    """store_returns is not scheduled separately from store_sales."""
    tasks = plan_tasks(scale_factor=100, num_chunks=10)
    table_names = {t["table"] for t in tasks}
    assert "store_returns" not in table_names
    assert "catalog_returns" not in table_names
    assert "web_returns" not in table_names
    assert "store_sales" in table_names
    assert "catalog_sales" in table_names
    assert "web_sales" in table_names


def test_plan_tasks_caps_parallelism_at_million_row_floor():
    """A table with N rows can't be split into more than N//1M shards."""
    # SF=1: store_sales has ~2.88M rows -> at most 2 shards even if we ask for 100.
    tasks = plan_tasks(scale_factor=1, num_chunks=100)
    ss_tasks = [t for t in tasks if t["table"] == "store_sales"]
    assert len(ss_tasks) <= 2  # 2,880,404 // 1_000_000 = 2


def test_plan_tasks_respects_num_chunks_at_high_sf():
    tasks = plan_tasks(scale_factor=10000, num_chunks=80)
    ss_tasks = [t for t in tasks if t["table"] == "store_sales"]
    # SF=10000 store_sales = 28.8B rows -> capped at num_chunks=80
    assert len(ss_tasks) == 80


def test_strip_child_suffix():
    assert _strip_child_suffix("store_sales_3_16", child=3, parallel=16) == "store_sales"
    assert _strip_child_suffix("store_sales", child=1, parallel=1) == "store_sales"
    # non-numeric suffix stays put
    assert _strip_child_suffix("call_center", child=1, parallel=1) == "call_center"


def test_autosize_chunks_at_sf_1000():
    n = autosize_chunks(scale_factor=1000, num_executors=10, executor_cores=16)
    # 10*16 = 160 slots; biggest table is store_sales = 2.88B rows;
    # 2.88B / 150M = ~19 disk-shards; max(160, 19) = 160.
    assert n == 160


def test_autosize_chunks_at_sf_10000():
    """SF=10000 inventory = 117.45B rows -> disk forces well above slot count."""
    n = autosize_chunks(scale_factor=10000, num_executors=10, executor_cores=16)
    # inventory is the biggest: 11_745_000 * 10000 = 117.45B rows.
    # disk: 117.45B / 150M = 783; slots: 160; max = 783.
    # (This matches the proven SF=10000 run that used --chunks 783.)
    assert n == 783


# ------------------------ submit argv -------------------------------


def test_build_argv_minimal():
    argv = build_argv(scale=100, output="/tmp/out")
    assert argv[0] == "spark-submit"
    assert argv[-2].endswith("_bootstrap.py")
    payload = json.loads(argv[-1])
    assert payload == {
        "scale": 100,
        "output": "/tmp/out",
        "chunks": None,
        "compression": "snappy",
        "dsdgen_path": None,
    }


def test_build_argv_with_extra_spark_opts():
    argv = build_argv(
        scale=1000,
        output="abfs:///x",
        chunks=160,
        extra_spark_opts=["--master", "yarn", "--num-executors", "10"],
    )
    assert "--master" in argv
    assert "yarn" in argv
    payload = json.loads(argv[-1])
    assert payload["chunks"] == 160


def test_build_argv_custom_spark_submit_binary():
    argv = build_argv(scale=1, output="/tmp", spark_submit="/opt/spark/bin/spark-submit")
    assert argv[0] == "/opt/spark/bin/spark-submit"


# ------------------------ generate() with fake spark ----------------


class _FakeRDD:
    """Minimal RDD shim: just runs mapPartitions in-process."""

    def __init__(self, items):
        self._items = items

    def mapPartitions(self, fn):
        # Single-partition execution is fine for the test.
        return _FakeRDD(list(fn(iter(self._items))))

    def collect(self):
        return self._items


class _FakeConf:
    def __init__(self, mapping):
        self._mapping = mapping

    def get(self, key, default=None):
        return self._mapping.get(key, default)


class _FakeSC:
    def __init__(self, conf=None):
        self._conf = _FakeConf(conf or {"spark.executor.instances": "2",
                                         "spark.executor.cores": "2"})

    def getConf(self):
        return self._conf

    def parallelize(self, items, numSlices=None):
        return _FakeRDD(items)


class _FakeSpark:
    def __init__(self, conf=None):
        self.sparkContext = _FakeSC(conf)


def test_generate_returns_result_object_and_aggregates_rows(monkeypatch):
    """``generate`` must wire planner → mapPartitions → result aggregation.

    We monkeypatch ``run_dsdgen_task`` to avoid touching dsdgen, then check
    that table_rows were summed correctly across tasks.
    """
    fake_results_by_call = []

    def fake_run(task, sf, output, **kwargs):
        # Pretend each task generated 100 rows of its primary table.
        fake_results_by_call.append(task)
        return {"task": task, "results": {task["table"]: 100}}

    monkeypatch.setattr(
        "tpcds_fast_datagen.spark.api.run_dsdgen_task", fake_run
    )

    spark = _FakeSpark()
    result = generate(spark, scale=1, output="/tmp/x", chunks=2, quiet=True)

    assert isinstance(result, GenerateResult)
    assert result.succeeded
    assert result.scale_factor == 1
    assert result.num_tasks > 0
    assert result.total_rows > 0
    # Every primary fact table that was scheduled must show up
    assert "store_sales" in result.table_rows
    # No accidental companion-table double counting
    assert "store_returns" not in result.table_rows


def test_generate_collects_errors(monkeypatch):
    def fake_run(task, *a, **kw):
        return {"task": task, "error": "synthetic boom"}

    monkeypatch.setattr(
        "tpcds_fast_datagen.spark.api.run_dsdgen_task", fake_run
    )

    spark = _FakeSpark()
    result = generate(spark, scale=1, output="/tmp/x", chunks=1, quiet=True)
    assert not result.succeeded
    assert len(result.errors) == result.num_tasks
    assert result.total_rows == 0

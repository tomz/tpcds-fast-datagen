"""High-level Spark generation API.

The single function exported from :mod:`tpcds_fast_datagen.spark` —
:func:`generate` — takes a ``SparkSession`` and a few config knobs and runs
the distributed TPC-DS datagen against it. Designed to be called from
notebooks (Fabric, Databricks), Livy session statements, or wrapped by
:mod:`tpcds_fast_datagen.spark.submit` for ``spark-submit`` users.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .job import autosize_chunks, plan_tasks, run_dsdgen_task


@dataclass
class GenerateResult:
    """Return value from :func:`generate`."""

    scale_factor: int
    output: str
    elapsed_s: float
    num_tasks: int
    num_chunks: int
    table_rows: dict[str, int] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(self.table_rows.values())

    @property
    def succeeded(self) -> bool:
        return not self.errors


def generate(
    spark: Any,
    scale: int,
    output: str,
    *,
    chunks: int | None = None,
    compression: str = "snappy",
    dsdgen_path: str | None = None,
    tmp_root: str | None = None,
    quiet: bool = False,
) -> GenerateResult:
    """Generate a TPC-DS dataset using ``spark`` and write to ``output``.

    Parameters
    ----------
    spark
        An active ``pyspark.sql.SparkSession``. In notebooks this is the
        preset ``spark`` variable; in Livy session statements you obtain it
        via ``SparkSession.builder.getOrCreate()``.
    scale
        TPC-DS scale factor in GB (1, 100, 1000, 10000, ...).
    output
        Base output path. Local filesystem, ``hdfs://``, ``abfs://``,
        ``s3://``, etc. — any URI ``hdfs dfs -put`` accepts on the executor
        nodes.
    chunks
        Parallel shard count for large tables. ``None`` (default) auto-sizes
        from the cluster's executor count and cores.
    compression
        Parquet compression codec: ``snappy`` (default), ``gzip``, ``zstd``,
        ``none``.
    dsdgen_path
        Explicit path to the ``dsdgen`` binary on each executor. ``None``
        falls back to (in order): ``SparkFiles.get('dsdgen')``,
        ``$DSDGEN_PATH``, and the single-node ``binary._SEARCH_PATHS``.
    tmp_root
        Per-executor scratch directory. ``None`` prefers ``/mnt/tmp`` on
        HDInsight-style nodes, else the system default.
    quiet
        Suppress driver-side progress prints.

    Returns
    -------
    GenerateResult
        ``elapsed_s``, per-table row counts, and any executor errors.
    """
    sc = spark.sparkContext

    if chunks is None:
        num_executors = int(sc.getConf().get("spark.executor.instances", "10"))
        executor_cores = int(sc.getConf().get("spark.executor.cores", "8"))
        num_chunks = autosize_chunks(scale, num_executors, executor_cores)
    else:
        num_chunks = chunks

    tasks = plan_tasks(scale, num_chunks)

    if not quiet:
        print(f"=== TPC-DS SF={scale} Distributed Generation ===")
        print(f"Chunks per large table: {num_chunks}")
        print(f"Output: {output}")
        print(f"Planned {len(tasks)} tasks")

    start = time.time()
    rdd = sc.parallelize(tasks, numSlices=len(tasks))

    def _process_partition(task_iter):
        for task in task_iter:
            yield run_dsdgen_task(
                task, scale, output, compression=compression,
                dsdgen_path=dsdgen_path, tmp_root=tmp_root,
            )

    raw_results = rdd.mapPartitions(_process_partition).collect()
    elapsed = time.time() - start

    table_rows: dict[str, int] = {}
    errors: list[dict] = []
    for r in raw_results:
        if "error" in r:
            errors.append(r)
        else:
            for tname, rows in r.get("results", {}).items():
                table_rows[tname] = table_rows.get(tname, 0) + rows

    if not quiet:
        print(f"\n=== Generation Complete in {elapsed:.1f}s ({elapsed/60:.1f}m) ===")
        for tname in sorted(table_rows):
            print(f"  {tname}: {table_rows[tname]:,} rows")
        if errors:
            print(f"\n{len(errors)} errors:")
            for e in errors[:10]:
                print(f"  {e}")

    return GenerateResult(
        scale_factor=scale,
        output=output,
        elapsed_s=elapsed,
        num_tasks=len(tasks),
        num_chunks=num_chunks,
        table_rows=table_rows,
        errors=errors,
    )

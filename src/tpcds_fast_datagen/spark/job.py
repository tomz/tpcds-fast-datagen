"""Spark task planner + executor worker for distributed TPC-DS datagen.

This module reuses the single-node package's ``schema`` and ``worker`` modules
so there is exactly one source of truth for table definitions and the
.dat → Parquet streaming pipeline. The Spark-specific concerns kept here:

* task planning (one task per dsdgen ``-CHILD`` shard),
* per-executor dsdgen invocation,
* remote (HDFS / ABFS / S3) staging via ``hdfs dfs -put``,
* distributing the ``dsdgen`` binary via ``SparkFiles`` when present.

All functions are pure / picklable so they work inside ``mapPartitions``.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable

from ..schema import ALL_TABLES, COMPANION_PAIRS, COMPANION_PRIMARY


def plan_tasks(scale_factor: int, num_chunks: int) -> list[dict]:
    """Plan dsdgen tasks for distribution across the cluster.

    Mirrors the single-node ``generator._plan_tasks`` behavior:

    * Tables with < 1M rows at this SF run as a single task (child=1).
    * Larger tables are sharded; effective parallelism is capped at
      ``row_count // 1_000_000`` (dsdgen's own ``split_work()`` floor).
    * Companion tables (e.g. store_sales/store_returns) are emitted by a
      single dsdgen invocation against the primary; the executor discovers
      and writes both ``.dat`` files.
    """
    tasks: list[dict] = []
    planned: set[str] = set()

    for table_name, table_def in ALL_TABLES.items():
        if table_name in planned:
            continue

        primary = COMPANION_PRIMARY.get(table_name, table_name)
        companion = COMPANION_PAIRS.get(primary)
        planned.add(primary)
        if companion:
            planned.add(companion)

        row_count = ALL_TABLES[primary].row_count(scale_factor)

        if row_count < 1_000_000:
            tasks.append({"table": primary, "child": 1, "parallel": 1})
        else:
            max_parallel = max(1, row_count // 1_000_000)
            effective = min(num_chunks, max_parallel)
            for child in range(1, effective + 1):
                tasks.append(
                    {"table": primary, "child": child, "parallel": effective}
                )

    return tasks


def _resolve_dsdgen_on_executor(explicit: str | None = None) -> str:
    """Find the dsdgen binary on an executor.

    Resolution order:
        1. Explicit path argument (highest priority).
        2. SparkFiles.get("dsdgen") — set by ``spark-submit --files dsdgen``
           or ``spark.sparkContext.addFile(...)`` in a notebook.
        3. ``DSDGEN_PATH`` env var.
        4. The single-node ``binary._SEARCH_PATHS`` list.
    """
    if explicit and os.path.exists(explicit):
        return explicit
    try:
        from pyspark import SparkFiles  # type: ignore
        path = SparkFiles.get("dsdgen")
        if os.path.exists(path):
            os.chmod(path, 0o755)
            # Make sure tpcds.idx is alongside (dsdgen reads it from cwd).
            try:
                idx_src = SparkFiles.get("tpcds.idx")
                idx_dst = os.path.join(os.path.dirname(path), "tpcds.idx")
                if not os.path.exists(idx_dst) and os.path.exists(idx_src):
                    shutil.copy2(idx_src, idx_dst)
            except Exception:
                pass
            return path
    except (ImportError, Exception):
        pass

    env_path = os.environ.get("DSDGEN_PATH")
    if env_path and os.path.exists(env_path):
        return env_path

    from ..binary import find_dsdgen
    return find_dsdgen()


def _is_remote(path: str) -> bool:
    # True for any URI scheme that must go through ``hdfs dfs -put``
    # (abfss://, wasbs://, s3a://, hdfs://, ...). ``file://`` URIs are just
    # local paths with a scheme decoration — strip it and treat as POSIX.
    # Databricks ``dbfs:/`` is special-cased in normalize_output() to use the
    # POSIX FUSE mount (/dbfs/...) instead of going through ``hdfs dfs -put``.
    if path.startswith("file://"):
        return False
    return "://" in path


def _strip_file_scheme(path: str) -> str:
    """``file:///tmp/x`` → ``/tmp/x``; other inputs are returned unchanged."""
    if path.startswith("file://"):
        return path[len("file://"):]
    return path


def normalize_output(path: str) -> str:
    """Translate platform-specific URIs to POSIX paths where a FUSE mount exists.

    Currently:
        ``dbfs:/foo/bar`` -> ``/dbfs/foo/bar`` on Databricks runtimes.
        ``file:///foo/bar`` -> ``/foo/bar`` for local filesystems.
    """
    if path.startswith("dbfs:/") and not path.startswith("dbfs://"):
        # dbfs:/FileStore/...  ->  /dbfs/FileStore/...
        return "/dbfs/" + path[len("dbfs:/"):]
    if path.startswith("file://"):
        return _strip_file_scheme(path)
    return path


def _hdfs_put(local_path: str, remote_path: str) -> None:
    remote_dir = remote_path.rsplit("/", 1)[0]
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", remote_dir],
        capture_output=True, text=True,
    )
    proc = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local_path, remote_path],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError("hdfs dfs -put failed: " + proc.stderr)


def _strip_child_suffix(stem: str, child: int, parallel: int) -> str:
    """``store_sales_3_16`` → ``store_sales`` for parallel runs."""
    if parallel <= 1:
        return stem
    suffix = f"_{child}_{parallel}"
    if stem.endswith(suffix):
        return stem[: -len(suffix)]
    parts = stem.rsplit("_", 2)
    if len(parts) >= 3 and parts[-1].isdigit() and parts[-2].isdigit():
        return "_".join(parts[:-2])
    return stem


def run_dsdgen_task(
    task: dict,
    scale_factor: int,
    output_base: str,
    compression: str = "snappy",
    dsdgen_path: str | None = None,
    tmp_root: str | None = None,
) -> dict:
    """Execute one dsdgen shard on the current executor.

    Returns ``{"task": task, "results": {table: rows, ...}}`` on success or
    ``{"task": task, "error": "..."}`` on failure.
    """
    # Lazy-import worker so this module stays importable on machines without
    # pyarrow (e.g. Livy gateways that only do task planning).
    from ..worker import _streaming_dat_to_parquet

    table_name = task["table"]
    child = task["child"]
    parallel = task["parallel"]

    binary = _resolve_dsdgen_on_executor(dsdgen_path)
    output_base = normalize_output(output_base)
    remote = _is_remote(output_base)

    if tmp_root is None:
        # Prefer /mnt/tmp on HDInsight (large data disk); fall back to /tmp.
        tmp_root = "/mnt/tmp" if os.path.isdir("/mnt") else None
    if tmp_root:
        try:
            os.makedirs(tmp_root, exist_ok=True)
        except OSError:
            tmp_root = None

    with tempfile.TemporaryDirectory(
        prefix=f"tpcds_{table_name}_c{child}_",
        dir=tmp_root,
    ) as tmp_dir:
        cmd = [
            binary,
            "-SCALE", str(scale_factor),
            "-TABLE", table_name,
            "-DIR", tmp_dir,
            "-FORCE", "Y",
            "-QUIET", "Y",
        ]
        if parallel > 1:
            cmd.extend(["-PARALLEL", str(parallel), "-CHILD", str(child)])

        proc = subprocess.run(
            cmd,
            capture_output=True, text=True,
            cwd=os.path.dirname(binary),
        )
        if proc.returncode != 0:
            return {"task": task, "error": "dsdgen failed: " + proc.stderr}

        results: dict[str, int] = {}
        for dat in Path(tmp_dir).glob("*.dat"):
            tname = _strip_child_suffix(dat.stem, child, parallel)
            if tname not in ALL_TABLES:
                continue

            table_def = ALL_TABLES[tname]
            pq_name = (
                f"part-{child:05d}.parquet" if parallel > 1 else "part-00000.parquet"
            )

            if remote:
                local_pq = os.path.join(tmp_dir, f"{tname}_{pq_name}")
                rows = _streaming_dat_to_parquet(
                    str(dat), table_def, local_pq, compression=compression
                )
                if rows > 0:
                    _hdfs_put(local_pq, f"{output_base}/{tname}/{pq_name}")
            else:
                out_file = f"{output_base}/{tname}/{pq_name}"
                os.makedirs(os.path.dirname(out_file), exist_ok=True)
                rows = _streaming_dat_to_parquet(
                    str(dat), table_def, out_file, compression=compression
                )
            results[tname] = rows

    return {"task": task, "results": results}


def autosize_chunks(
    scale_factor: int,
    num_executors: int,
    executor_cores: int,
    max_rows_per_chunk: int = 150_000_000,
) -> int:
    """Pick a good ``num_chunks`` when the user didn't specify one.

    The two constraints:

    * Each ``.dat`` shard must fit in per-task temp disk (~30 GB at 200 B/row,
      i.e. ~150 M rows by default).
    * Total parallelism should saturate the cluster
      (``num_executors × executor_cores`` slots).
    """
    biggest = max(td.row_count(scale_factor) for td in ALL_TABLES.values())
    chunks_for_disk = max(1, biggest // max_rows_per_chunk)
    chunks_for_parallelism = max(1, num_executors * executor_cores)
    return max(chunks_for_disk, chunks_for_parallelism)

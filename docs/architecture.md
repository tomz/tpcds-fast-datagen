# Architecture

Internal notes for contributors and AI agents working on this codebase. The user-facing story lives in [`README.md`](../README.md) and [`docs/notebooks-and-livy.md`](notebooks-and-livy.md); this file covers the *why* and the cross-file invariants that aren't obvious from any single module.

## Two single-node engines

Selected via `--engine` (default `auto`; resolved by `GenConfig.__post_init__` to `duckdb` for SF ≤ 50 and `dsdgen` for SF > 50):

### `duckdb` engine — `src/tpcds_fast_datagen/worker_duckdb.py`

A single in-process DuckDB connection runs `CALL dsdgen(sf=N)` once, then a sequential `COPY TO ... PARQUET` per table. DuckDB's `COPY` is internally parallel; the orchestrator does not multi-process. Fast at small SF but holds all 25 tables in memory before exporting → OOM somewhere between SF=100 and SF=200 on a 125 GB box (measured, see `benchmarks/bench_engines.py`).

### `dsdgen` engine — `src/tpcds_fast_datagen/worker.py` + `binary.py`

Orchestrator (`generator.py::_plan_tasks`) shards work across processes using the official C `dsdgen` binary's `-PARALLEL N -CHILD K` flags. Each worker:

1. Runs `dsdgen` into a tempdir → pipe-delimited `.dat` files
2. Streams each `.dat` through `pyarrow.csv.open_csv` in batches (constant memory, ~1.3 GB regardless of SF)
3. Writes Parquet row groups incrementally with `ParquetWriter.write_batch`

`dsdgen` is located via (in order): `--dsdgen-path` CLI flag → `DSDGEN_PATH` env → well-known paths in `binary.py::_SEARCH_PATHS` → `PATH`. `tpcds.idx` must sit alongside the binary, and `dsdgen` is invoked with `cwd=dirname(dsdgen_path)` because it reads `tpcds.idx` from cwd.

### Planning rules (dsdgen engine)

`_plan_tasks` in `generator.py` mirrors `dsdgen`'s internal `split_work()`:

- Tables with `<1M rows` at this SF are generated only by `child=1` (no parallelism).
- For larger tables, effective parallelism is capped at `row_count // 1_000_000`.
- **Companion tables** (e.g. `store_sales`/`store_returns`) must be generated together — `dsdgen` emits both in a single invocation. The planner deduplicates via `COMPANION_PRIMARY` / `COMPANION_PAIRS` in `schema.py`; only the primary is scheduled, and the worker discovers and writes both `.dat` files.

## Schema — `schema.py`

Single source of truth for all 25 TPC-DS tables: PyArrow types, column order matching `dsdgen` pipe-delimited output, row-count formulas `f(sf)`, and companion relationships. CSV parsing accounts for `dsdgen`'s trailing pipe (synthetic `__trailing__` column dropped via `include_columns`). Time columns are read as strings then cast to `time32('s')` because PyArrow's CSV reader does not natively parse `HH:MM:SS`.

The Spark variant (`src/tpcds_fast_datagen/spark/job.py`) imports from this same module — there is no duplicate schema definition.

## Spark variant — `src/tpcds_fast_datagen/spark/`

`spark_tpcds_gen.py` at the repo root is a **thin compatibility shim** (~50 LOC) that parses the legacy CLI flags, builds a `SparkSession`, and forwards to `tpcds_fast_datagen.spark.generate`. The real implementation lives in the package:

- **`spark/api.py`** — `generate(spark, scale, output, ...)` is the **public entry point** for notebooks (Fabric, Databricks), Livy session statements, and the bootstrap script. Returns `GenerateResult`.
- **`spark/job.py`** — task planner (`plan_tasks`, `autosize_chunks`) and the executor-side `run_dsdgen_task`. Reuses `schema.py` and `worker.py` from the parent package — **one source of truth**, no copy-paste between single-node and Spark.
- **`spark/_bootstrap.py`** — the file `spark-submit` ships when the user goes through `tpcds-gen-spark-submit` or `tpcds-gen --engine spark`. It just `import`s `generate` and calls it with JSON-encoded args.
- **`spark/submit.py`** — builds the `spark-submit` argv and `execvp`s; powers the `tpcds-gen-spark-submit` console script.

Both the driver and executors must have `tpcds_fast_datagen` importable (via `%pip install` in notebooks, `--py-files` for spark-submit, or `--archives conda_env.tar.gz`). The historical "driver must run Python 3.6 without pyarrow" constraint has been retired; notebooks and modern Spark all run Python 3.9+.

The `dsdgen` binary is resolved per-executor in this order: `dsdgen_path=` kwarg → `SparkFiles.get("dsdgen")` → `$DSDGEN_PATH` → `binary._SEARCH_PATHS`.

## Conventions

- **Output layout:** `output_dir/<table_name>/part-NNNNN.parquet` (one subdir per table, one file per child for the dsdgen engine, one file total for duckdb engine).
- **`parallel=0`** (CLI default) means `os.cpu_count()` — resolved in `GenConfig.__post_init__`.
- **`binaries/`** directory is intentionally empty; it's a placeholder for bundled `dsdgen` builds (not currently shipped).
- **PyArrow API floor: ≥ 11.** Avoid `RecordBatch.cast()` (added in 14) — use `worker._cast_to_schema()` which casts column-by-column with the older `Array.cast()` API. The `tests/test_streaming_cast.py` regression test guards this.

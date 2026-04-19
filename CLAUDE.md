# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`tpcds-fast-datagen` — parallel TPC-DS data generator that emits Parquet directly. Single-machine tool (no cluster), packaged as a Python wheel exposing the `tpcds-gen` CLI.

## Common commands

```bash
# Install in editable mode with dev deps
pip install -e ".[dev]"

# Run the CLI
tpcds-gen --scale 1 --parallel 8 --output /tmp/tpcds_sf1 --overwrite

# Run tests
pytest                                  # all
pytest tests/test_foo.py::test_bar -v   # single test
pytest --timeout=60                     # uses pytest-timeout

# Benchmark vs DuckDB baseline
python benchmarks/bench_vs_duckdb.py

# Distributed/Spark variant (self-contained, see file header for spark-submit invocation)
spark-submit ... spark_tpcds_gen.py --scale 1000 --output abfs:///tpcds/sf1000
```

## Architecture

Two independent generation **engines**, selected via `--engine` (default `duckdb`):

1. **`duckdb` engine** (`worker_duckdb.py`) — single in-process DuckDB connection runs `CALL dsdgen(sf=N)` once, then sequential `COPY TO ... PARQUET` per table. DuckDB's COPY is internally parallel; the orchestrator does not multi-process. Fast and the default path.

2. **`dsdgen` engine** (`worker.py` + `binary.py`) — orchestrator (`generator.py::_plan_tasks`) shards work across processes using the official C `dsdgen` binary's `-PARALLEL N -CHILD K` flags. Each worker:
   - Runs `dsdgen` into a tempdir → pipe-delimited `.dat` files
   - Streams each `.dat` through `pyarrow.csv.open_csv` in batches (constant memory)
   - Writes Parquet row groups incrementally with `ParquetWriter.write_batch`

   `dsdgen` must be located via (in order): `--dsdgen-path`, `DSDGEN_PATH` env, well-known paths in `binary.py::_SEARCH_PATHS`, then `PATH`. `tpcds.idx` must sit alongside the binary, and `dsdgen` is invoked with `cwd=dirname(dsdgen_path)` because it reads `tpcds.idx` from cwd.

### Planning rules (dsdgen engine)

`_plan_tasks` in `generator.py` mirrors `dsdgen`'s internal `split_work()`:
- Tables with `<1M rows` at this SF are generated only by `child=1` (no parallelism).
- For larger tables, effective parallelism is capped at `row_count // 1_000_000`.
- **Companion tables** (e.g. `store_sales`/`store_returns`) must be generated together — `dsdgen` emits both in a single invocation. The planner deduplicates via `COMPANION_PRIMARY` / `COMPANION_PAIRS` in `schema.py`; only the primary is scheduled, and the worker discovers and writes both `.dat` files.

### Schema

`schema.py` is the single source of truth for all 25 TPC-DS tables: PyArrow types, column order matching `dsdgen` pipe-delimited output, row-count formulas `f(sf)`, and companion relationships. CSV parsing accounts for `dsdgen`'s trailing pipe (synthetic `__trailing__` column dropped via `include_columns`). Time columns are read as strings then cast to `time32('s')` because PyArrow's CSV reader does not natively parse `HH:MM:SS`.

### Spark variant

`spark_tpcds_gen.py` at the repo root is **deliberately self-contained** — driver runs Python 3.6 without `pyarrow`; it must not import from `src/tpcds_fast_datagen/`. Executors get `pyarrow` via `--archives conda_env.tar.gz`. Edit it as a standalone script.

## Conventions

- Output layout is `output_dir/<table_name>/part-NNNNN.parquet` (one subdir per table, one file per child for the dsdgen engine, one file total for duckdb engine).
- `parallel=0` (CLI default) means `os.cpu_count()` — resolved in `GenConfig.__post_init__`.
- The `binaries/` directory is intentionally empty; it's a placeholder for bundled dsdgen builds (not currently shipped).

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] — 2026-04-19

### Added
- **Spark Python API** (`tpcds_fast_datagen.spark.generate(spark, ...)`) — usable from Microsoft Fabric notebooks, Databricks notebooks, Livy session statements, and any other environment where you have a `SparkSession`.
- **`tpcds-gen-spark-submit`** console script and **`tpcds-gen --engine spark`** dispatch — both build a `spark-submit` invocation around a tiny bundled bootstrap. Pass extra spark-submit options after `--`.
- `docs/notebooks-and-livy.md` — end-to-end examples for Fabric, Databricks, Livy `POST /sessions` and `POST /batches`, plus the `spark-submit` and legacy paths.
- `docs/live-test-status.md` — verified-paths matrix across HDInsight, Fabric, and Databricks (spark-submit, Livy, SJD, notebook, `spark_python_task`, `notebook_task`) at SF=1 and SF=100, plus the two platform-specific workarounds (HDI Livy `--archives venv`, Fabric SJD broadcast-bytes + high-fanout bootstrap).
- 12 new tests for `tpcds_fast_datagen.spark` (planner, autosizing, argv builder, fake-Spark `generate` smoke). Total: 36 passing.
- Auto engine selection: `--engine auto` (default) picks `duckdb` for SF ≤ 50 and `dsdgen` for SF > 50, with `--auto-threshold N` to override the cutoff.
- `benchmarks/bench_engines.py` — head-to-head harness measuring wall-clock + peak RSS for both engines, with OOM detection.
- `docs/spark-sizing-best-practices.md` (and HTML version) — cluster sizing tables for E8ads/E16ads × 3/5/10/15/20 nodes × SF=1000/10000/100000.
- `tests/test_streaming_cast.py` — regression tests for the streaming `.dat` → Parquet cast path.
- `LICENSE`, `CONTRIBUTING.md`, `CHANGELOG.md`.
- GitHub Actions CI: pytest + coverage on push and PR.

### Changed
- Tagline simplified from "the fastest TPC-DS data generator" to "the fast TPC-DS data generator" (README, `pyproject.toml`, package docstring, CLI help).
- **`spark_tpcds_gen.py` is no longer a 1300-line standalone script.** It is now a ~50-line back-compat shim that builds a `SparkSession` and delegates to `tpcds_fast_datagen.spark.generate`. Existing `spark-submit spark_tpcds_gen.py ...` invocations still work, but both driver and executors now need `tpcds_fast_datagen` importable (via `%pip` / `--py-files` / `--archives`).
- The Python-3.6-on-driver constraint has been retired. Modern Spark/HDInsight/Fabric/Databricks all run Python 3.9+; the package now imports freely on the driver.
- `worker.py` now uses a per-column `_cast_to_schema()` helper instead of `RecordBatch.cast()`, restoring compatibility with PyArrow ≥ 11. The previous code required PyArrow ≥ 14 and silently broke on conda/Spark images shipping older versions.
- `worker.py` CSV reader now decodes `dsdgen` output as `latin1` — fixes mid-stream `invalid UTF8 data` errors seen at large SF where name/address fields contain non-ASCII bytes.
- `spark/job.py` now `os.makedirs` the table subdirectory before writing Parquet on local/POSIX outputs, and gained `normalize_output()` to translate `dbfs:/...` URIs to the Databricks `/dbfs/...` FUSE mount.
- README rewritten around the three-tier design (DuckDB / dsdgen-multiprocess / Spark) with measured single-node performance numbers.

### Fixed
- Spark variant on Databricks: `dbfs:/...` outputs now route through the FUSE mount instead of the no-longer-supported `hdfs dfs -put` path.

## [0.2.0] — 2026-04

### Added
- `spark_tpcds_gen.py` — distributed generator for SF ≥ 1000 on Spark/YARN; proven at SF=10000 (3.6 TB) on 10× E16ads_v5 in 3h 51m.

## [0.1.0] — 2026-03

### Added
- Initial release: `tpcds-gen` CLI with two engines.
- `duckdb` engine: in-process `dsdgen()` + `COPY ... PARQUET`.
- `dsdgen` engine: multiprocess with the official C `dsdgen -PARALLEL`, streaming `.dat` → Parquet via PyArrow with constant per-worker memory.
- Calibrated row-group sizes, full TPC-DS schema (decimals, dates, time32), companion-table handling for `store_sales`/`store_returns` and similar pairs.

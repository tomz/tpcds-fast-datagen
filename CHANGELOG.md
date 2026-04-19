# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Auto engine selection: `--engine auto` (default) picks `duckdb` for SF ≤ 50 and `dsdgen` for SF > 50, with `--auto-threshold N` to override the cutoff.
- `benchmarks/bench_engines.py` — head-to-head harness measuring wall-clock + peak RSS for both engines, with OOM detection.
- `docs/spark-sizing-best-practices.md` (and HTML version) — cluster sizing tables for E8ads/E16ads × 3/5/10/15/20 nodes × SF=1000/10000/100000.
- `tests/test_streaming_cast.py` — regression tests for the streaming `.dat` → Parquet cast path.
- `LICENSE`, `CONTRIBUTING.md`, `CHANGELOG.md`.
- GitHub Actions CI: pytest + coverage on push and PR.

### Changed
- `worker.py` now uses a per-column `_cast_to_schema()` helper instead of `RecordBatch.cast()`, restoring compatibility with PyArrow ≥ 11. The previous code required PyArrow ≥ 14 and silently broke on conda/Spark images shipping older versions.
- README rewritten around the three-tier design (DuckDB / dsdgen-multiprocess / Spark) with measured single-node performance numbers.

## [0.2.0] — 2026-04

### Added
- `spark_tpcds_gen.py` — distributed generator for SF ≥ 1000 on Spark/YARN; proven at SF=10000 (3.6 TB) on 10× E16ads_v5 in 3h 51m.

## [0.1.0] — 2026-03

### Added
- Initial release: `tpcds-gen` CLI with two engines.
- `duckdb` engine: in-process `dsdgen()` + `COPY ... PARQUET`.
- `dsdgen` engine: multiprocess with the official C `dsdgen -PARALLEL`, streaming `.dat` → Parquet via PyArrow with constant per-worker memory.
- Calibrated row-group sizes, full TPC-DS schema (decimals, dates, time32), companion-table handling for `store_sales`/`store_returns` and similar pairs.

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **`tpcds-gen doctor`** — environment diagnostic subcommand. Reports Python
  version, pyarrow, duckdb (+ tpcds extension), dsdgen/tpcds.idx location, and
  pyspark. Exits nonzero if required pieces are missing. Replaces the
  one-crash-at-a-time discovery pattern that earlier users hit.
- **`tpcds-gen install-dsdgen`** — downloads a prebuilt `dsdgen` binary for the
  current platform from the project's GitHub Releases into
  `~/.cache/tpcds-fast-datagen/`. `--from-source` clones `databricks/tpcds-kit`
  and builds against the local glibc (use this on HDInsight / older cluster
  images where prebuilts fail with `GLIBC_2.34` not found).
- **`DSDGEN_URL` env var** — when set and no local `dsdgen` is found, the
  binary (and a sibling `tpcds.idx`, or `DSDGEN_IDX_URL` if set) is downloaded
  from the URL into the cache. Supports `http(s)://`, `file://`, and
  `abfs://`/`s3://`/`wasbs://`/`gs://` via `fsspec`. Lets Fabric / Databricks
  notebooks bootstrap a working `dsdgen` without `git clone` / `make`.
- **Prebuilt `dsdgen` binaries** attached to GitHub Releases, built by
  `.github/workflows/build-dsdgen.yml` for `{linux-x86_64, linux-arm64,
  macos-x86_64, macos-arm64}`. The workflow runs on release publish and on
  manual dispatch.
- `tpcds_fast_datagen.binary.install_dsdgen_from_url()`, `cache_dir()`,
  `platform_tag()`, `sha256sum()` — reusable helpers for programmatic
  bootstrap from notebooks.

### Changed
- CLI restructured as a click group with a default subcommand. The legacy
  flat form (`tpcds-gen --scale 1 --output /tmp/sf1`) still works identically
  — it now routes through `generate` under the hood.
- `dsdgen` lookup order extended: `DSDGEN_PATH` → well-known paths →
  `~/.cache/tpcds-fast-datagen/dsdgen` → system `PATH` → `DSDGEN_URL` download.
- The `dsdgen not found` error message now suggests `tpcds-gen install-dsdgen`
  first (the easy path) and only falls back to the `git clone && make`
  instructions as the last option.
- `requires-python` bumped from `>=3.8` to `>=3.9` to match actual usage
  (PEP 604 `str | None` and PEP 585 `tuple[str, str]` annotations appear
  throughout `src/tpcds_fast_datagen/spark/` and would fail to import on
  3.8 without `from __future__ import annotations`, which wasn't applied
  everywhere).
- `duckdb` minimum bumped from `>=0.10` to `>=1.0`.
- README notebook example no longer claims `%pip install tpcds-fast-datagen`
  (not on PyPI yet) — points at the v0.3.1 GitHub Release wheel URL instead.

### Fixed
- `docs/sf30k-hdi-runbook.md` and `docs/live-test-status.md`: dangling
  references to developer-local scratch scripts (`hdi_build_venv.py`,
  `hdi_build_dsdgen.py` in `/tmp/tpcds-live-tests/`) replaced with pointers
  to `tpcds-gen install-dsdgen --from-source` and the in-repo live-test docs.

## [0.3.1] — 2026-04-20

### Fixed
- **Critical: int32 overflow in `*_ticket_number` and `*_order_number` columns** at SF ≥ ~10,000. These columns were declared `int32` in the PyArrow schema, but ticket/order numbers grow monotonically across chunks and exceed `2^31 = 2,147,483,648` mid-run. Observed failure at SF=30K: `ArrowInvalid: In CSV column #9: CSV conversion error to int32: invalid value '2307000001'`. Columns changed from `int32` to `int64`:
  - `ss_ticket_number`, `sr_ticket_number`
  - `cs_order_number`, `cr_order_number`
  - `ws_order_number`, `wr_order_number`

  **Users running v0.3.0 or earlier at SF ≥ 10,000 should upgrade.**

### Verified
- **SF=10,000 end-to-end on 5× E32ads_v5 (160 slots) on HDInsight:** 1 h 41 min wall-clock, 3.95 TB to ABFS, zero failures, zero storage throttling.
- **SF=30,000 end-to-end on 5× E32ads_v5 (160 slots) on HDInsight:** 3 h 10 min wall-clock, 11.85 TB to ABFS, zero failures, zero storage throttling. See [`docs/sf30k-hdi-runbook.md`](docs/sf30k-hdi-runbook.md) §13 for the full writeup.

### Docs
- `docs/spark-sizing-best-practices.{md,html}` — promoted the two 5× E32ads_v5 production runs to the top, with full per-run metrics (cost, throughput, failures, throttling). Demoted the 10× E16ads_v5 / HDFS / v0.2.x run to a historical reference.
- `docs/sf30k-hdi-runbook.md` — added §13 with actual results, bugs caught, knob analysis, and cost breakdown from the SF=30K production run.
- HTML sizing doc upgraded to publication quality: serif body, dark-mode palette, responsive tables, Open Graph metadata, production/historical badges, print stylesheet.

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

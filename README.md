# TPC-DS Fast Datagen

[![CI](https://github.com/tomz/tpcds-fast-datagen/actions/workflows/ci.yml/badge.svg)](https://github.com/tomz/tpcds-fast-datagen/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](pyproject.toml)

**Author:** Tom Zeng ([@tomz](https://github.com/tomz))

The fast TPC-DS data generator — parallel, streaming, Parquet-native, single-node *and* distributed.

## Why

Existing TPC-DS data generators each fail at a different scale (**SF** = TPC-DS *scale factor*; SF=1 ≈ 1 GB of raw data, SF=1000 ≈ 1 TB):

| tool | small SF (≤50) | mid SF (50–500) | huge SF (≥1000) |
|---|---|---|---|
| Official C `dsdgen` | CSV-only; you orchestrate the parallel children | same | same, plus you orchestrate across machines |
| DuckDB built-in `dsdgen()` | **✓ very fast** | OOM (holds all 25 tables in RAM) | ✗ |
| Databricks `spark-sql-perf` | overkill | ✓ | ✓ but heavy Spark/Scala stack |

**`tpcds-fast-datagen`** picks the right strategy automatically:

- **SF ≤ 50** → DuckDB engine: in-process `dsdgen()` + `COPY ... PARQUET`. As fast as DuckDB itself.
- **SF > 50** → dsdgen-multiprocess engine: shards via the official C `dsdgen -PARALLEL`, streams `.dat` → Parquet through PyArrow with **constant memory per worker** (~200 MB). Bound only by local disk.
- **SF ≥ 1000** (distributed) → `spark_tpcds_gen.py`: same per-task logic on a Spark/YARN cluster. Proven at **SF=30,000 (11.85 TB) in 3 h 10 m on 5× E32ads_v5**.

Output is always Parquet with calibrated row groups, sane TPC-DS types (decimals, dates, time32), and one subdirectory per table.

## Quick Start

### Single node (CLI)

```bash
# 1. Install the wheel (not on PyPI yet — see Releases for the latest tag)
pip install https://github.com/tomz/tpcds-fast-datagen/releases/download/v0.3.2/tpcds_fast_datagen-0.3.2-py3-none-any.whl

# 2. Grab a prebuilt `dsdgen` for your platform (linux-x86_64, linux-arm64, macos-arm64, macos-x86_64)
tpcds-gen install-dsdgen

# 3. (Optional) sanity-check the environment
tpcds-gen doctor

# 4. Generate
tpcds-gen --scale 1   --output /tmp/tpcds_sf1                      # ~25 s, DuckDB engine (auto)
tpcds-gen --scale 100 --output /mnt/data/tpcds_sf100 --parallel 8  # ~16 min on 16 cores, dsdgen-multiprocess
tpcds-gen --scale 50  --output /tmp/tpcds_sf50 --engine dsdgen     # force a specific engine
```

No `dsdgen` for your platform in Releases, or you're on an old-glibc cluster (e.g. HDInsight)? Build from source:

```bash
tpcds-gen install-dsdgen --from-source   # clones databricks/tpcds-kit and `make OS=LINUX` into the cache
```

### Distributed (Spark / YARN / notebooks)

**From a notebook (Fabric, Databricks, Jupyter) or Livy session:**

```python
# Install the wheel + pyarrow. The package is not on PyPI yet; grab the release wheel.
%pip install https://github.com/tomz/tpcds-fast-datagen/releases/download/v0.3.2/tpcds_fast_datagen-0.3.2-py3-none-any.whl pyarrow

# Point at a prebuilt dsdgen in blob/S3/GCS. The library will auto-download it
# to every executor on first use. Alternatives: set DSDGEN_PATH, or use
# spark-submit --files to ship a binary you already have.
import os
os.environ["DSDGEN_URL"] = "abfs://public@myacct.dfs.core.windows.net/tpcds/dsdgen-linux-x86_64"

from tpcds_fast_datagen.spark import generate
result = generate(spark, scale=1000, output="abfs:///tpcds/sf1000")
print(result.total_rows, result.elapsed_s)
```

**From `spark-submit`:**

```bash
tpcds-gen-spark-submit --scale 10000 --output abfs:///tpcds/sf10000 \
  -- \
  --master yarn --deploy-mode client \
  --num-executors 10 --executor-cores 16 --executor-memory 96g \
  --archives ./conda_env.tar.gz#conda_env \
  --files ./dsdgen,./tpcds.idx
```

**Or via the unified CLI (just delegates to `spark-submit` under the hood):**

```bash
tpcds-gen --engine spark --scale 1000 --output abfs:///tpcds/sf1000 \
  -- --master yarn --num-executors 10
```

End-to-end notebook + Livy `POST /batches` examples live in [`docs/notebooks-and-livy.md`](docs/notebooks-and-livy.md). Cluster sizing tables and chunk-tuning guidance live in [`docs/spark-sizing-best-practices.md`](docs/spark-sizing-best-practices.md). Verified-paths matrix and platform-specific workarounds (HDI Livy, Fabric SJD) are in [`docs/live-test-status.md`](docs/live-test-status.md). For a fully-worked example of running SF=30,000 on a 5-node HDInsight cluster with HDFS on attached disks (~7 h, ~$104), see [`docs/sf30k-hdi-runbook.md`](docs/sf30k-hdi-runbook.md).

## How engine selection works

```
                              --engine
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
            auto              duckdb              dsdgen
              │
   ┌──────────┴──────────┐
   │                     │
SF ≤ 50              SF > 50
   │                     │
duckdb               dsdgen
```

Override the SF=50 cutoff with `--auto-threshold N`. On a memory-poor box, drop the threshold (e.g. `--auto-threshold 10`); on a 256 GB box, raise it.

## Prerequisites

- **Python ≥ 3.9**
- **DuckDB engine** uses the bundled `tpcds` extension — installed automatically the first time the engine runs.
- **dsdgen engine** needs the official `dsdgen` binary from [tpcds-kit](https://github.com/databricks/tpcds-kit). Easiest:

    ```bash
    tpcds-gen install-dsdgen              # prebuilt from GH Releases (linux-x86_64, linux-arm64, macos-x86_64, macos-arm64)
    tpcds-gen install-dsdgen --from-source # clones tpcds-kit and builds (use on old glibc, e.g. HDInsight)
    ```

    Or point at an existing binary via any of:
    1. `DSDGEN_PATH=/path/to/dsdgen`
    2. `DSDGEN_URL=https://.../dsdgen` (+ optional `DSDGEN_IDX_URL`; supports `http(s)://`, `file://`, `abfs://`, `wasbs://`, `s3://`, `gs://`) — handy for notebooks
    3. `dsdgen` + `tpcds.idx` on your `PATH`

    Run `tpcds-gen doctor` to verify your environment.

## Architecture

### Single node — `tpcds_fast_datagen` package

```
tpcds-gen CLI
   │
   ├─ engine=auto ──► picks based on --scale and --auto-threshold
   │
   ├─ engine=duckdb ──► single in-process DuckDB connection
   │     CALL dsdgen(sf=N)            # generates all 25 tables in RAM
   │     COPY <table> TO '...parquet' # one COPY per table
   │
   └─ engine=dsdgen ──► ProcessPoolExecutor of N workers, each:
         dsdgen -SCALE SF -PARALLEL N -CHILD K -DIR /tmp/chunk_K
              → pyarrow.csv.open_csv (1M-row batches, constant memory)
              → ParquetWriter.write_batch (incremental row groups)
```

Companion tables (e.g. `store_sales` + `store_returns`) are emitted in a single `dsdgen` invocation and written by the same worker. Small tables (<1M rows at this SF) are only generated by child 1, mirroring `dsdgen`'s internal `split_work()`.

### Distributed — Spark variant

Lives in `src/tpcds_fast_datagen/spark/` (with a thin back-compat shim at `spark_tpcds_gen.py` in the repo root). Both driver and executors must have the wheel importable. The planner shards the same chunked task list, parallelises via `mapPartitions` across YARN executors, and each executor task runs the same `.dat` → PyArrow → Parquet streaming pipeline. Output goes to HDFS / ABFS / GCS / S3 via `fs.defaultFS`.

The driver picks `chunks ≈ max(slots, biggest_table_rows / 150M)` by default; pass `--chunks N` to override.

## Performance

### Single node (Standard_E16ads_v5 reference box: 16 cores, 125 GB RAM, NVMe-backed local SSD)

All numbers below are measured by `benchmarks/bench_engines.py` on the same box; raw JSON in `bench_engines_*_results.json`. **Wall** is end-to-end (dsdgen + write Parquet); **peak RSS** is process + children high-water mark via `getrusage`.

| SF | DuckDB wall | DuckDB peak RSS | dsdgen wall (p=16) | dsdgen peak RSS | output | speedup |
|---:|---:|---:|---:|---:|---:|---:|
| 5   | (fast) | (fits) | 116 s | 1.3 GB | 2.0 GB | — |
| 10  | (fast) | (fits) | 190 s | 1.3 GB | 4.1 GB | — |
| 20  | (fast) | (fits) | 147 s | 1.0 GB | 7.0 GB | — |
| 50  | **720 s** | **98 GB** | **368 s** | **1.3 GB** | 18 GB | **1.96×** |
| 100 | **1633 s** | **118 GB** | **817 s** | **1.4 GB** | 38 GB | **2.00×** |
| 200 | **OOM** ✗ | (>125 GB) | **1468 s** | **1.4 GB** | 67 GB | ∞ |
| 300 | OOM (extrapolated) | — | 2731 s ✓ proven (older log) | ~1.4 GB | 100 GB | ∞ |

Two empirical takeaways:

1. **DuckDB peak RSS scales linearly at ~1.2 GB per SF.** It crosses the 125 GB box ceiling between SF=100 and SF=200 — measured OOM at SF=200, exactly as the auto-engine cutoff anticipates.
2. **dsdgen-multiprocess RSS is flat at ~1.3–1.4 GB regardless of SF.** Memory is bounded by `parallel × per-worker batch buffer`, not by data size. The bottleneck is local disk throughput.

At SF ≤ 20 DuckDB completes in seconds; the table omits its wall numbers because the difference is dominated by process startup. The auto-engine threshold of SF=50 is the point where dsdgen starts to win on both axes (faster *and* a 75× smaller memory footprint).

### Distributed (Spark, ABFS output)

| SF | cluster | wall-clock | output |
|---:|---|---:|---:|
| 1,000  | 10× E16ads_v5 | ~14 min (est.) | ~360 GB |
| 10,000 | 10× E16ads_v5 (80 slots used) | **3 h 51 m proven** | **3.6 TB** |
| 10,000 | 10× E16ads_v5 (160 slots) | ~2 h 15 m (est.) | 3.6 TB |
| 100,000 | 15× E16ads_v5 | ~15 h (est.) | ~36 TB |

See [`docs/spark-sizing-best-practices.md`](docs/spark-sizing-best-practices.md) and the HTML version for full sizing tables across 3/5/10/15/20-node clusters of E8ads_v5 and E16ads_v5.

## Roadmap

- **v0.1** ✓ shipped: DuckDB engine + dsdgen-multiprocess engine + Parquet output
- **v0.2** ✓ shipped: `spark_tpcds_gen.py` for distributed generation (SF=10000 proven)
- **v0.3.0** ✓ shipped: auto-engine selection (`--engine auto`), benchmark harness, Spark sizing docs, Spark Python API
- **v0.3.1** ✓ shipped: int64 fix for `*_ticket_number` / `*_order_number` at SF ≥ 10K (see CHANGELOG)
- **v0.3.2** ✓ shipped: `tpcds-gen doctor`, `tpcds-gen install-dsdgen`, `DSDGEN_URL` auto-bootstrap, prebuilt dsdgen binaries for linux/macOS × x86_64/arm64
- **v0.4**: fsspec support (S3, ADLS, GCS) for the single-node tool, Parquet validation
- **v1.0**: Rust core with PyO3 bindings — target 5–10× faster than the dsdgen+pyarrow streaming path

## License

MIT

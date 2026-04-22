# HDInsight 5.1 quickstart

**Author:** Tom Zeng ([@tomz](https://github.com/tomz))
**Last updated:** 2026-04-21
**Applies to:** `tpcds-fast-datagen` ≥ 0.4.0 on Azure HDInsight 5.1 (Spark 3.3)

This is the **5-command path** to generate TPC-DS data on an HDInsight cluster.
For the deep-dive (cluster sizing, cost analysis, troubleshooting matrix),
see [`docs/sf30k-hdi-runbook.md`](sf30k-hdi-runbook.md).

---

## Why HDI needs special handling

HDInsight 5.1 ships an Ubuntu 18.04-based image with:

- `glibc 2.27` — modern dsdgen prebuilt binaries need 2.33+, refuse to run.
- `libstdc++.so.6` topping out at `GLIBCXX_3.4.25` — modern pyarrow wheels
  need `3.4.26+`, fail at `import pyarrow`.

The fix is to ship a self-contained Python env (built with `conda-pack`)
to every executor via Spark's `--archives`, plus a glibc-2.27-compatible
`dsdgen` binary built on the cluster itself. The `package-env`,
`install-dsdgen --target hdi-glibc-2.27`, and `--hdi` / `--via-livy` flags
in this release automate all of that.

---

## The 5 commands

Run on the HDI **headnode** (SSH in as `sshuser`). Replace
`<account>` / `<container>` / `<cluster>` with your values.

```bash
# 0. Once: install tpcds-fast-datagen on the headnode
pip install tpcds-fast-datagen

# 1. Build the env tarball and upload it to cluster storage
tpcds-gen package-env \
    --output /tmp/tpcds-env.tar.gz \
    --upload wasbs://<container>@<account>.blob.core.windows.net/bench/tpcds-env.tar.gz

# 2. Build a glibc-2.27-compatible dsdgen on the headnode and upload it
tpcds-gen install-dsdgen --target hdi-glibc-2.27   # auto-falls-through to --from-source
hadoop fs -put -f ~/.cache/tpcds-fast-datagen/dsdgen \
    ~/.cache/tpcds-fast-datagen/tpcds.idx \
    wasbs://<container>@<account>.blob.core.windows.net/bench/

# 3. Submit the job via Livy
export HDI_ADMIN_PASSWORD='your-cluster-admin-password'
tpcds-gen-spark-submit \
    --hdi \
    --via-livy https://<cluster>.azurehdinsight.net \
    --env wasbs://<container>@<account>.blob.core.windows.net/bench/tpcds-env.tar.gz \
    --binary wasbs://<container>@<account>.blob.core.windows.net/bench/dsdgen \
    --dists wasbs://<container>@<account>.blob.core.windows.net/bench/tpcds.idx \
    --scale 100 \
    --output wasbs://<container>@<account>.blob.core.windows.net/bench/tpcds-out
```

The `tpcds-gen-spark-submit` call POSTs to `/livy/batches`, polls every
30 s, and prints the YARN appId and `yarn logs` command on completion.

---

## What each command does under the hood

### `tpcds-gen package-env`

Creates a throwaway conda env with `python=3.9 pyarrow=14 duckdb=1.0 click
rich conda-pack`, installs the current `tpcds_fast_datagen` wheel into it,
then runs `conda pack` to produce a relocatable tarball (~280 MB).
Optionally uploads via `hadoop fs -put -f`.

The pinned versions matter: `pyarrow=14` is the last release linking
against `GLIBCXX_3.4.25` (HDI's max). `pyarrow=15+` will import-crash on
HDI executors. Don't bump.

### `tpcds-gen install-dsdgen --target hdi-glibc-2.27`

Tries to download a prebuilt binary built on a manylinux2014 (CentOS 7,
glibc 2.17) GHA runner. If no such asset exists for the current release
tag, it falls through to `--from-source`, which:

1. clones `databricks/tpcds-kit`
2. runs `make OS=LINUX` with gcc-14-compatible CFLAGS
3. copies `dsdgen` + `tpcds.idx` into the cache

Run on the HDI headnode, this produces a binary against `glibc 2.27` that
runs unmodified on every executor.

### `tpcds-gen-spark-submit --hdi --via-livy ...`

Two things in one:

- **`--hdi`** splices in the fixed flag block:
  ```
  --archives wasbs://.../tpcds-env.tar.gz#env
  --files wasbs://.../dsdgen,wasbs://.../tpcds.idx
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/bin/python
  --conf spark.executorEnv.PYSPARK_PYTHON=./env/bin/python
  ```
- **`--via-livy <url>`** instead of running `spark-submit` locally,
  POSTs the equivalent JSON to `<url>/livy/batches` with the
  `X-Requested-By` header HDI's CSRF guard requires. Polls
  `/livy/batches/<id>/state` every 30 s, prints the YARN appId on
  completion.

Authentication: HTTP Basic with `HDI_ADMIN_USER` (default `admin`) and
`HDI_ADMIN_PASSWORD` env vars.

---

## Verification

After the job's `state == success`:

```bash
# On the headnode:
hadoop fs -du -h wasbs://<container>@<account>.../bench/tpcds-out/sf100/
# Should show 24 table directories totaling ~36 GB at SF=100.

# Pull the YARN logs to see the per-table row counts:
yarn logs -applicationId <appId> -log_files stdout 2>&1 | grep -A 30 'TPC-DS Livy'
```

Expected store_sales row counts:

| SF | store_sales rows |
|---:|---:|
| 1 | 2,880,404 |
| 10 | 28,800,991 |
| 100 | 287,997,024 |
| 1000 | ~2,879,987,999 |
| 10000 | ~28,800,000,000 |
| 30000 | ~86,400,000,000 |

---

## Pre-flight: `tpcds-gen doctor`

Run this on the headnode before step 1:

```bash
tpcds-gen doctor
```

The HDI-specific rows you care about:

- `[OK] conda-pack (HDI optional)` — needed for `package-env`
- `[WARN] libstdc++ GLIBCXX (HDI)` — expected on HDI 5.1; this is **why**
  you need `package-env`.

If `conda-pack` is missing, install Miniforge first:

```bash
curl -L -O https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
bash Miniforge3-Linux-x86_64.sh -b -p ~/miniforge3
export PATH=~/miniforge3/bin:$PATH
```

---

## Troubleshooting

| symptom | likely cause | fix |
|---|---|---|
| `ImportError: GLIBCXX_3.4.26 not found` | env tarball not unpacked | confirm `--archives` URI is reachable; check `spark.executorEnv.PYSPARK_PYTHON=./env/bin/python` |
| `dsdgen: GLIBC_2.33 not found` | uploaded a modern dsdgen | rebuild on the headnode with `tpcds-gen install-dsdgen --target hdi-glibc-2.27` |
| `Futures timed out after [100000 milliseconds]` | slow setup on the AM before SparkSession init | not your bug — the bootstrap `tpcds-gen` ships initializes SparkSession first |
| `400 Bad Request` from Livy | missing `X-Requested-By` header | always go through `--via-livy`; never craft the POST by hand |
| `dsdgen_path` ignored | (pre-0.4.0 footgun) — fixed in 0.4.0 | upgrade |

---

## Cost reference

A 5-node `Standard_E32ads_v5` cluster runs at ~$10/h (5×$1.93 worker +
2×$0.97 head). Approximate end-to-end SF wall times measured on this
cluster:

| SF | wall | cost |
|---:|---:|---:|
| 1 | ~1 min | $0.17 |
| 10 | ~2 min | $0.33 |
| 100 | ~8 min | $1.33 |
| 1000 | ~50 min | $8.33 |
| 10000 | 1 h 41 min | ~$16.83 |
| 30000 | 3 h 10 min | ~$31.67 |

For SF ≥ 10K tuning knobs (bigger workers, more chunks, ABFS instead of
HDFS), see the deep-dive [`docs/sf30k-hdi-runbook.md`](sf30k-hdi-runbook.md).

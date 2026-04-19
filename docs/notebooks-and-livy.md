# Distributed generation: notebooks, Livy, and `spark-submit`

`tpcds-fast-datagen` ships a single Spark code path, accessed three ways:

1. A Python function (`tpcds_fast_datagen.spark.generate`) — best for notebooks and Livy session statements.
2. A bundled bootstrap script + helper (`tpcds-gen-spark-submit`) — best for batch / Livy `POST /batches` / SSH.
3. The legacy `spark_tpcds_gen.py` shim at the repo root — preserved for back-compat.

All three paths execute the same `tpcds_fast_datagen.spark.api.generate(spark, ...)` underneath.

## 1. Microsoft Fabric notebook

```python
%pip install tpcds-fast-datagen pyarrow

# dsdgen binary must be reachable on every executor. Two common patterns:
#   (a) Bake it into the cluster image (preferred for repeated runs)
#   (b) Distribute via SparkContext.addFile() in a notebook cell
sc = spark.sparkContext
sc.addFile("abfs://bin@account.dfs.core.windows.net/dsdgen")
sc.addFile("abfs://bin@account.dfs.core.windows.net/tpcds.idx")

from tpcds_fast_datagen.spark import generate

result = generate(
    spark,
    scale=1000,
    output="abfs://lake@account.dfs.core.windows.net/tpcds/sf1000",
)
print(f"Generated {result.total_rows:,} rows in {result.elapsed_s:.0f}s")
```

## 2. Databricks notebook

```python
%pip install tpcds-fast-datagen pyarrow
dbutils.library.restartPython()

# Upload dsdgen + tpcds.idx to a workspace volume / DBFS, then distribute:
sc = spark.sparkContext
sc.addFile("/Volumes/main/default/tpcds_bin/dsdgen")
sc.addFile("/Volumes/main/default/tpcds_bin/tpcds.idx")

from tpcds_fast_datagen.spark import generate

result = generate(
    spark,
    scale=10000,
    output="s3://my-bucket/tpcds/sf10000",
    chunks=783,         # match the proven SF=10000 sizing; omit for auto
)
display(result.table_rows)
```

## 3. Livy `POST /sessions/{id}/statements`

After `POST /sessions` with `conf.spark.archives` (or pre-installing the wheel on every node), submit a statement:

```bash
curl -s -X POST "$LIVY/sessions/$SESSION/statements" \
     -H "Content-Type: application/json" \
     -d '{
       "kind": "pyspark",
       "code": "from tpcds_fast_datagen.spark import generate\nresult = generate(spark, scale=1000, output=\"abfs:///tpcds/sf1000\")\nprint(result.total_rows)"
     }'
```

## 4. Livy `POST /batches`

Use the bundled bootstrap that ships inside the wheel. Get its path on a host that has the package installed:

```bash
python -c "from tpcds_fast_datagen.spark.submit import bootstrap_path; print(bootstrap_path())"
# /opt/conda/lib/python3.11/site-packages/tpcds_fast_datagen/spark/_bootstrap.py
```

Then submit the batch:

```bash
curl -s -X POST "$LIVY/batches" \
     -H "Content-Type: application/json" \
     -d '{
       "file": "abfs:///bootstraps/_bootstrap.py",
       "args": ["{\"scale\": 1000, \"output\": \"abfs:///tpcds/sf1000\"}"],
       "pyFiles": ["abfs:///wheels/tpcds_fast_datagen-0.3.0-py3-none-any.whl"],
       "files": ["abfs:///bin/dsdgen", "abfs:///bin/tpcds.idx"],
       "numExecutors": 10,
       "executorCores": 16,
       "executorMemory": "96g"
     }'
```

## 5. `spark-submit` from the SSH gateway

```bash
pip install tpcds-fast-datagen
tpcds-gen-spark-submit --scale 10000 \
                      --output abfs:///tpcds/sf10000 \
                      -- \
                      --master yarn --deploy-mode client \
                      --num-executors 10 --executor-cores 16 --executor-memory 96g \
                      --archives /path/to/conda_env.tar.gz#conda_env \
                      --files /path/to/dsdgen,/path/to/tpcds.idx
```

`tpcds-gen-spark-submit --print …` prints the argv it would `execvp` instead of running it — handy for debugging or for piping into a job scheduler.

## 6. The legacy script (still works)

```bash
spark-submit --master yarn ... spark_tpcds_gen.py --scale 1000 --output abfs:///tpcds/sf1000
```

`spark_tpcds_gen.py` at the repo root is now a 50-line shim that builds a `SparkSession` and forwards to `tpcds_fast_datagen.spark.generate`. Both driver and executors must have the wheel installed.

## Distributing the `dsdgen` binary

`generate(...)` searches for `dsdgen` in this order on each executor:

1. The `dsdgen_path=` kwarg (highest priority).
2. `SparkFiles.get("dsdgen")` — set by `spark-submit --files dsdgen` or `sc.addFile("dsdgen")` in a notebook.
3. `$DSDGEN_PATH` env var.
4. The single-node `binary._SEARCH_PATHS` list.

`tpcds.idx` must sit alongside the binary; the helper auto-copies it from `SparkFiles.get("tpcds.idx")` when needed.

For repeated production use, prefer baking `dsdgen` + `tpcds.idx` into your cluster image at a stable path and pointing `dsdgen_path=` at it — `SparkFiles` distribution adds latency to every job.

## Verified paths and platform-specific workarounds

The matrix of paths verified end-to-end at SF=1 and SF=100 (HDInsight, Fabric, Databricks) is in [`docs/live-test-status.md`](live-test-status.md). Two paths require non-obvious workarounds and are documented there in detail:

- **HDInsight Livy** — every Python on the HDI 5.1 image ships a `pyarrow` linked against an unavailable `GLIBCXX_3.4.26`. Workaround: build a portable venv tarball *on the cluster* with `pyarrow==10.0.1` (last release linking against `3.4.25`) plus the wheel, and ship via `--archives venv.tar.gz#venv` so YARN unpacks it before the Python worker spawns.
- **Fabric Spark Job Definition (SJD)** — the SJD definition payload rejects a `conf` field, so `spark.archives` cannot be set at submit time. Workaround: extract the (pure-python) package on the driver, broadcast its bytes, and run a high-fanout bootstrap stage (`max(64, defaultParallelism * 8)` partitions) so every potential autoscaled executor stages the package before `generate(...)` runs.

Both workarounds bypass `SparkContext.addPyFile`, which doesn't propagate a wheel to executor Python workers in time for cloudpickle deserialization of the first task.

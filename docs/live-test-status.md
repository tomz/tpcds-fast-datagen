# Live integration test status

Status of end-to-end test runs of `tpcds-fast-datagen` against real cloud
Spark services. Last verified **2026-04-21** (HDI Livy paths re-verified
via the new `--via-livy` flow in 0.4.0; SF=10K and SF=30K results
preserved from 2026-04-19/20 measurements).

## Matrix

| Platform     | Submission path                          | SF=1    | SF=100  | SF=10K   | SF=30K  | Notes |
|--------------|------------------------------------------|---------|---------|----------|---------|-------|
| HDInsight 5.1 | `tpcds-gen-spark-submit --hdi --via-livy` | ✅      | ✅      | ✅ 1 h 41 m | ✅ 3 h 10 m | **0.4.0 just-works flow.** See [`hdinsight-quickstart.md`](hdinsight-quickstart.md) and [`sf30k-hdi-runbook.md`](sf30k-hdi-runbook.md) |
| HDInsight 5.1 | `spark-submit` (manual)                  | ✅ dev  | ✅ dev  | —        | —       | Original development path; superseded by --via-livy |
| HDInsight 5.1 | Livy batch (v2, manual)                  | ✅ ~30s | ✅ ~160s | —       | —       | Pre-0.4.0 path; subsumed by `--via-livy` (no manual `--archives` plumbing) |
| Fabric       | Livy session                             | ✅      | ✅      | —        | —       | |
| Fabric       | Spark Job Definition (v2)                | ✅ 82 s | ✅ 444 s (959M rows) | — | — | Needs broadcast-bytes + high-fanout bootstrap workaround |
| Fabric       | Notebook                                 | ✅      | ✅      | —        | —       | |
| Databricks   | `spark_python_task`                      | ✅      | ✅      | —        | —       | |
| Databricks   | `notebook_task`                          | ✅      | ✅      | —        | —       | |

The SF=10K and SF=30K runs were on a 5× E32ads_v5 + 2× E8ads_v5 cluster
against ABFS — see [`sf30k-hdi-runbook.md`](sf30k-hdi-runbook.md) §6 for
per-table breakdown and cost analysis.

## Workarounds (legacy — most fixed in 0.4.0)

Two of the seven paths needed non-obvious workarounds. The shared root cause:
**`SparkContext.addPyFile` does not propagate a wheel to executor python
workers in time for cloudpickle deserialization of the very first task** — by
the time the worker tries to import a module referenced in a closure, the
addFile-staged wheel has not yet been unpacked / placed on `sys.path`.
Anything that uses qualified-name closures (any normal `map(f)` where `f`
references a top-level function in `tpcds_fast_datagen`) will fail with
`ModuleNotFoundError` before the worker even starts running user code.

Both workarounds bypass `addPyFile` entirely and ensure the package is on
disk + `sys.path` **before** any python worker spawns.

### HDI Livy: `--archives venv.tar.gz#venv`

Root cause specific to HDI: every Python interpreter on the HDI 5.1 cluster
image has a broken `pyarrow` (links against `GLIBCXX_3.4.26`, image only
provides `3.4.25`). A diagnostic probe confirmed this across `py38`,
`py38jupyter`, `python3.6/3.10`, and `/opt/az/bin/python3.11`.

Fix:

1. **One-time:** build a portable venv tarball **on the cluster itself**
   (so it links against the cluster's glibc) containing pyarrow 10.0.1 (the
   last release linking against `GLIBCXX_3.4.25`) plus the `tpcds_fast_datagen`
   wheel. Upload to wasbs.
2. **One-time:** build `dsdgen` on the cluster too (pre-built binaries link
   against `GLIBC_2.34`, image provides older). Run
   `tpcds-gen install-dsdgen --from-source` on a worker node, or manually:
   `git clone https://github.com/databricks/tpcds-kit && cd tpcds-kit/tools && make OS=LINUX dsdgen`
   (skipping qgen avoids the yacc dependency).
3. **Per submission:** ship the venv tarball via YARN's
   `--archives URI#NAME` mechanism, which unpacks it into every container's
   working dir at launch time, before Python spawns.

   ```bash
   hdi-spark-submit \
     --archives "wasbs://.../tpcds_venv.tar.gz#venv" \
     --conf spark.executorEnv.PYTHONPATH=./venv \
     --conf spark.yarn.appMasterEnv.PYTHONPATH=./venv \
     wasbs://.../hdi_livy_v2.py 1 "$OUT" hdi-livy-sf1 \
       "$DSDGEN_URL" "$IDX_URL" "$LOG_URL"
   ```

   Entrypoint: `hdi_livy_v2.py` finds `./venv` next to the driver process
   (CWD = container working dir), prepends to `sys.path`, then runs the
   normal `tpcds_fast_datagen.spark.generate(...)`.

Key gotchas hit while developing this:

- `pip install --target` on HDI fails because `$HOME` isn't writable from
  YARN containers. Pass `env={**os.environ, "HOME": "/tmp",
  "PIP_CACHE_DIR": "/tmp/.pip-cache"}` to subprocess.
- pip rejects wheels that don't follow PEP 427 naming; preserve the original
  filename when fetching: `wheel_local = "/tmp/" + os.path.basename(url)`.
- spark-submit needs storage credentials for the script URL; use the
  cluster's primary wasbs (`<container>@<account>.blob.core.windows.net`)
  rather than a foreign abfss URL.

### Fabric SJD: broadcast bytes + high-fanout bootstrap

Root cause specific to Fabric SJD: the SJD definition payload **rejects a
`conf` field** (`ArtifactDefinitionPayloadPropertyNotFound: conf`), so we
can't set `spark.archives` / `spark.executorEnv.PYTHONPATH` at submit time.
Fabric's image already has a working pyarrow 22, so we only need to ship
the (pure-python) `tpcds_fast_datagen` package.

Fix: do the staging at **runtime** inside the entrypoint.

1. Pre-build a `fab_pkg.tar.gz` containing just the `tpcds_fast_datagen`
   package directory and upload to OneLake.
2. In the entrypoint (`fabric_sjd_v2.py`):
   - Driver downloads the archive via `sc.binaryFiles(...)`, extracts to
     `/tmp/fab_pkg`, and prepends to `sys.path`.
   - **Both** `sc.addFile(arch_local)` *and* a broadcast variable carrying
     the bytes are set up — different executors find one or the other
     depending on launch ordering.
   - Run a bootstrap stage with **WAY more partitions than there are
     current executors** (`max(64, defaultParallelism * 8)`) to force
     fan-out across every potential autoscaled host. Each task extracts
     the archive to `/tmp/fab_pkg` if missing and prepends to `sys.path`.

   ```python
   def _extract_and_check(_):
       d = "/tmp/fab_pkg"
       if not os.path.isdir(os.path.join(d, "tpcds_fast_datagen")):
           os.makedirs(d, exist_ok=True)
           try:
               from pyspark import SparkFiles
               src = SparkFiles.get("fab_pkg.tar.gz")
           except Exception:
               src = None
           if src and os.path.exists(src):
               with tarfile.open(src) as tf: tf.extractall(d)
           else:
               p = os.path.join(d, "_arch.tar.gz")
               with open(p, "wb") as f: f.write(arch_b.value)
               with tarfile.open(p) as tf: tf.extractall(d)
       if d not in sys.path:
           sys.path.insert(0, d)
       import pyarrow, tpcds_fast_datagen
       ...

   n = max(64, sc.defaultParallelism * 8)
   sc.parallelize(range(n), n).map(_extract_and_check).collect()
   ```

Without the high-fanout bootstrap, SF=100 failed with
`ModuleNotFoundError: tpcds_fast_datagen` partway through generation: the
initial 16-task bootstrap landed on a single executor host, then Fabric
autoscaled in fresh executors during `generate(...)` that had never seen
the bootstrap.

Submit it via `fabric_sjd_submit_v2.py`, which uses the standard
`fabric_spark_sjd.SjdClient` (no `conf` field).

## Reference scripts

In `/tmp/tpcds-live-tests/` (author's box, not checked in — they're
dev-local scratchpads, not part of the supported surface):

- `hdi_env_probe.py`, `hdi_python_scan.py` — diagnostics that confirmed
  every Python on the HDI image has a broken pyarrow.
- `hdi_pip_install.py` — proves `pip install --target` works once `HOME`
  is overridden.
- `hdi_build_venv.py` — builds the portable venv tarball (the steps it
  runs are described in §"HDI: venv workaround" above; for most users,
  `pip install` into a fresh venv + `tar czf` is sufficient).
- `hdi_build_dsdgen.py` — builds dsdgen on-cluster (equivalent to
  `tpcds-gen install-dsdgen --from-source`).
- `hdi_livy_v2.py` — HDI Livy entrypoint using `--archives`.
- `fabric_sjd_v2.py` — Fabric SJD entrypoint with broadcast-bootstrap.
- `fabric_sjd_submit_v2.py` — Fabric SJD submitter.
- `databricks_notebook.py`, `databricks_notebook_submit.py` — Databricks
  paths.

Logs from the verifying runs are in the same directory (`*_sf1*.log`,
`*_sf100*.log`).

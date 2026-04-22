"""HDInsight 5.1 'just-works' helpers.

Two top-level entry points:

* :func:`package_env` — build a conda-pack tarball with the exact recipe
  this project's executors need (pinned Python + pyarrow + duckdb + the
  current package wheel). Optionally upload it to cluster-visible storage.
* :func:`livy_submit` — POST a Spark batch to the cluster's Livy endpoint
  with the right CSRF header, poll until terminal, return the YARN appId.

Both are designed to absorb the manual steps from the historical
``sf30k-hdi-runbook.md``: see ``docs/hdinsight-quickstart.md`` for the
user-facing workflow.

The Livy code uses only ``urllib`` from stdlib so the CLI works on any
Python 3.9+ install (HDI's headnode Python included).
"""
from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# package-env
# ---------------------------------------------------------------------------

# Pinned recipe: matches the conda-pack we built by hand on 2026-04-21 that
# was verified against HDI 5.1 (Spark 3.3.1.5, Python 3.8 stock). Bumping
# pyarrow past 14 reintroduces the GLIBCXX_3.4.26 problem on HDI's old
# libstdc++. Bumping duckdb is fine in principle but 1.0 is the LTS line.
DEFAULT_PYTHON = "3.9"
DEFAULT_PYARROW = "14"
DEFAULT_DUCKDB = "1.0"
DEFAULT_PACKAGES_COMMON = ["click", "rich", "conda-pack"]
# Per-tool extras: TPC-DI needs lxml for the XML converter; H/DS don't.
DEFAULT_PACKAGES_THIS = []  # tpcds-fast-datagen needs no extras


def _find_conda() -> str:
    """Locate a conda/mamba/micromamba binary. Returns the executable path.

    Resolution order: $CONDA_EXE, $MAMBA_EXE, then PATH lookups for
    ``mamba``, ``micromamba``, ``conda``. Raises ``RuntimeError`` if none
    are available — we don't auto-bootstrap Miniforge (yet); user can
    install it themselves.
    """
    for var in ("CONDA_EXE", "MAMBA_EXE"):
        v = os.environ.get(var)
        if v and os.path.exists(v):
            return v
    for tool in ("mamba", "micromamba", "conda"):
        p = shutil.which(tool)
        if p:
            return p
    raise RuntimeError(
        "No conda/mamba/micromamba on PATH. Install Miniforge from "
        "https://github.com/conda-forge/miniforge#install and re-run."
    )


def _find_wheel() -> Path | None:
    """Find a built wheel for the current package, if any.

    Looks under ``<repo>/dist/*.whl`` (relative to the installed source).
    Returns the newest wheel by mtime, or ``None`` if none exist.
    """
    try:
        import tpcds_fast_datagen  # noqa: F401 — locate the package dir
        pkg_dir = Path(tpcds_fast_datagen.__file__).resolve().parent  # type: ignore[name-defined]
    except ImportError:
        return None
    # Walk up: pkg/ -> src/ -> repo root -> dist/
    for parent in [pkg_dir, *pkg_dir.parents]:
        dist = parent / "dist"
        if dist.is_dir():
            wheels = sorted(dist.glob("tpcds_fast_datagen-*.whl"),
                            key=lambda p: p.stat().st_mtime,
                            reverse=True)
            if wheels:
                return wheels[0]
    return None


def package_env(
    output: str | Path,
    *,
    python_version: str = DEFAULT_PYTHON,
    pyarrow_version: str = DEFAULT_PYARROW,
    duckdb_version: str = DEFAULT_DUCKDB,
    extra_packages: list[str] | None = None,
    wheel: str | Path | None = None,
    upload: str | None = None,
    quiet: bool = False,
) -> Path:
    """Build a conda-pack tarball ready for ``spark-submit --archives``.

    Returns the local path to the produced tarball. If ``upload`` is set
    (a wasbs:// / abfs:// / hdfs:// URI), also runs ``hadoop fs -put -f``
    to copy it to cluster storage.

    The env name is throwaway (``tpcds-pkg-<pid>``); we delete it after
    packing.
    """
    out_path = Path(output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conda = _find_conda()
    extras = list(DEFAULT_PACKAGES_COMMON + DEFAULT_PACKAGES_THIS)
    if extra_packages:
        extras.extend(extra_packages)

    env_name = f"tpcds-pkg-{os.getpid()}"
    pkg_spec = [
        f"python={python_version}",
        f"pyarrow={pyarrow_version}",
        f"duckdb={duckdb_version}",
        *extras,
    ]

    def _say(msg: str) -> None:
        if not quiet:
            print(msg, flush=True)

    _say(f"package-env: creating env {env_name} via {conda}")
    _say(f"  spec: {' '.join(pkg_spec)}")

    create_cmd = [conda, "create", "-y", "-n", env_name, "-c", "conda-forge", *pkg_spec]
    subprocess.run(create_cmd, check=True)

    try:
        # Resolve env prefix
        prefix = subprocess.check_output(
            [conda, "env", "list", "--json"], text=True,
        )
        prefixes = json.loads(prefix).get("envs", [])
        env_prefix = next((p for p in prefixes if p.endswith("/" + env_name) or p.endswith("\\" + env_name)), None)
        if not env_prefix:
            raise RuntimeError(f"Could not locate env prefix for {env_name}")
        env_python = os.path.join(env_prefix, "bin", "python")

        # Install our wheel into the env so executors have the right code.
        wheel_path = Path(wheel).resolve() if wheel else _find_wheel()
        if wheel_path is None:
            _say("  no wheel found; building one with `python -m build` ...")
            try:
                subprocess.run([sys.executable, "-m", "build", "--wheel"], check=True)
                wheel_path = _find_wheel()
            except (subprocess.CalledProcessError, FileNotFoundError):
                _say("  WARNING: wheel build failed; env will lack tpcds_fast_datagen.")
        if wheel_path is not None:
            _say(f"  pip-installing wheel: {wheel_path}")
            subprocess.run([env_python, "-m", "pip", "install", "--quiet", str(wheel_path)], check=True)

        _say(f"package-env: conda-pack -> {out_path}")
        # Use the env's own conda-pack binary (we installed it above) so the
        # version matches the env's python.
        env_conda_pack = os.path.join(env_prefix, "bin", "conda-pack")
        if not os.path.exists(env_conda_pack):
            env_conda_pack = shutil.which("conda-pack") or env_conda_pack
        subprocess.run(
            [env_conda_pack, "-n", env_name, "-o", str(out_path), "--force"],
            check=True,
        )

    finally:
        _say(f"package-env: removing throwaway env {env_name}")
        subprocess.run([conda, "env", "remove", "-y", "-n", env_name], check=False)

    if upload:
        _upload(out_path, upload, quiet=quiet)

    _say(f"package-env: done — {out_path} ({out_path.stat().st_size // (1024*1024)} MB)")
    return out_path


def _upload(local_path: Path, remote_uri: str, *, quiet: bool = False) -> None:
    """Copy ``local_path`` to ``remote_uri`` using whichever tool fits.

    ``wasbs://`` / ``abfs://`` / ``hdfs://`` go through ``hadoop fs -put -f``
    (works from HDI headnodes and any host with the Hadoop client). If
    ``hadoop`` isn't on PATH and the URI is a ``wasbs://`` blob, falls back
    to ``az storage blob upload`` so the same call works from a laptop with
    only the Azure CLI installed.
    """
    if not quiet:
        print(f"package-env: uploading to {remote_uri}", flush=True)
    if remote_uri.startswith(("wasbs://", "wasb://", "abfs://", "abfss://", "hdfs://", "s3a://", "s3://")):
        if shutil.which("hadoop"):
            subprocess.run(["hadoop", "fs", "-put", "-f", str(local_path), remote_uri], check=True)
            return
        if remote_uri.startswith(("wasbs://", "wasb://", "abfs://", "abfss://")) and shutil.which("az"):
            account, container, blob_path = _parse_wasbs(remote_uri)
            subprocess.run(
                ["az", "storage", "blob", "upload", "--overwrite",
                 "--account-name", account, "--container-name", container,
                 "--name", blob_path, "--file", str(local_path),
                 "--auth-mode", "key", "--only-show-errors"],
                check=True,
            )
            return
        raise RuntimeError(
            f"Cannot upload to {remote_uri}: neither `hadoop` nor `az` (with a "
            "matching scheme) is on PATH."
        )
    # Treat as a local path (handy for dev).
    shutil.copy2(local_path, remote_uri)


def _parse_wasbs(uri: str) -> tuple[str, str, str]:
    """``wasbs://container@account.blob.core.windows.net/path`` ->
    ``(account, container, path)``.
    """
    rest = uri.split("://", 1)[1]
    auth, _, blob_path = rest.partition("/")
    container, _, host = auth.partition("@")
    account = host.split(".", 1)[0]
    return account, container, blob_path


# ---------------------------------------------------------------------------
# livy-submit
# ---------------------------------------------------------------------------

# How often to poll Livy for state changes. 30s matches the cadence we used
# on the cluster runs (low enough to feel responsive, high enough to avoid
# hammering the gateway during multi-hour SF=10K runs).
LIVY_POLL_SEC = 30
LIVY_TERMINAL_STATES = {"success", "dead", "killed", "error"}


def _basic_auth_header(user: str, password: str) -> str:
    raw = f"{user}:{password}".encode()
    return "Basic " + base64.b64encode(raw).decode()


def _livy_post(url: str, payload: dict, user: str, password: str) -> dict:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Requested-By": user,  # required by HDI's Livy CSRF guard
            "Authorization": _basic_auth_header(user, password),
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def _livy_get(url: str, user: str, password: str) -> dict:
    req = urllib.request.Request(
        url, method="GET",
        headers={
            "X-Requested-By": user,
            "Authorization": _basic_auth_header(user, password),
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def livy_submit(
    *,
    cluster_url: str,
    scale: int,
    output: str,
    chunks: int | None,
    compression: str,
    env_uri: str,
    binary_uri: str,
    dists_uri: str | None,
    admin_user: str,
    admin_password: str,
    poll: bool = True,
    extra_conf: list[str] | None = None,
    print_only: bool = False,
    driver_memory: str = "8g",
    driver_cores: int = 4,
    executor_memory: str = "24g",
    executor_cores: int = 8,
    num_executors: int = 8,
) -> int:
    """Submit a TPC-DS datagen Spark job via the cluster's Livy REST API.

    The ``cluster_url`` is the HDI gateway, e.g.
    ``https://<name>.azurehdinsight.net``. We POST to ``/livy/batches``,
    poll ``/livy/batches/<id>/state`` every 30 s, and return 0 on success
    (state == ``success``), nonzero otherwise.

    Returns a process exit code.
    """
    cluster_url = cluster_url.rstrip("/")
    batches_url = f"{cluster_url}/livy/batches"

    files = [binary_uri]
    if dists_uri:
        files.append(dists_uri)

    bootstrap_remote = _ensure_bootstrap_uri(env_uri, admin_user, admin_password)

    payload: dict[str, Any] = {
        "file": bootstrap_remote,
        "args": [
            json.dumps({
                "scale": scale,
                "output": output,
                "chunks": chunks,
                "compression": compression,
                "dsdgen_path": None,  # let SparkFiles win (the footgun fix)
            }),
        ],
        "name": f"tpcds-sf{scale}",
        "files": files,
        "archives": [f"{env_uri}#env"],
        "conf": {
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./env/bin/python",
            "spark.executorEnv.PYSPARK_PYTHON": "./env/bin/python",
        },
        "driverMemory": driver_memory,
        "driverCores": driver_cores,
        "executorMemory": executor_memory,
        "executorCores": executor_cores,
        "numExecutors": num_executors,
    }

    # Splice in extra --conf X=Y from the user's leftover argv.
    if extra_conf:
        i = 0
        while i < len(extra_conf):
            if extra_conf[i] == "--conf" and i + 1 < len(extra_conf):
                k, _, v = extra_conf[i + 1].partition("=")
                payload["conf"][k] = v
                i += 2
            else:
                i += 1

    if print_only:
        print(json.dumps(payload, indent=2))
        return 0

    print(f"livy: POST {batches_url}", flush=True)
    resp = _livy_post(batches_url, payload, admin_user, admin_password)
    batch_id = resp["id"]
    print(f"livy: batch id = {batch_id}", flush=True)

    if not poll:
        return 0

    state_url = f"{batches_url}/{batch_id}/state"
    info_url = f"{batches_url}/{batch_id}"
    t0 = time.time()
    last_state = None
    while True:
        try:
            data = _livy_get(state_url, admin_user, admin_password)
            state = data.get("state", "?")
        except urllib.error.URLError as e:
            print(f"livy: poll error (will retry): {e}", flush=True)
            time.sleep(LIVY_POLL_SEC)
            continue
        elapsed = int(time.time() - t0)
        if state != last_state:
            print(f"livy: [{elapsed}s] {state}", flush=True)
            last_state = state
        if state in LIVY_TERMINAL_STATES:
            break
        time.sleep(LIVY_POLL_SEC)

    info = _livy_get(info_url, admin_user, admin_password)
    app_id = info.get("appId", "?")
    print(f"livy: appId={app_id}", flush=True)
    print(f"livy: yarn logs -applicationId {app_id}", flush=True)
    return 0 if state == "success" else 1


def _ensure_bootstrap_uri(env_uri: str, user: str, password: str) -> str:
    """Materialize a Livy-callable bootstrap script alongside the env tarball.

    Strategy: write a tiny driver script to a tempfile, ``hadoop fs -put``
    it next to the env tarball (same directory), and return its URI. The
    script imports ``tpcds_fast_datagen.spark.api`` from the unpacked env
    (so it has to be unpacked first — which is exactly what
    ``--archives env#env`` + ``PYSPARK_PYTHON=./env/bin/python`` arranges).
    """
    # Same directory as the env tarball.
    base, _, _ = env_uri.rpartition("/")
    bootstrap_uri = f"{base}/tpcds_livy_bootstrap.py"

    script = (
        "#!/usr/bin/env python3\n"
        "\"\"\"Auto-generated Livy bootstrap — calls tpcds_fast_datagen.spark.api.generate.\"\"\"\n"
        "import json, sys\n"
        "from pyspark.sql import SparkSession\n"
        "spark = SparkSession.builder.appName('tpcds-livy').getOrCreate()\n"
        "args = json.loads(sys.argv[1])\n"
        "from tpcds_fast_datagen.spark.api import generate\n"
        "result = generate(spark, scale=args['scale'], output=args['output'],\n"
        "                  chunks=args['chunks'], compression=args['compression'],\n"
        "                  dsdgen_path=args['dsdgen_path'])\n"
        "print(f'=== TPC-DS Livy SF={args[\"scale\"]} done in {result.elapsed_s:.1f}s ===', flush=True)\n"
        "for t, n in sorted(result.table_rows.items()):\n"
        "    print(f'  {t:12s} {n:>14,}', flush=True)\n"
        "spark.stop()\n"
    )

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
        tmp.write(script)
        tmp_path = tmp.name
    try:
        _upload(Path(tmp_path), bootstrap_uri, quiet=True)
    finally:
        os.unlink(tmp_path)
    return bootstrap_uri

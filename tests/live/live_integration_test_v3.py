"""Live integration test bootstrap (v3, robust).

Improvements over v2:
- Wheel + dsdgen + tpcds.idx are downloaded from URLs passed at the bottom
  (no --py-files or --files; works on Fabric Livy which rejects --py-files)
- pip install --no-deps the wheel into the driver, then addPyFile to executors
- dsdgen is downloaded as a binary (no make/gcc required); staged on every
  platform's blessed storage upfront

Args (positional):
    1. scale (int)
    2. output (URI string)
    3. label (string for log scraping)
    4. wheel_url
    5. dsdgen_url
    6. tpcds_idx_url
"""
import os
import subprocess
import sys
import time
import urllib.request


def _download(url, local):
    print(f"Downloading {url} -> {local}")
    sys.stdout.flush()
    if url.startswith(("http://", "https://")):
        urllib.request.urlretrieve(url, local)
        return local
    # Cloud storage URI -- read with Spark's binaryFiles
    raise RuntimeError(
        f"Non-http URL given to _download(): {url}. "
        "For abfss/dbfs URLs use _download_via_spark()."
    )


def _download_via_spark(spark, url, local):
    """Read a single file from cloud storage via Spark."""
    print(f"Spark-downloading {url} -> {local}")
    sys.stdout.flush()
    rdd = spark.sparkContext.binaryFiles(url)
    data = rdd.collect()[0][1]
    with open(local, "wb") as f:
        f.write(data)
    return local


def _resolve_and_download(spark, url, local):
    if url.startswith(("http://", "https://")):
        return _download(url, local)
    return _download_via_spark(spark, url, local)


def main():
    from pyspark.sql import SparkSession

    scale = int(sys.argv[1])
    output = sys.argv[2]
    label = sys.argv[3]
    wheel_url = sys.argv[4]
    dsdgen_url = sys.argv[5]
    tpcds_idx_url = sys.argv[6]

    print(f"=== LIVE-TEST {label} START ===")
    print(f"scale={scale} output={output}")
    print(f"wheel={wheel_url}")
    print(f"dsdgen={dsdgen_url}")
    print(f"tpcds.idx={tpcds_idx_url}")
    sys.stdout.flush()

    spark = (
        SparkSession.builder
        .appName(f"tpcds-live-{label}-sf{scale}")
        .getOrCreate()
    )

    # Stage all artifacts into a clean local dir on the driver.
    scratch = "/tmp/tpcds-live"
    os.makedirs(scratch, exist_ok=True)

    wheel_local = os.path.join(scratch, os.path.basename(wheel_url))
    dsdgen_local = os.path.join(scratch, "dsdgen")
    idx_local = os.path.join(scratch, "tpcds.idx")

    _resolve_and_download(spark, wheel_url, wheel_local)
    _resolve_and_download(spark, dsdgen_url, dsdgen_local)
    _resolve_and_download(spark, tpcds_idx_url, idx_local)
    os.chmod(dsdgen_local, 0o755)

    print(f"wheel size = {os.path.getsize(wheel_local)} B")
    print(f"dsdgen size = {os.path.getsize(dsdgen_local)} B; mode = "
          f"{oct(os.stat(dsdgen_local).st_mode)[-3:]}")
    print(f"tpcds.idx size = {os.path.getsize(idx_local)} B")
    sys.stdout.flush()

    # Install the wheel on the driver.
    # Some platforms (HDI) launch the driver as a user with no writable HOME,
    # which breaks pip's user-site install. Force --target into a writable dir
    # and prepend that to sys.path so `import tpcds_fast_datagen` succeeds.
    target_dir = os.path.join(scratch, "site")
    # Always start clean — pip --target refuses to overwrite, and a stale
    # cached install will mask the freshly built wheel.
    import shutil as _shutil
    if os.path.exists(target_dir):
        _shutil.rmtree(target_dir)
    os.makedirs(target_dir, exist_ok=True)
    pip_env = dict(os.environ)
    pip_env["HOME"] = scratch  # in case pip still wants to touch ~/.cache
    pip_cmd = [sys.executable, "-m", "pip", "install", "--quiet",
               "--target", target_dir, "--no-deps", wheel_local]
    # Optional: also install a specific pyarrow build (HDI ships a broken
    # one). Set TPCDS_LIVE_PIN_PYARROW=1 to enable.
    if os.environ.get("TPCDS_LIVE_PIN_PYARROW") == "1":
        pip_cmd[-2:-2] = ["pyarrow>=11.0,<12.0"]
    subprocess.run(pip_cmd, check=True, env=pip_env)
    # Make sure our target_dir wins over the cluster's site-packages so
    # `import pyarrow` (when pinned) resolves to our pinned copy.
    sys.path.insert(0, target_dir)

    # Ship the wheel to executors. They must already have a working pyarrow
    # (any version >=11). Bundling pyarrow inside an addPyFile zip does NOT
    # work because zipimport cannot load .so C extensions.
    spark.sparkContext.addPyFile(wheel_local)

    # Distribute dsdgen + tpcds.idx to executors.
    spark.sparkContext.addFile(dsdgen_local)
    spark.sparkContext.addFile(idx_local)
    print("dsdgen + tpcds.idx added to SparkFiles")
    sys.stdout.flush()

    from tpcds_fast_datagen.spark import generate
    from tpcds_fast_datagen import __version__ as pkg_ver
    print(f"tpcds_fast_datagen version: {pkg_ver}")
    sys.stdout.flush()

    t0 = time.time()
    result = generate(spark, scale=scale, output=output)
    elapsed = time.time() - t0

    print(f"=== LIVE-TEST {label} RESULT ===")
    print(f"label={label}")
    print(f"scale={scale}")
    print(f"output={output}")
    print(f"elapsed_s={elapsed:.1f}")
    print(f"num_tasks={result.num_tasks}")
    print(f"num_chunks={result.num_chunks}")
    print(f"total_rows={result.total_rows}")
    print(f"succeeded={result.succeeded}")
    for tname in sorted(result.table_rows):
        print(f"  rows.{tname}={result.table_rows[tname]}")
    print(f"=== LIVE-TEST {label} END ===")

    spark.stop()
    sys.exit(0 if result.succeeded else 1)


if __name__ == "__main__":
    main()

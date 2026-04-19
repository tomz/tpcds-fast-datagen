"""Live integration test bootstrap (self-installing variant).

Same as live_integration_test.py but pip-installs the wheel from the URL
passed as argv[4] (instead of relying on --py-files). Useful for Fabric Livy
where --py-files of arbitrary .whl is rejected.

Args:
    scale          int
    output         str
    label          str
    wheel_url      str  (abfss:// or http(s)://)
"""
import os
import subprocess
import sys
import time
import urllib.request


TPCDS_KIT_TARBALL = (
    "https://github.com/databricks/tpcds-kit/archive/refs/heads/master.tar.gz"
)


def _build_dsdgen_on_driver(scratch="/tmp/tpcds-kit-build"):
    binary = os.path.join(scratch, "tools", "dsdgen")
    if os.path.exists(binary) and os.access(binary, os.X_OK):
        return binary
    os.makedirs(scratch, exist_ok=True)
    tarball = os.path.join(scratch, "tpcds-kit.tar.gz")
    if not os.path.exists(tarball):
        urllib.request.urlretrieve(TPCDS_KIT_TARBALL, tarball)
    subprocess.run(
        ["tar", "xzf", tarball, "--strip-components=1", "-C", scratch],
        check=True,
    )
    subprocess.run(["make", "OS=LINUX"], check=True,
                   cwd=os.path.join(scratch, "tools"))
    return binary


def _install_wheel_everywhere(spark, wheel_url):
    """Download wheel to driver, pip-install on driver, distribute via addPyFile."""
    local = "/tmp/tpcds_fast_datagen.whl"
    if wheel_url.startswith("http"):
        urllib.request.urlretrieve(wheel_url, local)
    else:
        # ABFSS / DBFS / OneLake — read with Spark, write to local
        rdd = spark.sparkContext.binaryFiles(wheel_url)
        data = rdd.collect()[0][1]
        with open(local, "wb") as f:
            f.write(data)
    print(f"wheel downloaded to {local}, size={os.path.getsize(local)}")
    sys.stdout.flush()
    # Install on driver
    subprocess.run([sys.executable, "-m", "pip", "install", "--quiet",
                    "--no-deps", local], check=True)
    # Distribute to executors
    spark.sparkContext.addPyFile(local)


def main():
    from pyspark.sql import SparkSession

    scale = int(sys.argv[1])
    output = sys.argv[2]
    label = sys.argv[3] if len(sys.argv) > 3 else "live-test"
    wheel_url = sys.argv[4] if len(sys.argv) > 4 else None

    print(f"=== LIVE-TEST {label} START ===")
    print(f"scale={scale} output={output} wheel_url={wheel_url}")
    sys.stdout.flush()

    spark = (
        SparkSession.builder
        .appName(f"tpcds-live-{label}-sf{scale}")
        .getOrCreate()
    )

    if wheel_url:
        _install_wheel_everywhere(spark, wheel_url)

    t0 = time.time()
    dsdgen = _build_dsdgen_on_driver()
    tpcds_idx = os.path.join(os.path.dirname(dsdgen), "tpcds.idx")
    print(f"dsdgen built in {time.time()-t0:.1f}s at {dsdgen}")
    sys.stdout.flush()

    spark.sparkContext.addFile(dsdgen)
    spark.sparkContext.addFile(tpcds_idx)

    from tpcds_fast_datagen.spark import generate
    from tpcds_fast_datagen import __version__ as pkg_ver
    print(f"tpcds_fast_datagen version: {pkg_ver}")
    sys.stdout.flush()

    t1 = time.time()
    result = generate(spark, scale=scale, output=output)
    elapsed = time.time() - t1

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

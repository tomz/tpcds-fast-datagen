"""Live integration test bootstrap.

Submitted to HDI Livy / Fabric Livy / Databricks via *-spark-submit. The wheel
is shipped via --py-files; this script imports `tpcds_fast_datagen.spark.generate`
and runs it. dsdgen is built once on the driver (each executor has gcc/make in
the relevant images) and distributed via SparkFiles.

Args (all required, in order):
    scale          int  -- TPC-DS scale factor
    output         str  -- abfss://, dbfs:/, etc.
    label          str  -- tag to embed in stdout for log parsing

Usage::

    *-spark-submit \
        --py-files abfss://.../tpcds_fast_datagen-0.3.0.dev0-py3-none-any.whl \
        abfss://.../live_integration_test.py \
        1 abfss://lake/tpcds_test/sf1 hdi-livy-sf1
"""
import os
import subprocess
import sys
import time
import urllib.request

from pyspark.sql import SparkSession


TPCDS_KIT_TARBALL = (
    "https://github.com/databricks/tpcds-kit/archive/refs/heads/master.tar.gz"
)


def _build_dsdgen_on_driver(scratch="/tmp/tpcds-kit-build"):
    """Download tpcds-kit, run make, return absolute path to dsdgen.

    Cached: re-uses an existing build if present.
    """
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

    tools_dir = os.path.join(scratch, "tools")
    subprocess.run(
        ["make", "OS=LINUX"],
        check=True, cwd=tools_dir,
    )

    if not os.path.exists(binary):
        raise RuntimeError("dsdgen build did not produce a binary at " + binary)
    return binary


def main():
    scale = int(sys.argv[1])
    output = sys.argv[2]
    label = sys.argv[3] if len(sys.argv) > 3 else "live-test"

    print(f"=== LIVE-TEST {label} START ===")
    print(f"scale={scale} output={output}")
    sys.stdout.flush()

    t0 = time.time()
    dsdgen = _build_dsdgen_on_driver()
    tpcds_idx = os.path.join(os.path.dirname(dsdgen), "tpcds.idx")
    print(f"dsdgen built in {time.time()-t0:.1f}s at {dsdgen}")
    print(f"tpcds.idx at {tpcds_idx}, exists={os.path.exists(tpcds_idx)}")
    sys.stdout.flush()

    spark = (
        SparkSession.builder
        .appName(f"tpcds-live-{label}-sf{scale}")
        .getOrCreate()
    )

    # Distribute the binary + index to every executor.
    spark.sparkContext.addFile(dsdgen)
    spark.sparkContext.addFile(tpcds_idx)
    print("dsdgen + tpcds.idx added to SparkFiles")
    sys.stdout.flush()

    from tpcds_fast_datagen.spark import generate
    from tpcds_fast_datagen import __version__ as pkg_ver
    print(f"tpcds_fast_datagen version: {pkg_ver}")
    sys.stdout.flush()

    t1 = time.time()
    result = generate(
        spark,
        scale=scale,
        output=output,
        # No need for chunks override -- autosize from cluster conf
    )
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
    print(f"errors={len(result.errors)}")
    for tname in sorted(result.table_rows):
        print(f"  rows.{tname}={result.table_rows[tname]}")
    print(f"=== LIVE-TEST {label} END ===")

    spark.stop()
    sys.exit(0 if result.succeeded else 1)


if __name__ == "__main__":
    main()

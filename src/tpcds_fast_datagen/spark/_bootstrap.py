"""Bootstrap script shipped to ``spark-submit``.

This is the *only* file ``spark-submit`` needs to point at; it builds a
``SparkSession``, then immediately delegates to the package API. Both the
driver and the executors must have ``tpcds_fast_datagen`` importable
(via ``%pip install`` in notebooks, ``--archives conda_env.tar.gz`` for
spark-submit, or ``--py-files tpcds_fast_datagen-*.whl``).

Invoked by :mod:`tpcds_fast_datagen.spark.submit`; not intended to be run
directly by hand. Args are passed as a single JSON string in ``sys.argv[1]``.
"""
import json
import sys

from pyspark.sql import SparkSession  # noqa: E402

from tpcds_fast_datagen.spark import generate  # noqa: E402


def main():
    payload = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
    scale = int(payload["scale"])
    output = payload["output"]

    spark = (
        SparkSession.builder
        .appName(f"TPC-DS Datagen SF={scale}")
        .getOrCreate()
    )

    result = generate(
        spark,
        scale=scale,
        output=output,
        chunks=payload.get("chunks"),
        compression=payload.get("compression", "snappy"),
        dsdgen_path=payload.get("dsdgen_path"),
    )

    spark.stop()
    sys.exit(0 if result.succeeded else 1)


if __name__ == "__main__":
    main()

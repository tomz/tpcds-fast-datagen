#!/usr/bin/env python3
"""Compatibility shim — historical entry point for ``spark-submit``.

The implementation moved into the ``tpcds_fast_datagen.spark`` package. This
file remains so existing scripts that hard-code its path keep working::

    spark-submit spark_tpcds_gen.py --scale 1000 --output abfs:///tpcds/sf1000

Modern usage is one of:

* In a notebook (Fabric, Databricks, Jupyter)::

      %pip install tpcds-fast-datagen
      from tpcds_fast_datagen.spark import generate
      generate(spark, scale=1000, output="abfs:///tpcds/sf1000")

* In a Livy session statement: same as the notebook snippet above.

* From the CLI::

      tpcds-gen --engine spark --scale 1000 --output abfs:///tpcds/sf1000 \\
          -- --master yarn --num-executors 10 --executor-cores 16

* Via ``spark-submit`` directly with the bundled bootstrap (preferred for
  Livy ``POST /batches`` and other automation)::

      tpcds-gen-spark-submit --scale 1000 --output abfs:///tpcds/sf1000 \\
          --print  # show the spark-submit argv this would run

This shim parses the legacy argparse-style flags and forwards to
``tpcds_fast_datagen.spark.generate``. Both driver and executors must have
``tpcds_fast_datagen`` importable (use ``--py-files`` or ``--archives``).
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from tpcds_fast_datagen.spark import generate


def main() -> int:
    p = argparse.ArgumentParser(
        description="Distributed TPC-DS data generation via Spark + dsdgen"
    )
    p.add_argument("--scale", "-s", type=int, required=True)
    p.add_argument("--output", "-o", type=str, required=True)
    p.add_argument("--chunks", type=int, default=None,
                   help="Parallel shards per large table (auto-sized if unset)")
    p.add_argument(
        "--compression", type=str, default="snappy",
        choices=["snappy", "gzip", "zstd", "none"],
    )
    p.add_argument("--dsdgen-path", type=str, default=None)
    args = p.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"TPC-DS Datagen SF={args.scale}")
        .getOrCreate()
    )
    try:
        result = generate(
            spark,
            scale=args.scale,
            output=args.output,
            chunks=args.chunks,
            compression=args.compression,
            dsdgen_path=args.dsdgen_path,
        )
    finally:
        spark.stop()
    return 0 if result.succeeded else 1


if __name__ == "__main__":
    sys.exit(main())

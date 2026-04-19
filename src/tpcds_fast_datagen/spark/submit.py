"""``spark-submit`` shim for users who don't have a notebook.

Builds the right ``spark-submit`` argv for the bundled bootstrap script
(``spark_tpcds_gen.py`` at the repo root, which lives inside the wheel as
package data) and ``execvp``s it. Used by:

* the ``tpcds-gen-spark-submit`` console script,
* the main ``tpcds-gen --engine spark`` dispatch when ``pyspark`` is not
  importable in the current process.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from importlib import resources


def bootstrap_path() -> str:
    """Absolute path to the bootstrap script shipped with the wheel."""
    # Located via importlib.resources so it works whether the package is
    # installed normally, in editable mode, or unpacked in a zip-app.
    ref = resources.files("tpcds_fast_datagen.spark") / "_bootstrap.py"
    # files() returns a MultiplexedPath in editable mode; `as_file` materializes
    # it to a real path so spark-submit can ship it.
    with resources.as_file(ref) as p:
        return str(p)


def build_argv(
    *,
    scale: int,
    output: str,
    chunks: int | None = None,
    compression: str = "snappy",
    dsdgen_path: str | None = None,
    spark_submit: str = "spark-submit",
    extra_spark_opts: list[str] | None = None,
) -> list[str]:
    """Construct the full ``spark-submit ... bootstrap.py <args>`` argv.

    Caller appends their own ``--master``, ``--num-executors`` etc. via
    ``extra_spark_opts``.
    """
    boot = bootstrap_path()
    argv = [spark_submit]
    if extra_spark_opts:
        argv.extend(extra_spark_opts)
    args_payload = {
        "scale": scale,
        "output": output,
        "chunks": chunks,
        "compression": compression,
        "dsdgen_path": dsdgen_path,
    }
    argv.extend([boot, json.dumps(args_payload)])
    return argv


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="tpcds-gen-spark-submit",
        description=(
            "Submit a TPC-DS datagen job via spark-submit. Pass extra "
            "spark-submit options after `--` (e.g. `-- --master yarn "
            "--num-executors 10`)."
        ),
    )
    p.add_argument("--scale", "-s", type=int, required=True)
    p.add_argument("--output", "-o", type=str, required=True)
    p.add_argument("--chunks", type=int, default=None)
    p.add_argument(
        "--compression", type=str, default="snappy",
        choices=["snappy", "gzip", "zstd", "none"],
    )
    p.add_argument("--dsdgen-path", type=str, default=None)
    p.add_argument("--spark-submit", type=str, default="spark-submit")
    p.add_argument("--print", action="store_true",
                   help="Print the spark-submit argv instead of executing it")
    args, extra = p.parse_known_args(argv)

    # Drop the bare `--` separator if argparse left it in `extra`.
    if extra and extra[0] == "--":
        extra = extra[1:]

    full_argv = build_argv(
        scale=args.scale,
        output=args.output,
        chunks=args.chunks,
        compression=args.compression,
        dsdgen_path=args.dsdgen_path,
        spark_submit=args.spark_submit,
        extra_spark_opts=extra,
    )

    if args.print:
        print(" ".join(full_argv))
        return 0

    os.execvp(full_argv[0], full_argv)


if __name__ == "__main__":
    sys.exit(main())

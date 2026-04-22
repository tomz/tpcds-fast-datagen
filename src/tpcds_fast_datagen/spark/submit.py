"""``spark-submit`` shim for users who don't have a notebook.

Builds the right ``spark-submit`` argv for the bundled bootstrap script
(``spark_tpcds_gen.py`` at the repo root, which lives inside the wheel as
package data) and ``execvp``s it. Used by:

* the ``tpcds-gen-spark-submit`` console script,
* the main ``tpcds-gen --engine spark`` dispatch when ``pyspark`` is not
  importable in the current process.

The ``--hdi`` and ``--via-livy`` flags absorb the manual setup that used to
require a 486-line runbook (see ``docs/hdinsight-quickstart.md``).
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


def hdi_spark_opts(env_uri: str, binary_uri: str, dists_uri: str | None = None) -> list[str]:
    """The fixed block of spark-submit flags HDInsight 5.1 needs.

    Returns ``--archives``, ``--files``, and the two ``PYSPARK_PYTHON``
    ``--conf`` lines. The env tarball must be a ``conda-pack`` (or
    ``venv-pack``) archive whose root contains ``bin/python`` (the standard
    layout produced by ``tpcds-gen package-env``).

    Why each flag (see ``docs/hdinsight-quickstart.md`` for the long story):

    * ``--archives env#env`` ships a self-contained Python that doesn't depend
      on HDI's ancient ``GLIBCXX_3.4.25`` (modern pyarrow needs ``3.4.26+``).
    * ``--files dsdgen[,tpcds.idx]`` distributes the binary to every executor's
      ``SparkFiles`` dir; ``_resolve_dsdgen_on_executor`` picks it up.
    * ``spark.{yarn.appMasterEnv,executorEnv}.PYSPARK_PYTHON=./env/bin/python``
      tells YARN to launch Python workers from inside the unpacked env.
    """
    files = binary_uri
    if dists_uri:
        files = f"{binary_uri},{dists_uri}"
    return [
        "--archives", f"{env_uri}#env",
        "--files", files,
        "--conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/bin/python",
        "--conf", "spark.executorEnv.PYSPARK_PYTHON=./env/bin/python",
    ]


def build_argv(
    *,
    scale: int,
    output: str,
    chunks: int | None = None,
    compression: str = "snappy",
    dsdgen_path: str | None = None,
    spark_submit: str = "spark-submit",
    extra_spark_opts: list[str] | None = None,
    hdi: bool = False,
    env_uri: str | None = None,
    binary_uri: str | None = None,
    dists_uri: str | None = None,
) -> list[str]:
    """Construct the full ``spark-submit ... bootstrap.py <args>`` argv.

    Caller appends their own ``--master``, ``--num-executors`` etc. via
    ``extra_spark_opts``.

    With ``hdi=True``, the HDI flag block is spliced in *before* the user's
    ``extra_spark_opts`` so the user can still override (e.g.) ``--conf
    spark.executor.memory``.
    """
    boot = bootstrap_path()
    argv = [spark_submit]
    if hdi:
        if not (env_uri and binary_uri):
            raise ValueError("--hdi requires both --env and --binary URIs")
        argv.extend(hdi_spark_opts(env_uri, binary_uri, dists_uri))
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

    hdi_grp = p.add_argument_group(
        "HDInsight",
        "Flags that activate the 'just-works' HDI 5.1 profile. "
        "See docs/hdinsight-quickstart.md.",
    )
    hdi_grp.add_argument(
        "--hdi", action="store_true",
        help="Inject HDI 5.1 spark-submit flags (--archives env, --files dsdgen, "
             "PYSPARK_PYTHON conf). Requires --env and --binary.",
    )
    hdi_grp.add_argument(
        "--env", dest="env_uri", default=None,
        help="URI of the conda-pack env tarball (built with `tpcds-gen "
             "package-env`); typically wasbs://.../env.tar.gz",
    )
    hdi_grp.add_argument(
        "--binary", dest="binary_uri", default=None,
        help="URI of the dsdgen binary on cluster-visible storage; typically "
             "wasbs://.../dsdgen",
    )
    hdi_grp.add_argument(
        "--dists", dest="dists_uri", default=None,
        help="Optional URI of tpcds.idx alongside the dsdgen binary",
    )
    hdi_grp.add_argument(
        "--via-livy", dest="via_livy", default=None,
        help="Submit via Livy REST instead of spark-submit. Pass the "
             "cluster URL, e.g. https://<name>.azurehdinsight.net. Reads "
             "credentials from $HDI_ADMIN_USER and $HDI_ADMIN_PASSWORD.",
    )
    hdi_grp.add_argument(
        "--admin-user", default=os.environ.get("HDI_ADMIN_USER", "admin"),
    )
    hdi_grp.add_argument(
        "--admin-password-env", default="HDI_ADMIN_PASSWORD",
        help="Env var holding the HDI admin password (default HDI_ADMIN_PASSWORD)",
    )
    hdi_grp.add_argument(
        "--no-poll", action="store_true",
        help="With --via-livy: print the batch ID and exit instead of polling.",
    )

    args, extra = p.parse_known_args(argv)

    # Drop the bare `--` separator if argparse left it in `extra`.
    if extra and extra[0] == "--":
        extra = extra[1:]

    if args.via_livy:
        from ..hdi import livy_submit
        if not (args.hdi or (args.env_uri and args.binary_uri)):
            print("--via-livy currently requires --hdi (with --env/--binary)",
                  file=sys.stderr)
            return 2
        pw = os.environ.get(args.admin_password_env)
        if not pw:
            print(f"${args.admin_password_env} not set", file=sys.stderr)
            return 2
        return livy_submit(
            cluster_url=args.via_livy,
            scale=args.scale,
            output=args.output,
            chunks=args.chunks,
            compression=args.compression,
            env_uri=args.env_uri,
            binary_uri=args.binary_uri,
            dists_uri=args.dists_uri,
            admin_user=args.admin_user,
            admin_password=pw,
            poll=not args.no_poll,
            extra_conf=extra,
            print_only=args.print,
        )

    full_argv = build_argv(
        scale=args.scale,
        output=args.output,
        chunks=args.chunks,
        compression=args.compression,
        dsdgen_path=args.dsdgen_path,
        spark_submit=args.spark_submit,
        extra_spark_opts=extra,
        hdi=args.hdi,
        env_uri=args.env_uri,
        binary_uri=args.binary_uri,
        dists_uri=args.dists_uri,
    )

    if args.print:
        print(" ".join(full_argv))
        return 0

    os.execvp(full_argv[0], full_argv)


if __name__ == "__main__":
    sys.exit(main())

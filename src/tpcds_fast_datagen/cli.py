"""CLI entry point for tpcds-gen.

Historically ``tpcds-gen`` was a single flat command (``tpcds-gen --scale 1 …``).
We've grown subcommands (``doctor``, ``install-dsdgen``) without breaking that:
when the first non-option argument isn't a known subcommand, we implicitly
dispatch to ``generate``. Both of these still work:

    tpcds-gen --scale 1 --output /tmp/sf1
    tpcds-gen generate --scale 1 --output /tmp/sf1
"""

from __future__ import annotations

import os
import sys

import click


class _DefaultGroup(click.Group):
    """A click Group that falls through to a default subcommand.

    Any invocation where the first token isn't a known subcommand name gets
    routed to ``self.default_cmd_name`` instead, with all args forwarded. This
    preserves the pre-0.3.2 flat CLI: ``tpcds-gen --scale 1 --output /tmp/sf1``
    still runs ``generate``.
    """

    default_cmd_name = "generate"

    def parse_args(self, ctx, args):
        # If nothing to dispatch, let click show help as usual.
        if not args:
            return super().parse_args(ctx, args)
        # Peek at the first positional token. Options start with '-' and
        # belong to either the group or the default command — in either case
        # we haven't hit a subcommand name yet, so fall through.
        first_positional = next((a for a in args if not a.startswith("-")), None)
        known = set(self.commands)
        if first_positional is None or first_positional not in known:
            args = [self.default_cmd_name, *args]
        return super().parse_args(ctx, args)

from .config import GenConfig
from .generator import generate as _generate


# ---------------------------------------------------------------------------
# Shared option decorators
# ---------------------------------------------------------------------------

def _generate_options(f):
    """Apply all generate-related click options to a function."""
    opts = [
        click.option("--scale", "-s", type=int, default=1, help="TPC-DS scale factor (1, 10, 100, 1000, ...)"),
        click.option("--parallel", "-p", type=int, default=0, help="Number of parallel workers (0 = auto, uses all CPU cores)"),
        click.option("--output", "-o", type=str, default="./tpcds_data", help="Output directory for Parquet files"),
        click.option("--row-group-size-mb", type=int, default=128, help="Target row group size in MB"),
        click.option("--tables", "-t", type=str, default=None, help="Comma-separated list of tables to generate (default: all)"),
        click.option("--dsdgen-path", type=str, default=None, help="Path to dsdgen binary"),
        click.option("--overwrite", is_flag=True, help="Overwrite existing output directory"),
        click.option("--compression", type=click.Choice(["snappy", "gzip", "zstd", "none"]), default="snappy", help="Parquet compression codec"),
        click.option("--engine", type=click.Choice(["auto", "duckdb", "dsdgen", "spark"]), default="auto",
                     help="Generation engine. 'auto' (default) picks duckdb for SF<=50 and "
                          "dsdgen-multiprocess for SF>50. 'spark' delegates to spark-submit "
                          "(requires pyspark installed and a cluster)."),
        click.option("--auto-threshold", type=int, default=50, show_default=True,
                     help="Scale factor cutoff for --engine auto: SF<=N uses duckdb, SF>N uses dsdgen."),
        click.option("--chunks", type=int, default=None, help="(spark engine) parallel shards per large table; auto-sized if unset"),
        click.argument("spark_submit_args", nargs=-1, type=click.UNPROCESSED),
    ]
    for opt in reversed(opts):
        f = opt(f)
    return f


# ---------------------------------------------------------------------------
# Core generate logic (shared between default invocation and `generate` subcmd)
# ---------------------------------------------------------------------------

def _do_generate(
    scale: int,
    parallel: int,
    output: str,
    row_group_size_mb: int,
    tables,
    dsdgen_path,
    overwrite: bool,
    compression: str,
    engine: str,
    auto_threshold: int,
    chunks,
    spark_submit_args,
):
    if engine == "spark":
        from .spark.submit import build_argv
        argv = build_argv(
            scale=scale,
            output=output,
            chunks=chunks,
            compression=compression,
            dsdgen_path=dsdgen_path,
            extra_spark_opts=list(spark_submit_args) or None,
        )
        os.execvp(argv[0], argv)

    if spark_submit_args:
        click.echo(
            f"Warning: ignoring extra args {spark_submit_args!r} "
            "(only --engine spark forwards them to spark-submit).",
            err=True,
        )

    table_list = [t.strip() for t in tables.split(",")] if tables else None

    config = GenConfig(
        scale_factor=scale,
        parallel=parallel,
        output_dir=output,
        row_group_size_mb=row_group_size_mb,
        tables=table_list,
        dsdgen_path=dsdgen_path,
        overwrite=overwrite,
        compression=compression,
        engine=engine,
        auto_threshold=auto_threshold,
    )

    _generate(config)


# ---------------------------------------------------------------------------
# Group + default-invocation shim
# ---------------------------------------------------------------------------

@click.group(
    cls=_DefaultGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
)
def main():
    """TPC-DS Fast Datagen — the fast TPC-DS data generator.

    Three-tier design:
      * SF <= 50  → DuckDB engine (fast, in-process dsdgen, single COPY per table)
      * SF >  50  → dsdgen multiprocess engine (constant memory per worker, disk-bound)
      * SF >= 1000 (distributed) → --engine spark on a Spark/YARN cluster

    Subcommands:

    \b
      tpcds-gen generate         Generate TPC-DS data (DEFAULT if no subcommand)
      tpcds-gen doctor           Diagnose Python/pyarrow/duckdb/dsdgen setup
      tpcds-gen install-dsdgen   Download a prebuilt dsdgen for your platform

    Legacy flat invocation still works::

        tpcds-gen --scale 1 --output /tmp/sf1

    is equivalent to ``tpcds-gen generate --scale 1 --output /tmp/sf1``.
    """


@main.command("generate")
@_generate_options
def generate_cmd(**kwargs):
    """Generate TPC-DS data. (Default action if no subcommand given.)"""
    _do_generate(**kwargs)


# ---------------------------------------------------------------------------
# `tpcds-gen doctor`
# ---------------------------------------------------------------------------

@main.command("doctor")
def doctor_cmd():
    """Check the local environment for everything tpcds-gen needs to run.

    Exits with status 0 if all required components are present, 1 otherwise.
    Optional components (pyspark, tpcds.idx when only running duckdb) emit
    warnings but don't fail the check.
    """
    from . import __version__

    ok = True
    warn = False

    def _status(label: str, good: bool, detail: str = "", optional: bool = False):
        nonlocal ok, warn
        if good:
            mark = "[OK]  "
        elif optional:
            mark = "[WARN]"
            warn = True
        else:
            mark = "[FAIL]"
            ok = False
        click.echo(f"  {mark} {label:<28} {detail}")

    click.echo(f"tpcds-fast-datagen {__version__} — environment check")
    click.echo("")
    click.echo(f"Python: {sys.version.splitlines()[0]}")
    click.echo(f"Executable: {sys.executable}")
    click.echo("")

    # Required: Python 3.9+
    py_ok = sys.version_info >= (3, 9)
    _status("Python >= 3.9", py_ok, f"(got {sys.version_info.major}.{sys.version_info.minor})")

    # Required: pyarrow
    try:
        import pyarrow as pa  # noqa: F401
        _status("pyarrow", True, f"(version {pa.__version__})")
    except ImportError as e:
        _status("pyarrow", False, f"(not installed: {e})")

    # Required: duckdb + tpcds extension (only fully exercised by duckdb engine)
    try:
        import duckdb
        con = duckdb.connect()
        try:
            con.execute("INSTALL tpcds; LOAD tpcds;")
            _status("duckdb + tpcds ext", True, f"(version {duckdb.__version__})")
        except Exception as e:  # noqa: BLE001
            _status(
                "duckdb + tpcds ext",
                False,
                f"(duckdb {duckdb.__version__} installed, but tpcds extension failed: {e})",
                optional=True,
            )
        finally:
            con.close()
    except ImportError as e:
        _status("duckdb", False, f"(not installed: {e})")

    # Required (for dsdgen engine + spark): dsdgen + tpcds.idx
    from .binary import find_dsdgen, find_tpcds_idx, platform_tag

    click.echo(f"Platform tag: {platform_tag()}")
    try:
        dsdgen = find_dsdgen()
        _status("dsdgen binary", True, f"({dsdgen})")
        try:
            idx = find_tpcds_idx(dsdgen)
            _status("tpcds.idx", True, f"({idx})")
        except FileNotFoundError as e:
            _status("tpcds.idx", False, str(e))
    except FileNotFoundError as e:
        _status(
            "dsdgen binary",
            False,
            "(not found — run `tpcds-gen install-dsdgen` or set DSDGEN_PATH/DSDGEN_URL)",
            optional=True,
        )
        click.echo("")
        click.echo(str(e))

    # Optional: pyspark
    try:
        import pyspark  # noqa: F401
        _status("pyspark (optional)", True, f"(version {pyspark.__version__})", optional=True)
    except ImportError:
        _status(
            "pyspark (optional)",
            True,
            "(not installed; only needed for --engine spark)",
            optional=True,
        )

    click.echo("")
    if not ok:
        click.echo("doctor: FAIL — one or more required components missing.", err=True)
        sys.exit(1)
    if warn:
        click.echo("doctor: OK with warnings.")
    else:
        click.echo("doctor: all good.")


# ---------------------------------------------------------------------------
# `tpcds-gen install-dsdgen`
# ---------------------------------------------------------------------------

# Base URL for prebuilt binaries. Individual release tags live under this.
_PREBUILT_BASE = "https://github.com/tomz/tpcds-fast-datagen/releases/download"
_PREBUILT_DEFAULT_TAG = "v0.3.2"


@main.command("install-dsdgen")
@click.option("--url", "url", type=str, default=None,
              help="Explicit URL for the dsdgen binary (overrides --tag/--platform).")
@click.option("--idx-url", type=str, default=None,
              help="Explicit URL for tpcds.idx (defaults to <url>'s directory + /tpcds.idx).")
@click.option("--tag", type=str, default=_PREBUILT_DEFAULT_TAG, show_default=True,
              help="GitHub release tag to pull prebuilt binaries from.")
@click.option("--platform", "platform_override", type=str, default=None,
              help="Platform tag (e.g. linux-x86_64, linux-arm64, macos-x86_64, macos-arm64). "
                   "Defaults to auto-detected current platform.")
@click.option("--from-source", is_flag=True,
              help="Ignore prebuilts; git clone tpcds-kit and `make OS=LINUX` into the cache.")
def install_dsdgen_cmd(url, idx_url, tag, platform_override, from_source):
    """Install a ``dsdgen`` binary into the local cache (``~/.cache/tpcds-fast-datagen/``).

    By default we download a prebuilt binary for the current platform from the
    project's GitHub Releases. If your platform isn't covered (or you don't trust
    the prebuilt), pass ``--from-source`` to clone tpcds-kit and build it.
    """
    from .binary import cache_dir, install_dsdgen_from_url, platform_tag

    if from_source:
        _install_from_source(cache_dir())
        return

    if url is None:
        tag_ = tag
        plat = platform_override or platform_tag()
        url = f"{_PREBUILT_BASE}/{tag_}/dsdgen-{plat}"
        if idx_url is None:
            idx_url = f"{_PREBUILT_BASE}/{tag_}/tpcds.idx"

    click.echo(f"Installing dsdgen into {cache_dir()}")
    try:
        path = install_dsdgen_from_url(url, idx_url=idx_url)
    except Exception as e:  # noqa: BLE001
        click.echo(f"Download failed: {e}", err=True)
        click.echo("", err=True)
        click.echo(
            "No prebuilt may be available for your platform. Retry with --from-source "
            "to build from the official tpcds-kit sources:",
            err=True,
        )
        click.echo("    tpcds-gen install-dsdgen --from-source", err=True)
        sys.exit(1)
    click.echo(f"Installed: {path}")
    click.echo("`tpcds-gen` will now find this automatically (cached location is on the search path).")


def _install_from_source(dest_dir):
    """Clone tpcds-kit, build, copy binary + idx into ``dest_dir``."""
    import subprocess
    import tempfile
    from pathlib import Path

    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Pick make target: LINUX / MACOS
    system = sys.platform
    if system.startswith("linux"):
        make_os = "LINUX"
    elif system == "darwin":
        make_os = "MACOS"
    else:
        click.echo(f"Unsupported platform for source build: {system}", err=True)
        sys.exit(1)

    # Need git and make
    for tool in ("git", "make", "gcc"):
        if not _which(tool):
            click.echo(f"`{tool}` not found on PATH; required for --from-source.", err=True)
            sys.exit(1)

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        click.echo(f"Cloning databricks/tpcds-kit into {tmp} …")
        subprocess.check_call(
            ["git", "clone", "--depth=1", "https://github.com/databricks/tpcds-kit", str(tmp / "tpcds-kit")]
        )
        tools = tmp / "tpcds-kit" / "tools"
        click.echo(f"Building (make OS={make_os}) …")
        # GCC >= 14 promotes K&R-style implicit-int / implicit-function-declaration
        # from warnings to errors by default. tpcds-kit's date.c uses K&R prototypes,
        # so we inject compatibility flags via the Makefile's per-OS *_CFLAGS slot
        # (plain CFLAGS= would be overridden by the Makefile). On older gccs these
        # flags are a no-op.
        extra_cflags = (
            "-g -Wall "
            "-Wno-implicit-int -Wno-implicit-function-declaration "
            "-Wno-error=implicit-int -Wno-error=implicit-function-declaration"
        )
        subprocess.check_call(
            ["make", f"OS={make_os}", f"{make_os}_CFLAGS={extra_cflags}"],
            cwd=tools,
        )

        src_bin = tools / "dsdgen"
        src_idx = tools / "tpcds.idx"
        dst_bin = dest_dir / "dsdgen"
        dst_idx = dest_dir / "tpcds.idx"

        import shutil as _sh
        _sh.copyfile(src_bin, dst_bin)
        _sh.copyfile(src_idx, dst_idx)
        import stat as _stat
        dst_bin.chmod(dst_bin.stat().st_mode | _stat.S_IXUSR | _stat.S_IXGRP | _stat.S_IXOTH)

    click.echo(f"Installed: {dst_bin}")
    click.echo(f"Installed: {dst_idx}")


def _which(name: str):
    import shutil as _sh
    return _sh.which(name)


if __name__ == "__main__":
    main()

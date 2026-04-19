"""Orchestrator — plans and executes parallel TPC-DS data generation."""

import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table as RichTable

from .binary import validate_dsdgen
from .config import GenConfig
from .schema import (
    ALL_TABLES, FACT_TABLES, COMPANION_PAIRS, COMPANION_PRIMARY, TableDef,
)
from .worker import generate_chunk
from .worker_duckdb import generate_all_duckdb


console = Console()


def _plan_tasks(config: GenConfig) -> list[dict]:
    """Plan the generation tasks.

    Returns a list of task dicts, each describing one dsdgen invocation.
    For small tables (<1M rows), only child 1 generates them.
    For large tables, each child generates a chunk.
    Companion tables (e.g., store_sales + store_returns) are generated together.
    """
    tasks = []
    tables_to_generate = config.tables or list(ALL_TABLES.keys())

    # Track which tables we've already planned (for companion dedup)
    planned = set()

    for table_name in tables_to_generate:
        if table_name in planned:
            continue

        table_def = ALL_TABLES[table_name]
        row_count = table_def.row_count(config.scale_factor)

        # For companion pairs, plan the primary table only
        primary = COMPANION_PRIMARY.get(table_name, table_name)
        companion = COMPANION_PAIRS.get(primary)

        # Mark both as planned
        planned.add(primary)
        if companion:
            planned.add(companion)

        # Small tables (<1M rows at this SF) — only generate once (child 1)
        # This matches dsdgen's split_work() behavior
        if row_count < 1_000_000:
            tasks.append({
                "table": primary,
                "child": 1,
                "parallel": 1,  # no parallel for small tables
            })
        else:
            # dsdgen's split_work() skips children where chunk < 1M rows
            # So max useful parallelism = row_count / 1_000_000
            max_parallel = max(1, row_count // 1_000_000)
            effective_parallel = min(config.parallel, max_parallel)

            for child in range(1, effective_parallel + 1):
                tasks.append({
                    "table": primary,
                    "child": child,
                    "parallel": effective_parallel,
                })

    return tasks


def _run_task(args: tuple) -> dict:
    """Wrapper for ProcessPoolExecutor — unpacks args and calls generate_chunk."""
    dsdgen_path, table, sf, parallel, child, output_dir, rg_mb, compression = args
    result = generate_chunk(
        dsdgen_path=dsdgen_path,
        table_name=table,
        scale_factor=sf,
        parallel=parallel,
        child=child,
        output_dir=output_dir,
        row_group_size_mb=rg_mb,
        compression=compression,
    )
    return {"table": table, "child": child, "files": result}


def generate(config: GenConfig) -> dict:
    """Run the full TPC-DS data generation.

    Returns a summary dict with table names, file counts, row counts, and timing.
    """
    start_time = time.time()

    # Prepare output directory
    if os.path.exists(config.output_dir) and not config.overwrite:
        raise FileExistsError(
            f"Output directory {config.output_dir} already exists. Use --overwrite to replace."
        )
    os.makedirs(config.output_dir, exist_ok=True)

    if config.engine == "duckdb":
        return _generate_duckdb(config, start_time)
    else:
        return _generate_dsdgen(config, start_time)


def _generate_duckdb(config: GenConfig, start_time: float) -> dict:
    """Generate using DuckDB engine — single dsdgen() call, sequential COPY export."""
    try:
        import duckdb
    except ImportError:
        raise ImportError(
            "DuckDB is required for --engine duckdb. "
            "Install it: pip install duckdb>=0.10"
        )

    console.print(f"[green]Using engine:[/green] DuckDB {duckdb.__version__}")

    # Determine tables to export
    tables_to_export = config.tables or None
    # Estimate task count for progress bar (24 tables if all)
    est_tables = len(config.tables) if config.tables else 24

    console.print(
        f"[bold]Generating TPC-DS SF={config.scale_factor}[/bold] "
        f"(DuckDB engine — single dsdgen + {est_tables} table exports)"
    )

    results_files = {}
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        pbar = progress.add_task("Generating...", total=est_tables)

        def on_table_done(table_name):
            progress.advance(pbar)

        results_files = generate_all_duckdb(
            scale_factor=config.scale_factor,
            output_dir=config.output_dir,
            tables=tables_to_export,
            row_group_size_mb=config.row_group_size_mb,
            compression=config.compression,
            progress_callback=on_table_done,
        )

    elapsed = time.time() - start_time

    # Build results in the format _summarize expects
    results = [{"files": results_files}]
    summary = _summarize(results, config, elapsed)
    _print_summary(summary)
    return summary


def _generate_dsdgen(config: GenConfig, start_time: float) -> dict:
    """Generate using legacy dsdgen binary engine."""
    # Validate dsdgen
    dsdgen_path, _ = validate_dsdgen(config.dsdgen_path)
    console.print(f"[green]Using engine:[/green] dsdgen ({dsdgen_path})")

    # Plan tasks
    tasks = _plan_tasks(config)
    console.print(
        f"[bold]Generating TPC-DS SF={config.scale_factor}[/bold] "
        f"with {config.parallel} workers, {len(tasks)} tasks (dsdgen engine)"
    )

    # Build executor args
    executor_args = [
        (
            dsdgen_path,
            task["table"],
            config.scale_factor,
            task["parallel"],
            task["child"],
            config.output_dir,
            config.row_group_size_mb,
            config.compression,
        )
        for task in tasks
    ]

    # Execute in parallel
    results = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        pbar = progress.add_task("Generating...", total=len(executor_args))

        with ProcessPoolExecutor(max_workers=config.parallel) as pool:
            futures = {
                pool.submit(_run_task, args): args
                for args in executor_args
            }
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                progress.advance(pbar)

    elapsed = time.time() - start_time

    # Summarize
    summary = _summarize(results, config, elapsed)
    _print_summary(summary)
    return summary


def _summarize(results: list[dict], config: GenConfig, elapsed: float) -> dict:
    """Build a summary of the generation run."""
    table_files: dict[str, list[str]] = {}
    for r in results:
        for tname, fpath in r["files"].items():
            table_files.setdefault(tname, []).append(fpath)

    total_size = 0
    table_info = {}
    for tname, files in sorted(table_files.items()):
        size = sum(os.path.getsize(f) for f in files)
        total_size += size
        table_info[tname] = {
            "files": len(files),
            "size_mb": size / (1024 * 1024),
        }

    return {
        "scale_factor": config.scale_factor,
        "parallel": config.parallel,
        "tables": table_info,
        "total_tables": len(table_info),
        "total_size_mb": total_size / (1024 * 1024),
        "elapsed_seconds": elapsed,
    }


def _print_summary(summary: dict):
    """Print a rich summary table."""
    table = RichTable(title=f"TPC-DS SF={summary['scale_factor']} Generation Complete")
    table.add_column("Table", style="cyan")
    table.add_column("Files", justify="right")
    table.add_column("Size (MB)", justify="right")

    for tname, info in sorted(summary["tables"].items()):
        table.add_row(tname, str(info["files"]), f"{info['size_mb']:.1f}")

    table.add_row(
        "[bold]TOTAL[/bold]",
        str(sum(i["files"] for i in summary["tables"].values())),
        f"[bold]{summary['total_size_mb']:.1f}[/bold]",
    )

    console.print(table)
    console.print(
        f"\n[green]Done![/green] {summary['total_tables']} tables, "
        f"{summary['total_size_mb']:.1f} MB, "
        f"{summary['elapsed_seconds']:.1f}s "
        f"({summary['total_size_mb'] / max(0.01, summary['elapsed_seconds']):.1f} MB/s)"
    )

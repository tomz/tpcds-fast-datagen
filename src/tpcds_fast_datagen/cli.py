"""CLI entry point for tpcds-gen."""

import click

from .config import GenConfig
from .generator import generate


@click.command()
@click.option("--scale", "-s", type=int, default=1, help="TPC-DS scale factor (1, 10, 100, 1000, ...)")
@click.option("--parallel", "-p", type=int, default=0, help="Number of parallel workers (0 = auto, uses all CPU cores)")
@click.option("--output", "-o", type=str, default="./tpcds_data", help="Output directory for Parquet files")
@click.option("--row-group-size-mb", type=int, default=128, help="Target row group size in MB")
@click.option("--tables", "-t", type=str, default=None, help="Comma-separated list of tables to generate (default: all)")
@click.option("--dsdgen-path", type=str, default=None, help="Path to dsdgen binary")
@click.option("--overwrite", is_flag=True, help="Overwrite existing output directory")
@click.option("--compression", type=click.Choice(["snappy", "gzip", "zstd", "none"]), default="snappy", help="Parquet compression codec")
@click.option("--engine", type=click.Choice(["auto", "duckdb", "dsdgen"]), default="auto",
              help="Generation engine. 'auto' (default) picks duckdb for SF<=50 and "
                   "dsdgen-multiprocess for SF>50 to avoid DuckDB OOM at large scale.")
@click.option("--auto-threshold", type=int, default=50, show_default=True,
              help="Scale factor cutoff for --engine auto: SF<=N uses duckdb, SF>N uses dsdgen.")
def main(
    scale: int,
    parallel: int,
    output: str,
    row_group_size_mb: int,
    tables: str | None,
    dsdgen_path: str | None,
    overwrite: bool,
    compression: str,
    engine: str,
    auto_threshold: int,
):
    """TPC-DS Fast Datagen — the fastest TPC-DS data generator.

    Three-tier design:
      * SF <= 50  → DuckDB engine (fast, in-process dsdgen, single COPY per table)
      * SF >  50  → dsdgen multiprocess engine (constant memory per worker, disk-bound)
      * SF >= 1000 (distributed) → use spark_tpcds_gen.py on a Spark/YARN cluster

    --engine auto picks 1 vs 2 automatically based on --scale.
    """
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

    generate(config)


if __name__ == "__main__":
    main()

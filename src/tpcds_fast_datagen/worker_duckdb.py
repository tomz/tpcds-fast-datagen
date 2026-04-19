"""Worker — DuckDB-based TPC-DS data generation.

Uses DuckDB's built-in dsdgen() to generate data entirely in C++,
then COPY TO exports each table as Parquet. No intermediate text files,
no Python per-row parsing.

Strategy: Run dsdgen() once in a single DuckDB connection, then export
tables to Parquet sequentially (DuckDB COPY is already internally parallel).
"""

from __future__ import annotations

import os


# DuckDB compression codec mapping (Parquet codec names)
_COMPRESSION_MAP = {
    "snappy": "snappy",
    "gzip": "gzip",
    "zstd": "zstd",
    "none": "uncompressed",
}


def generate_all_duckdb(
    scale_factor: int,
    output_dir: str,
    tables: list[str] | None = None,
    row_group_size_mb: int = 128,
    compression: str = "snappy",
    progress_callback=None,
) -> dict[str, str]:
    """Generate TPC-DS data using DuckDB's dsdgen, then export to Parquet.

    Runs dsdgen() once, then exports each table via COPY TO.

    Args:
        scale_factor: TPC-DS scale factor
        output_dir: Root output directory
        tables: List of tables to export (None = all)
        row_group_size_mb: Target row group size in MB
        compression: Parquet compression codec
        progress_callback: Called with (table_name,) after each table is exported

    Returns dict mapping table_name -> output parquet file path.
    """
    import duckdb

    codec = _COMPRESSION_MAP.get(compression, "snappy")

    con = duckdb.connect()
    con.execute("INSTALL tpcds; LOAD tpcds;")
    con.execute(f"CALL dsdgen(sf={scale_factor})")

    # Get list of generated tables
    all_tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]

    if tables:
        export_tables = [t for t in tables if t in all_tables]
    else:
        export_tables = all_tables

    outputs = {}
    for table_name in export_tables:
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        if row_count == 0:
            if progress_callback:
                progress_callback(table_name)
            continue

        # Create output directory
        table_output_dir = os.path.join(output_dir, table_name)
        os.makedirs(table_output_dir, exist_ok=True)
        out_file = os.path.join(table_output_dir, "part-00000.parquet")

        # Use default ~1M rows per row group (DuckDB handles sizing well)
        rg_rows = max(1024, 1024 * 1024)

        con.execute(
            f"COPY {table_name} TO '{out_file}' "
            f"(FORMAT PARQUET, COMPRESSION '{codec}', ROW_GROUP_SIZE {rg_rows})"
        )
        outputs[table_name] = out_file

        if progress_callback:
            progress_callback(table_name)

    con.close()
    return outputs

"""Worker — runs dsdgen for a single table chunk and converts to Parquet.

Each worker is invoked as a separate process via multiprocessing.Pool.
It runs dsdgen to generate a .dat file, parses it with PyArrow's streaming
CSV reader (constant memory), and writes Parquet incrementally.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from .schema import TableDef, ALL_TABLES, COMPANION_PAIRS, COMPANION_PRIMARY

# Default batch size for streaming CSV reads: 1M rows per batch.
# At ~200 bytes/row for fact tables, this is ~200MB per batch in Arrow memory.
_DEFAULT_BATCH_ROWS = 1_000_000


def _build_csv_options(table_def: TableDef) -> tuple[pcsv.ReadOptions, pcsv.ParseOptions, pcsv.ConvertOptions, bool]:
    """Build PyArrow CSV reader options for a TPC-DS table.

    dsdgen output is pipe-delimited with a trailing pipe and no header.
    Returns (read_opts, parse_opts, convert_opts, has_time_cols).
    """
    # Extra column to absorb trailing pipe delimiter
    extra_col = "__trailing__"
    col_names = table_def.column_names + [extra_col]

    read_opts = pcsv.ReadOptions(
        column_names=col_names,
        autogenerate_column_names=False,
        block_size=1 << 24,  # 16MB read blocks for good I/O throughput
    )
    # Try to use invalid_row_handler to skip rows with wrong column count
    # (can happen with companion table dat files at large SF)
    try:
        parse_opts = pcsv.ParseOptions(
            delimiter="|",
            invalid_row_handler=lambda row: 'skip',
        )
    except TypeError:
        # Older pyarrow versions don't support invalid_row_handler
        parse_opts = pcsv.ParseOptions(
            delimiter="|",
        )

    has_time_cols = any(pa.types.is_time(dt) for _, dt in table_def.columns)

    csv_types = {}
    for name, dtype in table_def.columns:
        if pa.types.is_time(dtype):
            csv_types[name] = pa.string()  # parse as string, cast after
        else:
            csv_types[name] = dtype
    csv_types[extra_col] = pa.string()

    # Only include the columns we need (skip trailing)
    include_columns = table_def.column_names

    convert_opts = pcsv.ConvertOptions(
        column_types=csv_types,
        strings_can_be_null=True,
        null_values=[""],
        include_columns=include_columns,
    )

    return read_opts, parse_opts, convert_opts, has_time_cols


def _cast_to_schema(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """Coerce a RecordBatch to ``schema`` by per-column ``Array.cast``.

    Equivalent to ``batch.cast(schema)`` but avoids the PyArrow >= 14
    ``RecordBatch.cast`` method, so this works back to PyArrow 4. Columns whose
    type already matches are passed through unchanged.
    """
    arrays = []
    for name in schema.names:
        col = batch.column(name)
        target = schema.field(name).type
        arrays.append(col if col.type == target else col.cast(target))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _cast_time_columns(batch: pa.RecordBatch, table_def: TableDef) -> pa.RecordBatch:
    """Cast string time columns to time32('s') in a record batch."""
    arrays = list(batch.columns)
    names = batch.schema.names

    for i, (name, dtype) in enumerate(table_def.columns):
        if pa.types.is_time(dtype) and name in names:
            col_idx = names.index(name)
            str_col = arrays[col_idx]
            time_arr = pa.array(
                [_parse_time_str(v.as_py()) if v.is_valid else None for v in str_col],
                type=dtype,
            )
            arrays[col_idx] = time_arr

    return pa.RecordBatch.from_arrays(arrays, schema=table_def.schema)


def _parse_time_str(s: str):
    """Parse 'HH:MM:SS' to seconds since midnight (for time32('s'))."""
    if s is None:
        return None
    parts = s.split(":")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])


def _streaming_dat_to_parquet(
    dat_path: str,
    table_def: TableDef,
    out_file: str,
    row_group_size_mb: int = 128,
    compression: str = "snappy",
) -> int:
    """Stream a .dat file to Parquet with constant memory usage.

    Uses PyArrow's open_csv() streaming reader to read batches of rows,
    then writes each batch as a row group to the Parquet file.

    Returns the number of rows written.
    """
    read_opts, parse_opts, convert_opts, has_time_cols = _build_csv_options(table_def)

    try:
        reader = pcsv.open_csv(
            dat_path,
            read_options=read_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        )
    except pa.lib.ArrowInvalid as e:
        first_line = ""
        try:
            with open(dat_path, "r") as f:
                first_line = f.readline().rstrip("\n")
        except Exception:
            pass
        n_fields = len(first_line.split("|")) if first_line else 0
        raise RuntimeError(
            f"CSV parse error for table '{table_def.name}' in {dat_path}: {e}\n"
            f"  Expected {len(table_def.column_names)} columns + trailing pipe = "
            f"{len(table_def.column_names) + 1} fields\n"
            f"  First line has {n_fields} fields: {first_line[:200]}"
        ) from e

    writer = None
    total_rows = 0

    try:
        for batch in reader:
            if batch.num_rows == 0:
                continue

            # Cast time columns if needed
            if has_time_cols:
                batch = _cast_time_columns(batch, table_def)
            else:
                # Ensure schema matches (column types from CSV should already match).
                # Use per-column cast for compatibility with PyArrow < 14, which
                # has no RecordBatch.cast.
                batch = _cast_to_schema(batch, table_def.schema)

            if writer is None:
                writer = pq.ParquetWriter(
                    out_file,
                    schema=table_def.schema,
                    compression=compression,
                )

            writer.write_batch(batch)
            total_rows += batch.num_rows
    except pa.lib.ArrowInvalid as e:
        # If we already wrote some rows, log warning but don't fail
        # This can happen when dsdgen produces occasional malformed lines
        if total_rows > 0:
            import sys
            print(
                f"WARNING: CSV parse error after {total_rows} rows in "
                f"'{table_def.name}' ({dat_path}): {e}",
                file=sys.stderr,
            )
        else:
            raise

    finally:
        if writer is not None:
            writer.close()

    return total_rows


def generate_chunk(
    dsdgen_path: str,
    table_name: str,
    scale_factor: int,
    parallel: int,
    child: int,
    output_dir: str,
    row_group_size_mb: int = 128,
    compression: str = "snappy",
) -> dict[str, str]:
    """Generate one chunk of one (or two companion) tables.

    Uses streaming CSV-to-Parquet conversion with constant memory usage,
    regardless of table size.

    Returns dict mapping table_name -> output parquet file path.
    """
    # Resolve the primary table for companion pairs
    primary_table = COMPANION_PRIMARY.get(table_name, table_name)

    with tempfile.TemporaryDirectory(prefix=f"tpcds_{primary_table}_c{child}_") as tmp_dir:
        # Run dsdgen
        cmd = [
            dsdgen_path,
            "-SCALE", str(scale_factor),
            "-TABLE", primary_table,
            "-DIR", tmp_dir,
            "-FORCE", "Y",
            "-QUIET", "Y",
        ]
        if parallel > 1:
            cmd.extend(["-PARALLEL", str(parallel), "-CHILD", str(child)])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=os.path.dirname(dsdgen_path),  # dsdgen needs tpcds.idx in cwd
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"dsdgen failed for {primary_table} child={child}: {result.stderr}"
            )

        # Process each generated .dat file
        outputs = {}
        dat_files = list(Path(tmp_dir).glob("*.dat"))

        for dat_file in dat_files:
            # Determine table name from filename
            # dsdgen names: table.dat (no parallel) or table_child_parallel.dat
            fname = dat_file.stem
            tname = fname
            if parallel > 1:
                suffix = f"_{child}_{parallel}"
                if fname.endswith(suffix):
                    tname = fname[:-len(suffix)]
                else:
                    parts = fname.rsplit("_", 2)
                    if len(parts) >= 3 and parts[-1].isdigit() and parts[-2].isdigit():
                        tname = "_".join(parts[:-2])

            if tname not in ALL_TABLES:
                continue

            table_def = ALL_TABLES[tname]

            # Build output path
            table_output_dir = os.path.join(output_dir, tname)
            os.makedirs(table_output_dir, exist_ok=True)

            if parallel > 1:
                out_file = os.path.join(table_output_dir, f"part-{child:05d}.parquet")
            else:
                out_file = os.path.join(table_output_dir, "part-00000.parquet")

            # Stream .dat → Parquet with constant memory
            rows = _streaming_dat_to_parquet(
                dat_path=str(dat_file),
                table_def=table_def,
                out_file=out_file,
                row_group_size_mb=row_group_size_mb,
                compression=compression,
            )

            if rows > 0:
                outputs[tname] = out_file

        return outputs

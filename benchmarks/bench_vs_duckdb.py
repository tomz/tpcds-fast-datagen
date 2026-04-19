"""Benchmark: tpcds-fast-datagen vs DuckDB dsdgen at small scale factors.

NOTE: For comprehensive engine comparison (DuckDB vs dsdgen-multiprocess across
SF=1..200 with peak RSS measurement and OOM detection), use bench_engines.py.
This file remains as a quick spot-check at SF=1 and SF=5 only.
"""

import os
import shutil
import time
import json
from datetime import datetime

import duckdb
import pyarrow.parquet as pq


def bench_duckdb(scale_factor: int, output_dir: str) -> dict:
    """Benchmark DuckDB's built-in dsdgen → parquet export."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    start = time.time()
    con = duckdb.connect()

    print(f"  [DuckDB] Generating SF={scale_factor} in-memory...")
    t0 = time.time()
    con.execute(f"CALL dsdgen(sf={scale_factor})")
    gen_time = time.time() - t0

    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    print(f"  [DuckDB] Generated {len(tables)} tables in {gen_time:.1f}s, writing parquet...")

    write_start = time.time()
    for table in tables:
        table_dir = os.path.join(output_dir, table)
        os.makedirs(table_dir, exist_ok=True)
        con.execute(f"""
            COPY {table} TO '{table_dir}'
            (FORMAT 'parquet', PER_THREAD_OUTPUT, OVERWRITE)
        """)
        con.execute(f"DROP TABLE {table}")
    write_time = time.time() - write_start

    total_time = time.time() - start
    con.close()

    # Measure total output size
    total_size = 0
    for root, dirs, files in os.walk(output_dir):
        for f in files:
            total_size += os.path.getsize(os.path.join(root, f))

    return {
        "tool": "duckdb",
        "scale_factor": scale_factor,
        "gen_time_s": round(gen_time, 2),
        "write_time_s": round(write_time, 2),
        "total_time_s": round(total_time, 2),
        "total_size_mb": round(total_size / (1024 * 1024), 1),
        "throughput_mbs": round(total_size / (1024 * 1024) / max(0.01, total_time), 1),
    }


def bench_fast_datagen(scale_factor: int, parallel: int, output_dir: str) -> dict:
    """Benchmark tpcds-fast-datagen."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    from tpcds_fast_datagen.config import GenConfig
    from tpcds_fast_datagen.generator import generate

    config = GenConfig(
        scale_factor=scale_factor,
        parallel=parallel,
        output_dir=output_dir,
        overwrite=True,
    )

    start = time.time()
    summary = generate(config)
    total_time = time.time() - start

    # Measure total output size
    total_size = 0
    for root, dirs, files in os.walk(output_dir):
        for f in files:
            total_size += os.path.getsize(os.path.join(root, f))

    return {
        "tool": f"fast-datagen (p={parallel})",
        "scale_factor": scale_factor,
        "gen_time_s": None,  # combined gen+write
        "write_time_s": None,
        "total_time_s": round(total_time, 2),
        "total_size_mb": round(total_size / (1024 * 1024), 1),
        "throughput_mbs": round(total_size / (1024 * 1024) / max(0.01, total_time), 1),
    }


def run_benchmarks():
    import multiprocessing
    cpu_count = multiprocessing.cpu_count()
    print(f"CPU cores: {cpu_count}")
    print(f"Date: {datetime.now().isoformat()}")
    print()

    scale_factors = [1, 5]
    results = []

    for sf in scale_factors:
        print(f"\n{'='*60}")
        print(f"Scale Factor = {sf}")
        print(f"{'='*60}")

        # DuckDB
        print(f"\n--- DuckDB dsdgen SF={sf} ---")
        duckdb_dir = f"/tmp/bench_duckdb_sf{sf}"
        try:
            r = bench_duckdb(sf, duckdb_dir)
            results.append(r)
            print(f"  Total: {r['total_time_s']}s ({r['total_size_mb']} MB, {r['throughput_mbs']} MB/s)")
        except Exception as e:
            print(f"  FAILED: {e}")
            results.append({"tool": "duckdb", "scale_factor": sf, "error": str(e)})
        finally:
            if os.path.exists(duckdb_dir):
                shutil.rmtree(duckdb_dir)

        # fast-datagen with cpu_count workers
        parallel = cpu_count
        print(f"\n--- fast-datagen SF={sf}, parallel={parallel} ---")
        fast_dir = f"/tmp/bench_fast_sf{sf}"
        try:
            r = bench_fast_datagen(sf, parallel, fast_dir)
            results.append(r)
            print(f"  Total: {r['total_time_s']}s ({r['total_size_mb']} MB, {r['throughput_mbs']} MB/s)")
        except Exception as e:
            print(f"  FAILED: {e}")
            results.append({"tool": f"fast-datagen (p={parallel})", "scale_factor": sf, "error": str(e)})
        finally:
            if os.path.exists(fast_dir):
                shutil.rmtree(fast_dir)

    # Print summary table
    print(f"\n\n{'='*80}")
    print("BENCHMARK RESULTS")
    print(f"{'='*80}")
    print(f"{'Tool':<30} {'SF':>4} {'Time (s)':>10} {'Size (MB)':>10} {'MB/s':>8} {'Speedup':>8}")
    print("-" * 80)

    # Group by SF for speedup calculation
    by_sf = {}
    for r in results:
        sf = r["scale_factor"]
        by_sf.setdefault(sf, []).append(r)

    for sf in scale_factors:
        entries = by_sf.get(sf, [])
        duckdb_time = None
        for r in entries:
            if r["tool"] == "duckdb" and "error" not in r:
                duckdb_time = r["total_time_s"]

        for r in entries:
            if "error" in r:
                print(f"{r['tool']:<30} {sf:>4} {'FAILED':>10}")
                continue
            speedup = ""
            if duckdb_time and r["tool"] != "duckdb" and r["total_time_s"]:
                speedup = f"{duckdb_time / r['total_time_s']:.2f}x"
            print(f"{r['tool']:<30} {sf:>4} {r['total_time_s']:>10.1f} {r['total_size_mb']:>10.1f} {r['throughput_mbs']:>8.1f} {speedup:>8}")

    # Save results
    results_file = "/home/tomz/research/benchmarks/TPC-DS-fast-datagen/benchmarks/results.json"
    os.makedirs(os.path.dirname(results_file), exist_ok=True)
    with open(results_file, "w") as f:
        json.dump({"cpu_count": cpu_count, "date": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"\nResults saved to {results_file}")


if __name__ == "__main__":
    run_benchmarks()

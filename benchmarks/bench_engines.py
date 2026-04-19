#!/usr/bin/env python3
"""Single-node datagen comparison: DuckDB engine vs dsdgen-multiprocess engine.

Measures wall-clock + peak RSS for both engines across a range of scale factors.
Designed to demonstrate:
  1. At small SF, both engines are comparable.
  2. As SF grows, DuckDB engine memory grows ~linearly and eventually OOMs.
  3. dsdgen-multiprocess holds constant memory per worker and scales by disk.

Outputs JSON to benchmark_results.json + a markdown summary table.

Run on wn0 (16c / 125 GB RAM / 305 GB free /mnt):
    python3 bench_engines.py --output /mnt/resource/sshuser/bench_engines
"""
from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def _peak_rss_mb_self_and_children() -> float:
    """Peak RSS in MB across this process + all reaped children (Linux)."""
    self_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    chld_kb = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return (self_kb + chld_kb) / 1024.0


def _dir_size_mb(path: str) -> float:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total / (1024 * 1024)


def bench_duckdb(sf: int, output_dir: str, timeout_s: int) -> dict:
    """Run DuckDB engine in a subprocess so we can measure peak RSS and catch OOM."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    code = f"""
import resource, time, json, sys
from tpcds_fast_datagen.config import GenConfig
from tpcds_fast_datagen.generator import generate
t0 = time.time()
generate(GenConfig(scale_factor={sf}, output_dir={output_dir!r}, overwrite=True, engine='duckdb'))
elapsed = time.time() - t0
peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print('__RESULT__', json.dumps({{'elapsed_s': elapsed, 'peak_rss_mb': peak_kb/1024}}))
"""
    start = time.time()
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired:
        return {"engine": "duckdb", "sf": sf, "status": "timeout",
                "wall_s": time.time() - start, "size_mb": 0.0}

    if proc.returncode != 0:
        last_lines = (proc.stderr or proc.stdout or "")[-500:]
        # Heuristic: detect OOM kills
        oom = "MemoryError" in last_lines or "Killed" in last_lines or proc.returncode == -9
        return {
            "engine": "duckdb", "sf": sf,
            "status": "oom" if oom else "error",
            "wall_s": time.time() - start,
            "size_mb": _dir_size_mb(output_dir),
            "error_tail": last_lines.strip().splitlines()[-3:] if last_lines else [],
        }

    # Parse the embedded result JSON line
    elapsed_s = peak_rss_mb = None
    for line in proc.stdout.splitlines():
        if line.startswith("__RESULT__"):
            payload = json.loads(line.split(" ", 1)[1])
            elapsed_s = payload["elapsed_s"]
            peak_rss_mb = payload["peak_rss_mb"]

    return {
        "engine": "duckdb", "sf": sf, "status": "ok",
        "wall_s": round(elapsed_s or (time.time() - start), 2),
        "peak_rss_mb": round(peak_rss_mb or 0.0, 1),
        "size_mb": round(_dir_size_mb(output_dir), 1),
    }


def bench_dsdgen(sf: int, parallel: int, output_dir: str, timeout_s: int) -> dict:
    """Run dsdgen-multiprocess engine in a subprocess; track peak RSS of self + children."""
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    code = f"""
import resource, time, json, sys
from tpcds_fast_datagen.config import GenConfig
from tpcds_fast_datagen.generator import generate
t0 = time.time()
generate(GenConfig(scale_factor={sf}, parallel={parallel}, output_dir={output_dir!r},
                   overwrite=True, engine='dsdgen'))
elapsed = time.time() - t0
peak_self_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_chld_kb = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
print('__RESULT__', json.dumps({{'elapsed_s': elapsed,
    'peak_rss_mb': (peak_self_kb + peak_chld_kb)/1024}}))
"""
    start = time.time()
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, timeout=timeout_s,
        )
    except subprocess.TimeoutExpired:
        return {"engine": f"dsdgen-p{parallel}", "sf": sf, "status": "timeout",
                "wall_s": time.time() - start, "size_mb": 0.0}

    if proc.returncode != 0:
        last_lines = (proc.stderr or proc.stdout or "")[-500:]
        return {
            "engine": f"dsdgen-p{parallel}", "sf": sf, "status": "error",
            "wall_s": time.time() - start,
            "size_mb": _dir_size_mb(output_dir),
            "error_tail": last_lines.strip().splitlines()[-3:],
        }

    elapsed_s = peak_rss_mb = None
    for line in proc.stdout.splitlines():
        if line.startswith("__RESULT__"):
            payload = json.loads(line.split(" ", 1)[1])
            elapsed_s = payload["elapsed_s"]
            peak_rss_mb = payload["peak_rss_mb"]

    return {
        "engine": f"dsdgen-p{parallel}", "sf": sf, "status": "ok",
        "wall_s": round(elapsed_s or (time.time() - start), 2),
        "peak_rss_mb": round(peak_rss_mb or 0.0, 1),
        "size_mb": round(_dir_size_mb(output_dir), 1),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True, help="Working dir for outputs (will be cleaned per run)")
    p.add_argument("--scales", type=str, default="1,5,10,20,50,100,200",
                   help="Comma-separated scale factors to test")
    p.add_argument("--parallel", type=int, default=0, help="Workers for dsdgen engine (0=cpu_count)")
    p.add_argument("--results", type=str, default="bench_engines_results.json")
    p.add_argument("--timeout", type=int, default=7200, help="Per-run timeout (seconds)")
    p.add_argument("--skip-duckdb-above", type=int, default=200,
                   help="Skip DuckDB engine above this SF (it will OOM)")
    args = p.parse_args()

    os.makedirs(args.output, exist_ok=True)
    scales = [int(s) for s in args.scales.split(",")]
    parallel = args.parallel or os.cpu_count()

    host = os.uname().nodename
    cpus = os.cpu_count()
    try:
        with open("/proc/meminfo") as f:
            mem_kb = int(next(line for line in f if line.startswith("MemTotal:")).split()[1])
        ram_gb = mem_kb / (1024 * 1024)
    except Exception:
        ram_gb = None

    print(f"Host: {host}, CPUs: {cpus}, RAM: {ram_gb:.0f} GB" if ram_gb else f"Host: {host}, CPUs: {cpus}")
    print(f"Scales: {scales}, dsdgen parallel: {parallel}")
    print(f"Output dir: {args.output}")
    print()

    results = []
    for sf in scales:
        print(f"=== SF={sf} ===")
        if sf <= args.skip_duckdb_above:
            print(f"  duckdb engine ...")
            r = bench_duckdb(sf, os.path.join(args.output, f"duckdb_sf{sf}"), args.timeout)
            print(f"    {r}")
            results.append(r)
        else:
            print(f"  duckdb engine SKIPPED (sf > {args.skip_duckdb_above})")
            results.append({"engine": "duckdb", "sf": sf, "status": "skipped"})

        print(f"  dsdgen engine (p={parallel}) ...")
        r = bench_dsdgen(sf, parallel, os.path.join(args.output, f"dsdgen_sf{sf}"), args.timeout)
        print(f"    {r}")
        results.append(r)
        print()

    summary = {
        "host": host,
        "cpus": cpus,
        "ram_gb": round(ram_gb, 1) if ram_gb else None,
        "date_utc": datetime.now(timezone.utc).isoformat(),
        "parallel": parallel,
        "scales": scales,
        "results": results,
    }
    with open(args.results, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Wrote {args.results}")

    # Markdown table
    print()
    print("| SF | engine | status | wall (s) | peak RSS (MB) | output (MB) | MB/s |")
    print("|---:|---|---|---:|---:|---:|---:|")
    for r in results:
        if r.get("status") == "skipped":
            print(f"| {r['sf']} | {r['engine']} | skipped | - | - | - | - |")
        elif r.get("status") in ("oom", "error", "timeout"):
            print(f"| {r['sf']} | {r['engine']} | **{r['status']}** | {r.get('wall_s','-')} | - | {r.get('size_mb',0)} | - |")
        else:
            mbs = (r['size_mb'] / r['wall_s']) if r['wall_s'] else 0
            print(f"| {r['sf']} | {r['engine']} | ok | {r['wall_s']} | {r['peak_rss_mb']} | {r['size_mb']} | {mbs:.1f} |")


if __name__ == "__main__":
    main()

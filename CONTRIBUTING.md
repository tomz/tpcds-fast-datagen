# Contributing

Thanks for your interest in `tpcds-fast-datagen`. This is a small project; the workflow is intentionally light.

## Dev setup

```bash
git clone https://github.com/tomz/tpcds-fast-datagen
cd tpcds-fast-datagen
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Running tests

```bash
pytest                       # all
pytest -v --timeout=60       # verbose with timeout
pytest --cov=tpcds_fast_datagen --cov-report=term-missing
pytest tests/test_foo.py::test_bar -v   # single test
```

The bundled tests exercise the streaming `.dat` → Parquet pipeline with a synthetic dat file — **no `dsdgen` binary is required to run the suite**. Tests that touch the orchestration layer or the DuckDB engine are gated and skipped if their dependencies are missing.

## Running the CLI

```bash
# single-node, auto engine
tpcds-gen --scale 1 --parallel 8 --output /tmp/tpcds_sf1 --overwrite

# distributed via Spark (delegates to spark-submit)
tpcds-gen --engine spark --scale 1000 --output abfs:///tpcds/sf1000 \
    -- --master yarn --num-executors 10 --executor-cores 16

# spark-submit-only convenience wrapper
tpcds-gen-spark-submit --scale 1000 --output abfs:///tpcds/sf1000 \
    -- --master yarn --num-executors 10
```

For notebooks, see [`docs/notebooks-and-livy.md`](docs/notebooks-and-livy.md).

## Benchmarks

```bash
python benchmarks/bench_engines.py --output /var/tmp/bench   # full DuckDB vs dsdgen comparison
python benchmarks/bench_vs_duckdb.py                          # quick spot-check at SF=1, SF=5
```

## Running the dsdgen engine end-to-end (optional)

You only need this if you're touching `worker.py`, `binary.py`, `generator.py`, or the dsdgen-multiprocess path.

1. Build `dsdgen` from <https://github.com/databricks/tpcds-kit>:
   ```bash
   git clone https://github.com/databricks/tpcds-kit
   cd tpcds-kit/tools && make OS=LINUX
   ```
2. Point the package at it:
   ```bash
   export DSDGEN_PATH=$PWD/dsdgen     # tpcds.idx must sit alongside it
   ```
3. Run a small generation:
   ```bash
   tpcds-gen --scale 1 --engine dsdgen --output /tmp/tpcds_sf1 --overwrite
   ```

## Running the Spark variant

`spark_tpcds_gen.py` is **deliberately self-contained** — the driver runs Python 3.6 without `pyarrow`. Do not add imports from `src/tpcds_fast_datagen/`. Edit it as a standalone script. See its module docstring for the `spark-submit` invocation.

## Coding conventions

- Schema is the single source of truth in `schema.py`. New columns/tables go there first.
- PyArrow APIs: stay compatible with **PyArrow ≥ 11**. Avoid `RecordBatch.cast()` (added in 14) — use the `_cast_to_schema` helper.
- The dsdgen engine must hold **constant memory per worker** regardless of SF. Anything that buffers a whole table in memory is a bug.

## Submitting changes

1. Create a feature branch.
2. Add a test for new behavior or bug fix.
3. Run `pytest`; CI runs the same suite.
4. Open a PR with a short rationale.

## Reporting bugs

Include:
- OS + Python version
- `pip show pyarrow duckdb tpcds-fast-datagen`
- The full `tpcds-gen` command, scale factor, and the failing log
- For dsdgen-engine bugs, the `DSDGEN_PATH` and `dsdgen --help | head -5`

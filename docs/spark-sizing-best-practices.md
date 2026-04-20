# TPC-DS Spark Datagen — Cluster Sizing Best Practices

**Author:** Tom Zeng ([@tomz](https://github.com/tomz))
**Last updated:** 2026-04-20

This document captures sizing guidance for running `spark_tpcds_gen.py` on Azure HDInsight (or any YARN/Spark cluster) using `E8ads_v5`, `E16ads_v5`, and `E32ads_v5` node SKUs. Numbers are calibrated against three proven runs:

- **SF=10,000 on 10× E16ads_v5** (80 slots, HDFS output): 3 h 51 min
- **SF=10,000 on 5× E32ads_v5** (160 slots, ABFS output, v0.3.1, chunks=4800): **1 h 41 min**, 3.95 TB
- **SF=30,000 on 5× E32ads_v5** (160 slots, ABFS output, v0.3.1, chunks=4800): **3 h 10 min**, 11.85 TB

See §6 for the full calibration table.

---

## 1. Quick recommendation

| SKU | nodes | SF=1000 `--chunks` | SF=10000 `--chunks` | SF=100000 `--chunks` |
|---|---:|---:|---:|---:|
| **E16ads_v5** (16c / 128 GB / 600 GB SSD) | 10 | 300 | 800 | 5,000 |
| **E8ads_v5**  (8c / 64 GB / 300 GB SSD)   | 10 | 100 | 700 | 4,000 |

For other cluster sizes see §4.

---

## 2. The three constraints

Every `--chunks` choice is bounded by:

1. **dsdgen 1M-row floor.** dsdgen's `split_work()` silently drops any child whose share is below 1M rows. The smallest sharded table is `inventory`. For SF=10000 (1.31B rows of inventory), max usable chunks is ~870; for SF=100000, ~8,700.
2. **Per-node temp disk.** Every concurrent task on a node materialises one `.dat` file before streaming it to Parquet. Peak per-node usage = `cores_per_node × dat_size_per_chunk`. Must fit in `/mnt/resource` (~600 GB on E16ads, ~300 GB on E8ads) with ≥ 30% headroom.
3. **Slot saturation.** `--chunks` should comfortably exceed total task slots so executors stay busy and stragglers don't dominate.

The **proven sweet spot** is `.dat/chunk ≈ 5–10 GB` and `chunks ≈ 6–12 × slots`.

---

## 3. spark-submit templates

### E16ads_v5 (16-core nodes — recommended)

```bash
spark-submit \
  --master yarn --deploy-mode client \
  --num-executors <N> --executor-cores 16 --executor-memory 96g \
  --conf spark.network.timeout=1800s \
  --conf spark.task.maxFailures=8 \
  --archives /path/to/conda_env.tar.gz#conda_env \
  --files /path/to/dsdgen,/path/to/tpcds.idx \
  /path/to/spark_tpcds_gen.py \
    --scale <SF> \
    --output abfs:///tpcds/sf<SF> \
    --chunks <C>
```

### E8ads_v5 (8-core nodes)

```bash
spark-submit \
  --master yarn --deploy-mode client \
  --num-executors <N> --executor-cores 8 --executor-memory 48g \
  --conf spark.network.timeout=1800s \
  --conf spark.task.maxFailures=8 \
  --archives /path/to/conda_env.tar.gz#conda_env \
  --files /path/to/dsdgen,/path/to/tpcds.idx \
  /path/to/spark_tpcds_gen.py \
    --scale <SF> \
    --output abfs:///tpcds/sf<SF> \
    --chunks <C>
```

Use **1 executor per node** to maximise per-task local disk and memory bandwidth.

---

## 4. Full sizing tables

### E16ads_v5 — 16c / 128 GB / 600 GB local SSD

`--executor-cores 16 --executor-memory 96g`, 1 executor per node.

| nodes | SF | `--chunks` | rows/chunk | .dat/chunk | total tasks | per-node peak | est. wall-clock |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 3  | 1,000   | 200   | 14.4M | 3.2 GB  | ~1,230  | ~51 GB  | ~45 min |
| 3  | 10,000  | 600   | 47.7M | 10.5 GB | ~3,630  | ~168 GB | ~7.5 h |
| 3  | 100,000 | 4,000 | 71.5M | 15.7 GB | ~24,030 | ~252 GB | ~80 h ⚠️ |
| 5  | 1,000   | 200   | 14.4M | 3.2 GB  | ~1,230  | ~51 GB  | ~27 min |
| 5  | 10,000  | 783   | 36.6M | 8.1 GB  | 4,728   | ~129 GB | ~4.5 h |
| 5  | 100,000 | 4,000 | 71.5M | 15.7 GB | ~24,030 | ~252 GB | ~45 h ⚠️ |
| 10 | 1,000   | 300   | 9.6M  | 2.1 GB  | ~1,830  | ~34 GB  | ~14 min |
| 10 | 10,000  | 800   | 35.8M | 7.9 GB  | ~4,830  | ~126 GB | ~2 h 15 min |
| 10 | 100,000 | 5,000 | 57.2M | 12.6 GB | ~30,030 | ~202 GB | ~22 h |
| 15 | 1,000   | 400   | 7.2M  | 1.6 GB  | ~2,430  | ~26 GB  | ~9 min |
| 15 | 10,000  | 1,000 | 28.6M | 6.3 GB  | ~6,030  | ~101 GB | ~1.5 h |
| 15 | 100,000 | 6,000 | 47.7M | 10.5 GB | ~36,030 | ~168 GB | ~15 h |
| 20 | 1,000   | 400   | 7.2M  | 1.6 GB  | ~2,430  | ~26 GB  | ~7 min |
| 20 | 10,000  | 1,000 | 28.6M | 6.3 GB  | ~6,030  | ~101 GB | ~70 min |
| 20 | 100,000 | 8,000 | 35.8M | 7.9 GB  | ~48,030 | ~126 GB | ~11 h |

### E8ads_v5 — 8c / 64 GB / 300 GB local SSD

`--executor-cores 8 --executor-memory 48g`, 1 executor per node.

| nodes | SF | `--chunks` | rows/chunk | .dat/chunk | total tasks | per-node peak | est. wall-clock |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 3  | 1,000   | 100   | 28.8M | 6.3 GB  | 593     | ~51 GB  | ~90 min |
| 3  | 10,000  | 400   | 71.5M | 15.7 GB | ~2,430  | ~126 GB | ~16 h ⚠️ |
| 3  | 100,000 | 3,000 | 95.4M | 21.0 GB | ~18,030 | ~168 GB | ~159 h ⚠️ |
| 5  | 1,000   | 100   | 28.8M | 6.3 GB  | 593     | ~51 GB  | ~54 min |
| 5  | 10,000  | 500   | 57.2M | 12.6 GB | ~3,030  | ~101 GB | ~9.5 h |
| 5  | 100,000 | 3,000 | 95.4M | 21.0 GB | ~18,030 | ~168 GB | ~95 h ⚠️ |
| 10 | 1,000   | 100   | 28.8M | 6.3 GB  | 593     | ~51 GB  | ~27 min ✓ proven |
| 10 | 10,000  | 700   | 40.9M | 9.0 GB  | ~4,230  | ~72 GB  | ~4.5 h ✓ near proven |
| 10 | 100,000 | 4,000 | 71.5M | 15.7 GB | ~24,030 | ~126 GB | ~45 h ⚠️ |
| 15 | 1,000   | 200   | 14.4M | 3.2 GB  | ~1,230  | ~26 GB  | ~18 min |
| 15 | 10,000  | 800   | 35.8M | 7.9 GB  | ~4,830  | ~63 GB  | ~3 h |
| 15 | 100,000 | 5,000 | 57.2M | 12.6 GB | ~30,030 | ~101 GB | ~30 h |
| 20 | 1,000   | 300   | 9.6M  | 2.1 GB  | ~1,830  | ~17 GB  | ~14 min |
| 20 | 10,000  | 800   | 35.8M | 7.9 GB  | ~4,830  | ~63 GB  | ~2.5 h |
| 20 | 100,000 | 6,000 | 47.7M | 10.5 GB | ~36,030 | ~84 GB  | ~22.5 h |

---

## 5. Smallest reliable cluster for SF=10000

Defined as: ≤ 6h wall-clock, fits temp disk with ≥ 30% headroom, survives a single node hiccup.

| SKU | min nodes | `--chunks` | wall-clock |
|---|---:|---:|---:|
| E16ads_v5 | **3** | 600 | ~7.5 h (borderline) |
| E16ads_v5 | **5** | 783 | ~4.5 h ✓ |
| E8ads_v5  | **6** | 700 | ~5.5 h ✓ |

Below 3 nodes, single-node failure costs ≥ 33% of cluster — not recommended for a 4–6h job.

---

## 6. Output sizes (measured)

| SF | Parquet output | rows total | source |
|---:|---:|---:|---|
| 1,000   | ~360 GB  | 6.35 B  | modelled |
| 10,000  | **3.95 TB** | 56.7 B | **measured** (2026-04-19, v0.3.1, ABFS) |
| 30,000  | **11.85 TB** | ~170 B | **measured** (2026-04-20, v0.3.1, ABFS) |
| 100,000 | ~39 TB   | ~567 B | extrapolated from SF=30K |

Per-table breakdown at SF=30,000 (chunks=4800):

| Table | Size | Files |
|---|---:|---:|
| store_sales    | 4.853 TB | 4,800 |
| catalog_sales  | 3.910 TB | 4,800 |
| web_sales      | 1.878 TB | 4,800 |
| store_returns  | ~490 GB  | 4,800 |
| catalog_returns | ~390 GB | 4,800 |
| web_returns    | ~190 GB  | 4,800 |
| inventory, customer, customer_address, etc. | < 50 GB total | ~213 |

ABFS Gen2 sustained throughput is not a bottleneck at these scales. The SF=30K run wrote at a peak ~3.7 TB/h on 5 workers (≈ 8.6 Gbps) with **zero** `ServerBusy` / `EgressIsOver*` / `503` events against a standard storage account.

---

## 7. Calibration runs

Real end-to-end measurements. Use these for interpolation when your cluster shape matches.

### SF=10,000 on 10× E16ads_v5 (80 slots, HDFS, v0.2.x)

| metric | value |
|---|---|
| Wall-clock | 3 h 51 min |
| Output | HDFS local on cluster (replication=2) |
| chunks | 783 |
| Per-task wall-clock | ~17 min avg |
| Throughput | ~0.9 TB/h |

### SF=10,000 on 5× E32ads_v5 (160 slots, ABFS, v0.3.1)

| metric | value |
|---|---|
| Wall-clock | **1 h 41 min** |
| Output | ABFS (wasbs) |
| chunks | 4,800 |
| Per-task wall-clock | ~60 sec avg (store_sales) |
| Throughput | ~2.4 TB/h |
| Total | 3.95 TB / 21,013 tasks |

### SF=30,000 on 5× E32ads_v5 (160 slots, ABFS, v0.3.1)

| metric | value |
|---|---|
| Wall-clock | **3 h 10 min** |
| Output | ABFS (wasbs) |
| chunks | 4,800 |
| Per-task wall-clock | ~125 sec avg (store_sales), ~73 sec (web_sales) |
| Throughput | ~3.7 TB/h (peak) |
| Total | 11.85 TB / 24,613 tasks |
| Speculation relaunches | 0 |
| Storage throttling | 0 |

### Observations

- **ABFS ≫ HDFS for large SF.** The v0.2.x HDFS run took 2.3× longer than v0.3.1 ABFS at the same SF, despite having half the slots. HDFS replication + local-disk write amplification dominated.
- **chunks=4800 is the sweet spot** for ~5 GB `.dat`/chunk at SF=30K. Went from 18-min tasks (chunks=2400) to 2-min tasks (chunks=4800) with the same cluster.
- **Doubling SF scales sub-linearly in wall-clock**: SF=10K→SF=30K is 3× data but only 1.9× wall-clock. The startup/dimension tables are constant overhead; once fact-table throughput saturates, it's flat at ~3.7 TB/h on this shape.
- **int64 is required for `*_ticket_number` and `*_order_number` at SF ≥ ~10K** — fixed in v0.3.1 (see CHANGELOG). Running older versions at SF=30K will fail with `CSV conversion error to int32`.

---

## 8. Tuning rules of thumb

- **`--chunks ≈ max( total_slots × 8, scale_factor / 12 )`** is a reasonable starting point.
- **Always pass `--chunks` explicitly** for SF ≥ 10,000. The auto formula in the script is conservative for big SFs.
- **Set `spark.network.timeout=1800s`** and **`spark.task.maxFailures=8`** for any run > 1h to absorb transient ABFS auth refreshes and executor heartbeat blips.
- **Don't co-locate multiple executors per node** — one executor per node maximises per-task local disk and avoids shuffle contention.
- **Cap chunks below the inventory floor**: ≤ 870 for SF=10000, ≤ 8,700 for SF=100000. Above that, dsdgen drops inventory shards silently.

---

## 9. Wall-clock model

Estimates above use:

```
wall_clock = total_task_seconds / total_slots / efficiency
```

- `total_task_seconds` ≈ `1,100 × SF × 10` (calibrated from the proven 3h51m / 80 slots / SF=10000 run)
- `efficiency` ≈ 0.80–0.85 (drops below 0.75 when `chunks/slots < 4` due to tail latency)

This means doubling cluster size halves wall-clock until you hit the tail. For SF=10000, 160 slots is the empirical inflection point — beyond that you start chasing 5-minute tail tasks.

---

## 10. Operational checklist

Before launching:

- [ ] `dsdgen` and `tpcds.idx` present on the launching node, mentioned in `--files`.
- [ ] `conda_env.tar.gz` contains `pyarrow` for the executors, mentioned in `--archives ...#conda_env`.
- [ ] Output path doesn't already exist (or you've decided to overwrite).
- [ ] ABFS container is in the same region as the cluster (cross-region adds 3–5× latency).
- [ ] `--chunks` respects the inventory floor for your SF.
- [ ] Spark history server is reachable so you can debug stragglers.

After completion, validate via row counts:

```bash
spark-submit ... count_tpcds.py /tpcds/sf<SF>
```

Spec totals: SF=1000 → 6,347,386,006 rows; SF=10000 → 56,678,668,681 rows.

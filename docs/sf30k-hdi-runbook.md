# SF≥10K on HDInsight: sizing & cost reference

> **First time on HDInsight?** Start with
> **[`docs/hdinsight-quickstart.md`](hdinsight-quickstart.md)** — five
> commands, no manual venv plumbing. This document is the **deep dive**:
> cluster sizing, storage choices, cost modeling, and the actual SF=10K /
> SF=30K timings we measured on 2026-04-19/20.
>
> Most of the manual recipes that lived here in 0.3.x (hand-pinning
> `pyarrow==10.0.1`, `tar czf venv.tar.gz`, writing a `spark-submit`
> invocation by hand) are **obsolete in 0.4.0** — the quickstart absorbs
> them. What stays here is the part you can't automate: choosing a worker
> SKU, sizing disks, picking `chunks=`, deciding HDFS vs ABFS.

**Author:** Tom Zeng ([@tomz](https://github.com/tomz))
**Last updated:** 2026-04-21
**Applies to:** `tpcds-fast-datagen` ≥ 0.4.0
**Verified at:** SF=10,000 (1 h 41 m) and SF=30,000 (3 h 10 m) on
5× E32ads_v5 against ABFS — see §6.

---

## 1. When you need this document

The quickstart is sufficient up to ~SF=1000 on a default 5×E16ads_v5
cluster. Past that, you'll bottleneck on one or more of: vCPU count, NVMe
scratch space, attached HDD throughput, or the storage account's egress
cap. This document tells you what to change.

| target SF | recommended cluster | rough wall time | document |
|---:|---|---:|---|
| 1 – 1000 | 5× E16ads_v5 (default) | sec to ~1 h | quickstart |
| 1000 – 10,000 | 5× E16ads_v5 → 5× E32ads_v5 | 1 – 4 h | this doc, §2-§4 |
| 10,000 – 30,000 | 5× E32ads_v5 + 16× S30 disks | 3 – 4 h | this doc, §2-§6 |
| 30,000+ | scale workers up; consider rep=1 | 4 h+ | this doc, §6.4 |

---

## 2. Cluster sizing

### 2.1 The four constraints

For a single TPC-DS run, in priority order:

1. **vCPU × executors → task-slot count.** Aim for `chunks ≥ 15 × slots`
   to absorb tail-task drag. At 160 slots (5 × 32 cores), use
   `chunks=2400` (15× ratio) to `chunks=4800` (30× ratio). 4800 is what
   we used for SF=30K v2 — see §6.2 for why that mattered.

2. **NVMe scratch per worker.** Each task writes its raw `.dat` shard to
   `/mnt/tpch_*` before streaming to Parquet. At SF=30K, `chunks=4800`
   gives ~5 GB per shard for `store_sales`. With 32 concurrent tasks per
   worker that's 160 GB of live scratch. E32ads_v5's 1.2 TB NVMe absorbs
   this with 7× headroom.

3. **Attached HDD throughput** (only matters for `--output hdfs:///...`,
   not ABFS). A single S30 caps at 60 MB/s; 16× S30 = 960 MB/s aggregate
   per node, capped at the VM's 865 MB/s outbound. ABFS sidesteps this
   entirely (see §3).

4. **Storage account egress cap.** A standard ABFS Gen2 account caps at
   60 Gbps regional egress. We saw zero `503 ServerBusy` events at peak
   ~3 TB/h aggregate write to ABFS — well under the cap.

### 2.2 Recommended SKUs

| SF | worker SKU | workers | head SKU | extra disks |
|---:|---|---:|---|---|
| 1K – 10K | `Standard_E16ads_v5` (16 vCPU, 128 GB, 600 GB NVMe) | 5 | `Standard_E8ads_v5` × 2 | none |
| 10K – 30K (ABFS output) | `Standard_E32ads_v5` (32 vCPU, 256 GB, 1.2 TB NVMe) | 5 | `Standard_E8ads_v5` × 2 | none |
| 10K – 30K (HDFS output) | `Standard_E32ads_v5` | 5 | `Standard_E8ads_v5` × 2 | **16× S30 (1 TiB) per worker** |
| 30K+ | scale workers up to fit your quota | 8-10 | `Standard_E8ads_v5` × 2 | match storage choice |

### 2.3 Quota fit (East US 2 reference)

A 5× E32ads_v5 + 2× E8ads_v5 cluster needs:

- Subscription `Standard EADSv5 Family vCPUs`: 5×32 + 2×8 = **176**
- HDI `StandardEadsV5Family` cores: **176** (HDI maintains its own quota)

Check both before provisioning:

```bash
SUB=<your-subscription-id>
REGION=eastus2
az vm list-usage --location $REGION -o table | grep -iE "EADSv5|Total Regional vCPUs"
az rest --method get \
  --uri "https://management.azure.com/subscriptions/$SUB/providers/Microsoft.HDInsight/locations/$REGION/usages?api-version=2023-04-15-preview" \
  --query 'value[?contains(to_string(name.value), `Eads`)].{name:name.localizedValue, used:currentValue, limit:limit}' \
  -o table
```

---

## 3. HDFS vs ABFS for output

This is the single biggest knob. We measured a **2.3× throughput
improvement** going from HDFS-on-attached-HDDs to ABFS at the same node
count (SF=10K validation, §6.1).

| dimension | HDFS on attached S30s | ABFS Hot |
|---|---|---|
| write throughput per node | ~170 MB/s sustained | ~690 MB/s sustained |
| node loss tolerance | rep=2 (33% disk overhead) | erasure-coded by Azure |
| post-run copy needed | yes (distcp to ABFS or it dies with the cluster) | no |
| disk provisioning cost | $40/disk-month × 80 disks = ~$3200/mo | nil |
| operational complexity | ambari reconfig of `dfs.datanode.data.dir`, `chmod 1777 /mnt/tmp` | none |

**Recommendation: use ABFS unless you have a specific reason not to.** Pass
`--output wasbs://container@account.blob.core.windows.net/path` directly
to `tpcds-gen-spark-submit`. Skip the `--workernode-data-disks-per-node`
flag at provision time.

The historical reason to choose HDFS was "avoid ABFS egress costs during
the run." But ABFS-internal traffic (cluster ↔ same-region storage
account) is **free**. Only cross-region egress is billed.

---

## 4. Choosing `chunks`

Pass via `tpcds-gen-spark-submit --chunks N` (or as the `chunks=` kwarg
in `generate()`).

| target | rule of thumb |
|---|---|
| SF ≤ 1000 | leave at default — auto-sizer picks ~96-230 |
| SF = 10,000 | `chunks=2400` (15× slots) |
| SF = 30,000 | `chunks=4800` (30× slots) — halved per-task time, dramatically improved load balancing |
| SF > 30,000 | scale linearly with SF |

**Why higher chunks helped at SF=30K:** at `chunks=2400`, the longest
`store_sales` task ran 18 minutes; one straggler delayed the whole stage.
At `chunks=4800`, per-task time dropped to 60-125 s, which made the tail
recoverable via speculation (`spark.speculation=true`,
`spark.speculation.quantile=0.85`, `spark.speculation.multiplier=1.4`).

The auto-sizer doesn't go this high yet — pass explicitly for SF ≥ 10K.

---

## 5. Provisioning the cluster

The `tpcds-gen` tool does not yet provision clusters. Use `az hdinsight
create` directly.

### 5.1 ABFS-output cluster (recommended)

```bash
RG=tomz
NAME=tomz-spark-sf30k
REGION=eastus2
STORAGE_ACCT=<your-account>
STORAGE_CONTAINER=<your-container>
ADMIN_PW='<strong-password>'
SSH_PW='<different-strong-password>'

az hdinsight create \
  -g $RG -n $NAME \
  -t spark --version 5.1 --location $REGION \
  --workernode-count 5 \
  --workernode-size standard_e32ads_v5 \
  --headnode-size standard_e8ads_v5 \
  --storage-account $STORAGE_ACCT \
  --storage-container $STORAGE_CONTAINER \
  --http-user admin -p "$ADMIN_PW" \
  --ssh-user sshuser --ssh-password "$SSH_PW"
```

Provisioning takes ~15-25 min. Watch:

```bash
az hdinsight show -n $NAME -g $RG --query 'properties.clusterState' -o tsv
# Accepted -> InProgress -> Running
```

### 5.2 HDFS-output cluster (if you must)

Add the disk flags:

```bash
az hdinsight create \
  ... \
  --workernode-data-disks-per-node 16 \
  --workernode-data-disk-size 1024 \
  --workernode-data-disk-storage-account-type standard_lrs
```

After provisioning, you must reconfigure HDFS to actually use the disks
(see §7.3 for the gotcha — Ambari REST is finicky here).

### 5.3 Then run the quickstart

Everything from `tpcds-gen package-env` through `tpcds-gen-spark-submit
--hdi --via-livy` is the same as the small-SF case — see
[`docs/hdinsight-quickstart.md`](hdinsight-quickstart.md). The only
deltas at large SF are:

- pass `--chunks` explicitly (see §4)
- override Spark resource defaults: `--driver-memory`, `--executor-memory`,
  `--executor-cores`, `--num-executors` via the `tpcds-gen-spark-submit`
  trailing args

Example for SF=30K:

```bash
tpcds-gen-spark-submit \
    --hdi --via-livy https://$NAME.azurehdinsight.net \
    --env wasbs://.../tpcds-env.tar.gz \
    --binary wasbs://.../dsdgen \
    --idx wasbs://.../tpcds.idx \
    --scale 30000 --chunks 4800 \
    --output wasbs://.../tpcds/sf30000 \
    -- \
    --num-executors 5 --executor-cores 32 --executor-memory 192g \
    --conf spark.network.timeout=1800s \
    --conf spark.task.maxFailures=4 \
    --conf spark.speculation=true \
    --conf spark.speculation.quantile=0.85 \
    --conf spark.speculation.multiplier=1.4
```

---

## 6. Actual results (2026-04-19/20)

### 6.1 Timing summary

| Run | Output | Wall-clock | Total written | Throughput | Outcome |
|---|---|---|---|---|---|
| SF=30K v1 (HDFS, chunks=2400, rep=2) | `hdfs:///tpcds/sf30000` | ~5 h before kill | 1.5 TB | ~0.3 TB/h | ❌ killed — int32 overflow on `ss_ticket_number` (`2307000001 > 2^31`) |
| **SF=10K validation** (ABFS, chunks=4800, schema fix v0.3.1) | `wasbs://.../tpcds/sf10000` | **1 h 41 min** | 3.95 TB | ~2.4 TB/h | ✅ |
| **SF=30K v2** (ABFS, chunks=4800, schema fix v0.3.1) | `wasbs://.../tpcds/sf30000` | **3 h 10 min** | 11.847 TB | ~3.7 TB/h | ✅ |

### 6.2 Per-table breakdown (SF=30K v2)

| Table | Chunks | Size | Stage 3 task pace |
|---|---|---|---|
| store_sales | 4800 | 4.853 TB | ~125 s/task |
| catalog_sales | 4800 | 3.910 TB | ~127 s/task |
| web_sales | 4800 | 1.878 TB | ~73 s/task |
| dimensions + small fact tables | ~213 | < 50 GB total | ~12 s/task each |
| **Total** | 24,613 | **11.847 TB** | — |

### 6.3 Knobs that mattered

The v1 → v2 throughput gain was **~12×**. Two changes mattered most:

1. **HDFS → ABFS** — removed datanode write-amp + sync overhead. ABFS
   scales to ~60 Gbps egress on a standard storage account; we saw zero
   `ServerBusy` / `EgressIsOverAccountLimit` / `503` events at peak ~3 TB/h.
2. **chunks=2400 → 4800** — halved per-task runtime, dramatically improved
   load balancing, shortened the failure-recovery tail.

A third worth mentioning: `spark.task.maxFailures: 8 → 4` surfaced the
int32 overflow bug in attempt 4 instead of attempt 8. Worth ~1 h of
wasted compute on a deterministic-failure run.

### 6.4 Beyond SF=30K

We haven't measured SF≥50K. Linear extrapolation suggests:

| SF | est. wall (5× E32ads_v5, ABFS) | est. output | est. cost |
|---:|---:|---:|---:|
| 50,000 | ~5 h | 20 TB | ~$50 |
| 100,000 | ~10 h | 40 TB | ~$100 |

For SF ≥ 50K, consider:
- Adding workers (8 or 10× E32ads_v5) to keep wall time under a coffee break
- Sharding the output across multiple ABFS accounts to dodge the 60 Gbps cap
- `spark.hadoop.dfs.replication=1` if you used HDFS (you didn't, see §3)

---

## 7. Failure modes and recovery

| symptom | likely cause | fix |
|---|---|---|
| `ImportError: GLIBCXX_3.4.26 not found` | env tarball not unpacked, or wrong pyarrow version | confirm `--archives env#env` and `spark.executorEnv.PYSPARK_PYTHON=./env/bin/python`; rebuild tarball with `tpcds-gen package-env` (which pins pyarrow=14) |
| `dsdgen: GLIBC_2.34 not found` | uploaded a modern dsdgen | rebuild on the headnode: `tpcds-gen install-dsdgen --target hdi-glibc-2.27` (auto-falls-through to `--from-source`) |
| `Futures timed out after [100000 milliseconds]` | YARN AM 100 s timeout | not your bug — the 0.4.0 bootstrap initializes SparkSession first |
| `400 Bad Request` from Livy | missing `X-Requested-By` header | always go through `tpcds-gen-spark-submit --via-livy`; never hand-craft the POST |
| `dsdgen_path` ignored on cluster | (0.3.x footgun) | upgrade to ≥0.4.0; explicit path is now belt-and-suspenders, SparkFiles wins |
| Tasks stuck >30 min on one node | slow HDD or noisy neighbor | speculation kicks in at 85% completion; otherwise restart that executor |
| HDFS DataNode disk full | one disk filled before HDFS rebalanced | shouldn't happen with ABFS (§3); if HDFS, manually rebalance |
| Tasks fill `/tmp` on OS disk | `tmp_root` not set; `/tmp` is the 124 GB OS disk | pass `tmp_root="/mnt/tmp"` to `generate()`; first `chmod 1777 /mnt/tmp` on every worker |
| `ss_ticket_number` int32 overflow | pre-0.3.1 schema | upgrade — fixed by widening order/ticket cols to int64 |
| ABFS auth token expired mid-run | service principal token TTL < run wall-clock | use long-lived MSI on the cluster |

### 7.1 HDFS-specific gotcha: disk attachment

If you provisioned with `--workernode-data-disks-per-node 16`, those 16
disks land on the workers but **HDFS won't see them until you reconfigure
`dfs.datanode.data.dir`** in Ambari. The Ambari REST API is finicky here;
use the Python `configs.py` helper (Python 2 only — `/usr/bin/python`),
then restart HDFS from the Ambari Web UI.

---

## 8. Cost (actual, 2026-04-19/20)

5 × E32ads_v5 + 2 × E8ads_v5 head ≈ **$9.84/h** at list price (East US 2):

| Run | Wall | Compute |
|---|---|---|
| v1 attempt (killed on int32 bug) | 5 h | $49 |
| SF=10K validation | 1.7 h | $17 |
| SF=30K v2 | 3.2 h | $31 |
| Provisioning + post-run idle (scaler bug, see §8.1) | 6 h | ~$60 |
| **Total cluster compute** | | **~$160** |

Storage (held 11.85 TB on ABFS Hot, 1 month): ~$245 if held; **~$0 if
deleted same day.** This is why ABFS-as-output is so much cheaper than
HDFS-then-distcp at this scale.

### 8.1 Operational gotcha worth its own line

Our auto-scale-down script's `awk` parse left trailing whitespace in the
state string, so `[[ "$STATE" == "SUCCEEDED" ]]` never matched and the
cluster sat idle at 5-worker size for ~5 h after job completion. **About
$40 wasted.** Use `tr -d ' \r\t'` on parsed values or have the script
just call `az hdinsight resize --workernode-count 1` unconditionally on
exit.

---

## 9. Tear-down

```bash
az hdinsight delete -n $NAME -g $RG --yes
```

The storage account (containing your env tarball, dsdgen binary, output
parquet) is preserved by default — HDI only detaches it.

If your output is on HDFS (§3) and you didn't distcp to ABFS first, **the
data dies with the cluster.** This is the single biggest argument for
using ABFS output from the start.

---

## 10. Cross-references

- [`docs/hdinsight-quickstart.md`](hdinsight-quickstart.md) — the
  5-command flow that supersedes the manual recipe sections that used to
  live here.
- [`docs/spark-sizing-best-practices.md`](spark-sizing-best-practices.md)
  — calibrated wall-clock model for arbitrary SF.
- [`docs/notebooks-and-livy.md`](notebooks-and-livy.md) — Spark API
  usage from notebooks (Fabric, Databricks, Livy sessions).
- [`docs/live-test-status.md`](live-test-status.md) — current pass/fail
  matrix across submission paths.

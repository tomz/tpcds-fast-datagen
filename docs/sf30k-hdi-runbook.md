# SF=30K on HDInsight: end-to-end runbook

**Author:** Tom Zeng ([@tomz](https://github.com/tomz))
**Last updated:** 2026-04-19
**Status:** planned (not yet executed)

End-to-end procedure for generating TPC-DS at scale factor 30,000
(~10.8 TB Parquet, ~3.4 B rows of `store_sales` alone) on a 5-node
HDInsight Spark cluster, writing to HDFS on attached Standard HDDs.

Sized to finish in **~6–7 h with comfortable margin in a 12 h budget**.

---

## 1. Target

| | |
|---|---|
| Scale factor | **SF = 30,000** |
| Output | HDFS, replication = 2 |
| Output size | ~10.8 TB Parquet (one subdir per table) |
| Total rows | ~190 B (across 25 tables) |
| Wall-clock target | ~6–7 h |
| Wall-clock budget | 12 h |
| Total cost (list) | ~$110 (compute + storage for the run window) |

---

## 2. Cluster spec

| component | value | notes |
|---|---|---|
| Region | `eastus2` | within EADSv5 quota; close to ABFS account |
| HDI version | 5.1 | Spark 3.5 / Python 3.8 |
| Cluster type | Spark | |
| Worker SKU | **`Standard_E32ads_v5`** | 32 vCPU / 256 GB RAM / 1.2 TB local NVMe |
| Worker count | **5** | 160 task slots total |
| Headnode SKU | `Standard_E8ads_v5` | 8 vCPU / 64 GB; 2 nodes (HA) |
| Worker attached disks | **16× S30** (1 TiB Standard HDD each) | 16 TB usable / node |
| Disk type | `standard_lrs` | HDI-supported up to S30; up to 16 disks per worker |
| HDFS replication | **2** | survives one node loss; cheaper than rep=3 for one-shot run |

**Quota fit (East US 2):**
- Subscription EADSv5 family vCPUs: 5×32 + 2×8 = **176** (limit 300, ✓)
- HDI EADSv5 family cores: **176** (limit 200, ✓)

**Capacity sanity-check:**
- Output 10.8 TB × rep=2 = 21.6 TB / 5 nodes = **4.3 TB per node**
- 16× S30 = **16 TB usable per node** — 270% headroom
- HDFS rebalancing + Spark spill + logs — all easily covered

**Throughput sanity-check:**
- 21.6 TB written / 7 h ≈ 860 MB/s aggregate ≈ 172 MB/s per node sustained
- Burst peak ~690 MB/s/node (4× sustained)
- 16× S30 = 960 MB/s aggregate per node, capped at VM's 865 MB/s — clean fit

---

## 3. Pre-flight

### 3.1 Verify quota

```bash
SUB=c1f2cd4b-4e91-4a9f-9211-07df640f8610   # Fabric CAT Subscription
REGION=eastus2

# Subscription-level EADSv5 family
az vm list-usage --location $REGION -o table | grep -iE "EADSv5|Total Regional vCPUs"

# HDI-specific EADSv5 family
az rest --method get \
  --uri "https://management.azure.com/subscriptions/$SUB/providers/Microsoft.HDInsight/locations/$REGION/usages?api-version=2023-04-15-preview" \
  --query 'value[?contains(to_string(name.value), `Eads`) || name.value==`cores`].{name:name.localizedValue, used:currentValue, limit:limit}' \
  -o table
```

Need ≥ 176 free in **both** `Standard EADSv5 Family vCPUs` (sub) and
`StandardEadsV5Family` (HDI). Delete idle EADSv5 clusters first if short.

### 3.2 Storage account

Reuse an existing ABFS Gen2 account in the same region. The HDFS run
itself writes only to local disks, but you'll want ABFS for:
- the cluster's primary storage container (HDI requirement)
- staging `dsdgen` + `tpcds.idx` so workers can `addFile` them
- (optional) post-run distcp of the 10.8 TB Parquet output

```bash
# Confirm the account exists, in the right region
az storage account show -n <account> --query '{loc:primaryLocation, kind:kind, hns:isHnsEnabled}' -o table
# Must be `StorageV2` with `isHnsEnabled = true`.
```

### 3.3 Build dsdgen + tpcds.idx (on a glibc-compatible box)

The cluster's image glibc may not match your dev box. Easiest path: build
dsdgen *inside* the cluster after it's up, **OR** build on any Linux
box with glibc ≤ 2.34 and stage to ABFS.

```bash
git clone https://github.com/databricks/tpcds-kit
cd tpcds-kit/tools && make OS=LINUX dsdgen     # NOT plain `make` — qgen needs yacc

# Upload to wasbs (cluster's primary storage)
az storage blob upload --account-name <account> --container-name <container> \
  --file dsdgen --name livetest/dsdgen --overwrite
az storage blob upload --account-name <account> --container-name <container> \
  --file tpcds.idx --name livetest/tpcds.idx --overwrite
```

If glibc compatibility bites, see `docs/live-test-status.md` for the
on-cluster build pattern, or run `tpcds-gen install-dsdgen --from-source`
on one of the worker nodes to build against the cluster's glibc.

---

## 4. Provision the cluster

```bash
RG=tomz
NAME=tomz-spark-sf30k
REGION=eastus2
STORAGE_ACCT=<your-account>
STORAGE_CONTAINER=<your-container>
ADMIN_PW='<strong-password>'         # 10+ chars, mixed
SSH_PW='<different-strong-password>'

az hdinsight create \
  -g $RG -n $NAME \
  -t spark --version 5.1 --location $REGION \
  --workernode-count 5 \
  --workernode-size standard_e32ads_v5 \
  --workernode-data-disks-per-node 16 \
  --workernode-data-disk-size 1024 \
  --workernode-data-disk-storage-account-type standard_lrs \
  --headnode-size standard_e8ads_v5 \
  --storage-account $STORAGE_ACCT \
  --storage-container $STORAGE_CONTAINER \
  --http-user admin -p "$ADMIN_PW" \
  --ssh-user sshuser --ssh-password "$SSH_PW"
```

Provisioning takes **~15–25 min**. Watch:

```bash
az hdinsight show -n $NAME -g $RG --query 'properties.clusterState' -o tsv
# Accepted -> InProgress -> Running
```

Once `Running`, verify the disks landed correctly:

```bash
ssh sshuser@$NAME-ssh.azurehdinsight.net 'sudo -u hdfs hdfs dfsadmin -report | head -40'
```

Expected: 5 live datanodes, ~80 TB total HDFS capacity (5 nodes × 16 TB usable).

---

## 5. Stage the package + binary

The Spark variant (`tpcds_fast_datagen.spark.generate`) needs the wheel
on every executor's Python path **before** the first task spawns. HDI
5.1's image Python has known pyarrow issues (see `docs/live-test-status.md`)
— ship a venv tarball via `--archives`.

### 5.1 Build a portable venv on the cluster

SSH in and follow the venv-build pattern from [`docs/live-test-status.md`](live-test-status.md):

```bash
ssh sshuser@$NAME-ssh.azurehdinsight.net

# On the headnode:
PY=/usr/bin/miniforge/envs/py38/bin/python3
$PY -m pip install --no-deps --target /tmp/_venv_site \
    pyarrow==10.0.1
$PY -m pip install --no-deps --target /tmp/_venv_site \
    https://github.com/tomz/tpcds-fast-datagen/releases/download/v0.3.0/tpcds_fast_datagen-0.3.0-py3-none-any.whl

cd /tmp && tar czf tpcds_venv.tar.gz -C _venv_site .

# Upload to wasbs
hdfs dfs -put -f tpcds_venv.tar.gz wasbs:///livetest/tpcds_venv.tar.gz
```

Why pyarrow 10.0.1: last release linking against `GLIBCXX_3.4.25`,
which is what HDI 5.1's image provides. Newer pyarrow needs `3.4.26`
and crashes on import.

### 5.2 Stage dsdgen + idx (already done in §3.3 if you uploaded then)

```bash
hdfs dfs -ls wasbs:///livetest/
# Should show: dsdgen, tpcds.idx, tpcds_venv.tar.gz
```

---

## 6. Run

### 6.1 Submit command (HDFS output)

From the HDI headnode:

```bash
ARCH=wasbs:///livetest/tpcds_venv.tar.gz
DSDGEN=wasbs:///livetest/dsdgen
IDX=wasbs:///livetest/tpcds.idx

spark-submit \
  --master yarn --deploy-mode client \
  --num-executors 5 \
  --executor-cores 32 \
  --executor-memory 192g \
  --conf spark.network.timeout=1800s \
  --conf spark.task.maxFailures=8 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.speculation=true \
  --conf spark.speculation.quantile=0.85 \
  --conf spark.speculation.multiplier=1.4 \
  --conf spark.hadoop.dfs.replication=2 \
  --conf spark.executorEnv.PYTHONPATH=./venv \
  --conf spark.yarn.appMasterEnv.PYTHONPATH=./venv \
  --archives "$ARCH#venv" \
  --files dsdgen,tpcds.idx \
  hdi_livy_v2.py \
  30000 \
  hdfs:///tpcds/sf30000 \
  sf30k-prod \
  $DSDGEN $IDX \
  wasbs:///livetest/sf30k-run.log
```

### 6.2 Why each flag matters

| flag | reason |
|---|---|
| `--num-executors 5` | one executor per worker node — max per-task NVMe + memory bandwidth |
| `--executor-cores 32` | fully use each worker; 5 × 32 = 160 task slots |
| `--executor-memory 192g` | leaves ~64 GB for OS / YARN overhead per node |
| `spark.network.timeout=1800s` | absorbs ABFS auth refreshes and heartbeat blips on 6 h+ runs |
| `spark.task.maxFailures=8` | retry tail-task hiccups instead of failing the stage |
| `spark.dynamicAllocation.enabled=false` | want fixed 5-executor fleet from t=0; no ramp-up cost |
| `spark.speculation=true` (+85%/1.4×) | re-launches stuck tail tasks; recovers ~15–30 min of tail |
| `spark.hadoop.dfs.replication=2` | survives one node loss; saves 33% disk vs default rep=3 |
| `--archives venv.tar.gz#venv` + `PYTHONPATH=./venv` | ships pyarrow 10.0.1 + wheel to executors before any Python worker spawns (bypasses `addPyFile` race) |
| `--files dsdgen,tpcds.idx` | distributes the binary + index to every executor's `SparkFiles` dir |

### 6.3 Generator parameters

| arg | value | reason |
|---|---|---|
| scale | **30,000** | target SF |
| output | `hdfs:///tpcds/sf30000` | writes to attached-disk HDFS, not ABFS |
| `--chunks` (in script) | **2,400** | chunks/slots = 15× (above tail-drag); rows/chunk ≈ 23.8M ≈ 5.2 GB .dat — fits NVMe scratch easily; under SF=30K inventory floor (~2,610) |

The script's auto-chunks formula is conservative for big SFs; pass
explicitly.

---

## 7. Monitoring

In one terminal, watch the Spark UI:

```bash
# Tunnel the Spark UI to your laptop:
ssh -L 8088:headnodehost:8088 sshuser@$NAME-ssh.azurehdinsight.net
# then open http://localhost:8088
```

Key things to watch:
- **Stages tab:** the `mapPartitions(run_dsdgen_task)` stage should show 2,400 tasks; aim for ≥ 80% running concurrently
- **Executors tab:** all 5 executors active; per-executor task time should be uniform (variance > 2× = stragglers, speculation kicks in)
- **Storage tab:** no large RDD caching — this job is purely streaming
- **Environment tab:** confirm `spark.executorEnv.PYTHONPATH=./venv` is set

In another terminal, watch HDFS fill up:

```bash
ssh sshuser@$NAME-ssh.azurehdinsight.net \
  'while true; do
    sudo -u hdfs hdfs dfs -du -s -h /tpcds/sf30000 2>/dev/null
    sudo -u hdfs hdfs dfsadmin -report | grep -E "DFS Used|Remaining" | head -2
    sleep 300
   done'
```

Expected fill rate: ~30 GB/min on average across the cluster (10.8 TB
output × rep=2 / 360 min ≈ 60 GB/min HDFS-side; per-node ~12 GB/min).

If fill rate stalls > 10 min, check the Spark UI for stragglers.

---

## 8. Verification

After the job completes, validate row counts before tearing down:

```bash
spark-submit \
  --master yarn --deploy-mode client \
  --num-executors 5 --executor-cores 32 --executor-memory 192g \
  --conf spark.executorEnv.PYTHONPATH=./venv \
  --archives "$ARCH#venv" \
  count_tpcds.py hdfs:///tpcds/sf30000
```

Expected total rows for SF=30,000:

| table | rows |
|---|---:|
| store_sales | ~86.4 B |
| catalog_sales | ~43.2 B |
| web_sales | ~21.6 B |
| inventory | ~3.94 B |
| (others) | summing to ~190 B grand total |

The script reports per-table row counts in its final log line:

```
=== HDI-LIVY-V2 sf30k-prod RESULT ===
label=sf30k-prod scale=30000 output=hdfs:///tpcds/sf30000
elapsed_s=23847.3
num_tasks=2403 num_chunks=2400
total_rows=190123456789 succeeded=True
```

(Exact total depends on TPC-DS spec version; check `docs/architecture.md`
schema definitions.)

---

## 9. Copy off-cluster (optional)

If you want to retain the 10.8 TB output past cluster lifetime, distcp
to ABFS before deleting:

```bash
hadoop distcp -m 80 \
  -strategy dynamic \
  hdfs:///tpcds/sf30000 \
  abfs://<container>@<account>.dfs.core.windows.net/tpcds/sf30000
```

Budget: **~1.5–2 h** at this size, ~$25–50 in Storage egress depending
on tier. The cluster compute keeps ticking during distcp, so factor
that in if you're paying by the hour.

Skip distcp and use `--output abfs:///...` from the start if you don't
need the HDFS-resident copy. Adds ~10–15% to wall-clock vs HDFS but
removes the copy step entirely.

---

## 10. Tear-down

```bash
az hdinsight delete -n $NAME -g $RG --yes
```

The storage account (containing your `dsdgen`, `venv`, logs, optional
distcp output) is preserved by default — HDI only detaches it.

If you copied output to ABFS in §9, you can delete the cluster
immediately. If you didn't, **the HDFS data dies with the cluster**.

---

## 11. Cost breakdown (list price, East US 2)

| component | unit rate | quantity | duration | cost |
|---|---:|---:|---:|---:|
| E32ads_v5 worker | $1.92/h | 5 | 7 h | $67 |
| E8ads_v5 head | $0.48/h | 2 | 7 h | $7 |
| S30 Standard HDD | $40/disk-month | 80 (16×5) | 7 h | $30 |
| Storage account (existing, ABFS) | included | — | — | ~$0 marginal |
| **Cluster total** | | | | **~$104** |

Optional add-ons:
- Distcp HDFS → ABFS: 5×$1.92 × 1.5 h ≈ $15 compute + ~$30 ABFS ingress = ~$45
- Holding 10.8 TB Parquet on ABFS Hot for 1 month: ~$220
- Holding it on ABFS Cool: ~$110

**Pre-distcp run cost: ~$104.** Add ~$45 if you copy off-cluster.

---

## 12. Failure modes and recovery

| symptom | likely cause | fix |
|---|---|---|
| `ModuleNotFoundError: tpcds_fast_datagen` on executors | venv archive not unpacked, or `PYTHONPATH=./venv` not set | verify both `--archives` and `spark.executorEnv.PYTHONPATH` are present |
| `ImportError: ... GLIBCXX_3.4.26 not found` | wrong pyarrow version in venv | rebuild venv with `pyarrow==10.0.1` |
| `dsdgen: GLIBC_2.34 not found` | pre-built dsdgen used newer glibc than HDI image | build dsdgen on the cluster (see live-test-status.md) |
| Tasks stuck > 30 min on one node | slow HDD or noisy neighbor | speculation should kick in at 85% completion; otherwise restart that executor |
| HDFS `DataNode disk full` | one disk filled before HDFS rebalanced | shouldn't happen with 270% headroom; if it does, manually rebalance or restart datanode |
| ABFS auth token expired mid-run | service principal token TTL < run wall-clock | use long-lived MSI on the cluster, or pre-refresh before submit |
| Job hung at "Generating xxx" with no progress | dsdgen child crashed silently | check executor logs for that task; usually a stale `tpcds.idx` mismatch |

---

## 13. Actual results (2026-04-19/20 run)

### 13.1 Timing summary

| Run | Output | Wall-clock | TOT written | Throughput | Outcome |
|---|---|---|---|---|---|
| **SF=30K v1** (HDFS, chunks=2400, replication=2 attempted) | `hdfs:///tpcds/sf30000` | ~5 h before kill | 1.5 TB | ~0.3 TB/h | ❌ killed — int32 overflow on `ss_ticket_number` (`2307000001 > 2^31`) |
| **SF=10K validation** (ABFS, chunks=4800, schema fix v0.3.1) | `wasbs://.../tpcds/sf10000` | **~1 h 41 min** | 3.95 TB | ~2.4 TB/h | ✅ SUCCEEDED |
| **SF=30K v2** (ABFS, chunks=4800, schema fix v0.3.1) | `wasbs://.../tpcds/sf30000` | **~3 h 10 min** | 11.847 TB | ~3.7 TB/h | ✅ SUCCEEDED |

### 13.2 Per-table breakdown (SF=30K v2)

| Table | Chunks | Size | Stage 3 task pace |
|---|---|---|---|
| store_sales | 4800 | 4.853 TB | ~125 sec/task |
| catalog_sales | 4800 | 3.910 TB | ~127 sec/task |
| web_sales | 4800 | 1.878 TB | ~73 sec/task |
| dimensions + small fact tables | ~213 | < 50 GB total | ~12 sec/task each |
| **Total** | 24,613 | **11.847 TB** | — |

### 13.3 Bugs caught and fixed

1. **`ss_ticket_number` / `*_order_number` int32 overflow** at SF ≥ ~10K. Ticket and order numbers monotonically increase across chunks and exceed `2^31 = 2,147,483,648` mid-run. Fixed in `tpcds_fast_datagen.schema` v0.3.1 by changing the following columns from `_int` (int32) to `_bigint` (int64):
   - `ss_ticket_number`, `sr_ticket_number`
   - `cs_order_number`, `cr_order_number`
   - `ws_order_number`, `wr_order_number`

2. **HDFS scratch fills `/tmp` on the OS disk** (124 GB) when `tmp_root` is unset. Workers' `/dev/sda1` filled with `/tmp/tpcds_*` chunks, NodeManagers crashed. Fixed by passing `tmp_root="/mnt/tmp"` to `generate()` (1.2 TB NVMe per worker, must be `chmod 1777`-prepped first).

3. **HDFS only saw `/mnt/resource` after provision** — needed Ambari reconfig of `dfs.datanode.data.dir` to add 16 × `/data_disk_*`. Ambari REST kept returning empty 500s; `configs.py` (Python 2 only — use `/usr/bin/python`) worked. HDFS service restart from Ambari Web UI was required to pick up the new dirs.

### 13.4 Knobs that mattered

Going from the v1 config (HDFS, chunks=2400, replication=2) to v2 (ABFS, chunks=4800, replication=1) gave a **~12× throughput improvement**:

- **HDFS → ABFS:** removed datanode write-amp + sync overhead. ABFS scales to ~60 Gbps egress on a standard storage account; we saw zero `ServerBusy` / `EgressIsOverAccountLimit` / `503` events at peak ~3 TB/h.
- **chunks=2400 → 4800:** halved per-task runtime (18 min → 60–125 sec depending on table), dramatically improved load balancing and shortened the failure-recovery tail.
- **`spark.task.maxFailures=8 → 4`:** surfaced the int32 bug in attempt 4 instead of attempt 8. Worth ~1 h of wasted compute on a deterministic-failure run.

### 13.5 Cost (actual)

5 × E32ads_v5 + 2 × E8ads_v5 head ≈ $9.84/h.
- v1 attempt: ~5 h × $9.84 = **$49** (wasted, killed on int32 bug)
- SF=10K validation: 1.7 h × $9.84 = **$17**
- SF=30K v2: 3.2 h × $9.84 = **$31**
- Plus ~1 h provisioning + ~5 h post-completion idle (scaler bug, see §13.6) = ~$60
- HDD costs (16×5 = 80 × S30): negligible at hourly proration
- ABFS storage (11.85 TB Hot, 1 month): ~$245 if held; ~$0 if used and deleted same day
- **Total cluster compute: ~$160** (would have been ~$100 without the scaler bug + v1 wasted run)

### 13.6 Operational gotcha: auto-scaler bug

The post-run scale-down script (`scale_down_when_done.sh`) polled `yarn application -status` every 2 min and was supposed to break on `Final-State=SUCCEEDED`, then issue `az hdinsight resize --workernode-count 1`. It correctly detected SUCCEEDED but its string comparison failed (awk `-F:` left trailing whitespace in `$STATE`, so `[[ "$STATE" == "SUCCEEDED" ]]` never matched). The cluster sat idle at 5-worker size for ~5 h after job completion before manual intervention — about **$40 wasted**. For next run: use `tr -d ' \r\t'` on the parsed value, or parse via `yarn application -status | awk -F: '/Final-State/ {gsub(/ /,"",$2); print $2}'`.

---

## 14. What to record after the run

For the project's `docs/live-test-status.md` and CHANGELOG:

- [ ] Actual wall-clock (vs ~7 h estimate)
- [ ] Total rows (must match expected)
- [ ] Per-table generation times (from script logs)
- [ ] Peak HDFS used (`hdfs dfsadmin -report`)
- [ ] Speculation re-launches count (Spark UI)
- [ ] Any failed tasks and their errors
- [ ] Total cost (Azure cost-management export, prorated)

---

## 15. References

- [`docs/spark-sizing-best-practices.md`](spark-sizing-best-practices.md) — calibrated wall-clock model
- [`docs/live-test-status.md`](live-test-status.md) — HDI Livy / Fabric SJD workarounds, dsdgen + venv bootstrap
- [`docs/notebooks-and-livy.md`](notebooks-and-livy.md) — Spark API usage
- `hdi_build_venv.py` / `hdi_build_dsdgen.py` — developer-side helpers that live
  outside the repo (in `/tmp/tpcds-live-tests/` on the author's box) used to
  build the portable venv tarball and an on-cluster `dsdgen` for HDI. Not
  required to use this package — `tpcds-gen install-dsdgen` now covers the
  common case; see [`docs/live-test-status.md`](live-test-status.md) for the
  exact steps they implement.

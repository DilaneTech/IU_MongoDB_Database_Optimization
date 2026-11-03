# Optimization of a Local Database — MongoDB

A financial transactions dataset (~13.3M rows) is imported into **MongoDB 8.2**. Source: https://www.kaggle.com/code/ibrahimragabrashaad/financial-transactions-dataset-analytics/. 
The newest 20% of the data is then replayed as concurrent inserts alongside continuous reads, and the effect of three
optimizations is measured:

1. **Schema optimization**: native types, `Decimal128`, natural keys as `_id`, empty fields omitted.
2. **Indexes**: 4 indexes targeted at the workload's queries, with `explain()` before/after.
3. **Replication**: two copies of the database (replica set) in a constrained environment.

Every claim is backed by before/after measurements recorded in `results/`. The final deliverable is
an English PowerPoint deck (`presentation/IU_NoSQL_MongoDB_Optimization.pptx`).

---

## Contents

- [Repository layout](#repository-layout)
- [Dataset](#dataset)
- [Requirements and setup](#requirements-and-setup)
- [Usage: local environment (on-premises)](#usage-local-environment-on-premises)
- [Usage: constrained environment (Docker)](#usage-constrained-environment-docker)
- [Code architecture](#code-architecture)
- [Measurement protocol](#measurement-protocol)
- [Key results](#key-results)
- [Presentation](#presentation)
- [Restoring the host](#restoring-the-host)

---

## Repository layout

```
data/                 Dataset (CSV + JSON), not baked into the Docker images (not present in this repo but can be downloaded using the above link from Kaggle)
docs/                 Assignment assignment (Assignment.docx)
src/
  config.py           Paths, Mongo URI/DB, 80/20 split point
  schemas.py          One adapter per schema variant (conversion + queries + indexes)
  import_data.py      Import of the oldest 80%
  create_indexes.py   Index build + explain() before/after
  workload.py         Concurrent write + read workload (closed or open loop)
  capacity_model.py   Scale-up projection
scripts/
  docker_sweep.sh     Load + memory sweep (6g / 3g / 1.5g)
  docker_run.sh       One constraint level (memory, CPUs), closed or open loop
  docker_rs.sh        Two-member replica set (setup + benchmark runs)
results/              All raw results (JSON + docker stats samples as CSV)
Dockerfile            Python image for the program (dependencies only)
docker-compose.yml    Services mongo, app, and mongo1/mongo2 (rs profile)
requirements.txt      pymongo, pandas, matplotlib
```

## Dataset

| File | Rows | Key | Links |
|---|---|---|---|
| `users_data.csv` | ~2,000 | `id` | — |
| `cards_data.csv` | ~6,100 | `id` | `client_id` → users.id |
| `transactions_data.csv` | ~13.3M (1.26 GB) | `id` | `client_id`, `card_id`, `mcc` |
| `mcc_codes.json` | ~100 | MCC code (string key) | flat `{code: description}` object |

Quirks handled at import (optimized schema): money values prefixed with `$` and sometimes negative
(`$-77.00`), `zip` as a float string and sometimes empty, `errors` empty or a comma-separated list,
dates as `YYYY-MM-DD HH:MM:SS` vs `MM/YYYY`, booleans as `YES/NO` and `Yes/No`, MCC codes as strings
on one side and integers on the other.

**Chronological 80/20 split**: `transactions_data.csv` is already sorted by date. The first
10,644,732 rows are imported; the newest 2,661,183 are held back for the live write workload.

## Requirements and setup

- Windows, **Python 3.12**
- **MongoDB 8.2** installed as a Windows service (`MongoDB`, port 27017) for local measurements
- **Docker Desktop** for the constrained environment and the replica set
- **Node.js** to build the deck (`pptxgenjs` dependency)

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt
```

The database is `DatabaseOptimization`. Run every command from the repository root.

## Usage: local environment (on-premises)

```bash
# 1. Import (drop + import of the oldest 80%, ~200 s)                  -> results/import_<schema>.json
.venv/Scripts/python src/import_data.py --schema baseline
.venv/Scripts/python src/import_data.py --schema optimized

# 2. Indexes: reset, explain before, build, explain after              -> results/indexes_<schema>.json
.venv/Scripts/python src/create_indexes.py --schema baseline
.venv/Scripts/python src/create_indexes.py --schema optimized

# 3. 120 s workload                                                    -> results/workload_<label>.json
.venv/Scripts/python src/workload.py --schema baseline --label baseline
.venv/Scripts/python src/workload.py --schema baseline --label smoke --duration 10   # quick check
```

`workload.py` options:

| Option | Default | Purpose |
|---|---|---|
| `--schema` | `baseline` | `baseline`, `optimized`, `baseline_indexed`, `optimized_indexed` |
| `--label` | (required) | name of the result file |
| `--writers` / `--readers` | 2 / 4 | number of processes (`--readers 0` = write-only) |
| `--duration` | 120 | duration in seconds |
| `--uri` | `MONGO_URI` | MongoDB URI (e.g. a replica set) |
| `--read-pref` | `primary` | `primary`, `secondaryPreferred`, `nearest`, … |
| `--write-concern` | server default | `1` or `majority` |
| `--read-rate` / `--write-rate` | — | **open-loop** mode: fixed demand in reads/s and docs/s |

## Usage: constrained environment (Docker)

The assignment asks to run the program in an environment with at least 20% less memory, then to tighten
the constraints until requests take more than a second. **Both MongoDB and the Python program run
in containers**:

- `mongo`: `mongo:8.2` image, `MONGO_MEM` memory (no swap), `MONGO_CPUS` CPUs (default 4),
  host port 27027.
- `app`: Python image fixed at **2 GB / 2 CPUs**, so slowdowns can be attributed to the database
  rather than the client (checked with the `docker stats` samples).

The 4 configurations of the 2×2 (schema × index) live side by side in separate collections
(`transactions`, `transactions_opt`, `transactions_idx`, `transactions_opt_idx`), so indexes never
need rebuilding between runs.

```bash
# Load the 4 configurations at 6g, then sweep 6g / 3g / 1.5g
bash scripts/docker_sweep.sh
SKIP_LOAD=1 bash scripts/docker_sweep.sh                 # sweep only

# One constraint level: <memory> <cpus> <configs...>
bash scripts/docker_run.sh 1g 2 baseline_indexed optimized_indexed
READ_RATE=300 WRITE_RATE=2000 bash scripts/docker_run.sh 1g 2 optimized_indexed   # open loop
RUN_SUFFIX=_r2 bash scripts/docker_run.sh 1g 4 optimized_indexed                   # repeat run

# Two copies (replica set rs0: mongo1 primary, mongo2 secondary, 1g / 2 CPUs each)
bash scripts/docker_rs.sh setup                          # stop mongo, initiate rs0, load optimized_indexed
bash scripts/docker_rs.sh runs                           # primary / secondary / secondary + majority
READ_RATE=500 WRITE_RATE=200 bash scripts/docker_rs.sh readheavy   # primary / nearest
```

Inside the `app` container, `RESULT_PREFIX=docker_` keeps local results from being overwritten.
Labels follow the format `docker_<mem>[_<cpus>cpu][_open<R>r<W>w]_<config>`; each run produces
`results/workload_<label>.json` and `results/dockerstats_<label>.csv`.

"Unbearable" is defined as a **read p95 > 1 s**. Reads time out after 60 s and are counted as
unserved.

## Code architecture

**Schema adapters (`src/schemas.py`).** The workload is schema-agnostic. Each variant is a class
registered in `SCHEMAS` that defines:

- `collections`: collection names;
- `to_doc`, `user_doc`, `card_doc`, `mcc_doc`: conversion of a CSV row into a document;
- `live_filter(last_imported_id)`: selects the documents inserted by the workload (used for the reset);
- the 4 read queries and the `INDEXES` list.

| Variant | Description |
|---|---|
| `Baseline` | raw import: every value stays a string, only the `_id` index exists |
| `Optimized` | native types, `Decimal128` for money, natural key as `_id`, empty fields omitted, references (no embedding); collections suffixed `_opt` |
| `BaselineIndexed` / `OptimizedIndexed` | same schemas in their own collection, with the indexes |

To test a new schema, add an adapter; `workload.py` does not change.

**Workload queries** (readers pick at random with a fixed seed):

| Query | Description | Index |
|---|---|---|
| `client_history` | a client's 20 most recent transactions | `client_date` `{client_id: 1, date: -1}` |
| `card_transactions` | 50 transactions for a card | `card_date` `{card_id: 1, date: -1}` |
| `spend_per_mcc` | total spend per MCC category over 30 days | `mcc_date_amount` `{mcc, date, amount}` (ESR rule, covered query) |
| `errors_on_day` | transactions with errors on a given day | `date_errors_partial` `{date: 1}` partial (~1.6% of docs) |

**Writers.** They seek straight to the held-back rows using the byte offset cached in
`results/split_point.json` and insert batches of 100 in chronological order.

**`create_indexes.py`** runs `explain()` through the adapters' own query methods (using a recording
stand-in collection), so the explained plans are exactly those of the workload.

## Measurement protocol

Fixed parameters for every configuration: **2 writers, 4 readers, batches of 100, 120 s**.

1. **Reset**: delete documents whose id is beyond `last_imported_id` (back to the post-import state).
2. `fsync` + 30 s settle.
3. **Warm-up**: full collection scan plus a hinted scan of each index. Both schemas together
   (~7.7 GB of data + indexes) exceed the 7.4 GB WiredTiger cache; without warm-up, run order would
   bias the results.
4. The cache state is recorded in the result.

Two modes:

- **Closed loop** (default): each reader issues queries back to back. Measures **capacity**.
- **Open loop** (`--read-rate`, `--write-rate`): requests are issued on a fixed schedule and latency
  is measured from the scheduled time. Measures **user experience** and avoids coordinated omission.

Write throughput uses the writers' real elapsed time, as they often finish all 2.66M documents
before the 120 s run ends. Run-to-run variance is about ±15% on writes, so small gaps are not
significant.

## Key results

### On-premises (Windows service, 7.4 GB WiredTiger cache)

| Configuration | Reads/s | Read p95 (worst query) | Writes (docs/s) |
|---|---:|---:|---:|
| baseline (raw, no index) | 0.23 | 29.1 s | 56,167 |
| optimized schema, no index | 0.21 | 26.9 s | 24,190 |
| baseline + indexes | 1,225 | 33 ms | 20,784 |
| optimized + indexes | 1,406 | 20 ms | 19,538 |

- **Indexes**: `explain()` goes from a `COLLSCAN` over 10.6M documents (~17–26 s) to an `IXSCAN`
  over 20–14,045 keys (1–127 ms); `spend_per_mcc` becomes a covered query (0 documents read).
- **Cost of indexes** (write-only): ~48k → ~18k docs/s; ~90–110 s to build each index.
- **Optimized schema**: documents shrink from 275 to 195 bytes (−29%), `client_date`/`card_date`
  indexes from ~226 to ~135 MB, exact money values with `Decimal128`.

### Constrained environment (Docker)

- Without indexes: 20–33 s per read at every memory level.
- With indexes: < 65 ms down to 1 GB. At 512 MB, MongoDB is OOM-killed during the reset.
- Open loop at 1 GB, 300 reads/s + 2,000 docs/s: 4 CPUs → p95 ~20 ms (optimized) / ~29 ms
  (baseline); **2 CPUs → breaking point** (optimized p95 3 s, baseline p95 16 s); 1 CPU → collapse.

### Two copies (replica set, 1 GB / 2 CPUs per member)

- **Write-heavy demand** (300 r/s + 2,000 w/s): copies don't help, since every member replays every
  write; reading from the secondary saturates it (lag up to 91 s).
- **Read-heavy demand** (500 r/s + 200 w/s): single node p95 650 ms; two copies with `nearest` →
  **p95 23 ms**, all writes served, lag ≤ 1 s.
- Takeaway: replication scales reads and availability, not writes; scaling writes requires sharding.

### Scale-up planning

`src/capacity_model.py` computes the cost per transaction (data, disk, indexes) and the yearly
growth (~1.39M transactions/year), then projects when the indexes will stop fitting in each
environment's cache, for growth factors of ×1, ×10 and ×100 (`results/capacity_model.json`).

## Presentation

```bash
.venv/Scripts/python src/capacity_model.py          # -> results/capacity_model.json
.venv/Scripts/python presentation/build_data.py     # results/ -> presentation/deck_data.json
PPTX_SKILL_DIR=<pptx skill folder> NODE_PATH=presentation/node_modules \
  node presentation/build_deck.js                   # -> IU_NoSQL_MongoDB_Optimization.pptx
```

Every number on the slides is read from `deck_data.json`: rerun `build_data.py` after any new
benchmark, and never hand-type a figure into `build_deck.js`. The deck has 17 main slides and
5 appendix slides (A–E), with speaker notes.

## Restoring the host

Host changes made for the Docker measurements, to revert once the project is done:

- `C:\Users\<user>\.wslconfig`: remove `memory=10GB`;
- Windows `MongoDB` service stopped: restart it with `Start-Service MongoDB` (requires admin rights);
- Docker volumes `mongo-data`, `rs1-data`, `rs2-data`: remove them with `docker compose --profile rs down -v`.

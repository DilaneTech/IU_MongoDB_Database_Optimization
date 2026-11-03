"""High-pace read/write workload against the transactions collection.

Writers replay the held-back 20% of transactions (chronological, batches of
100); readers run a random mix of four queries at the same time. The DB is
reset to its post-import state before each run.

Two load models:
  closed loop (default): each worker sends its next request as soon as the
    previous one returns -> measures maximum capacity, but latency stays
    bounded because the load slows down with the server.
  open loop (--read-rate / --write-rate): requests are issued on a fixed
    schedule regardless of how the server keeps up, and latency is measured
    from the scheduled time, so queueing delay is included (no "coordinated
    omission") -> measures what users experience under a fixed demand.

Usage:
  .venv/Scripts/python src/workload.py --schema baseline --label baseline
  .venv/Scripts/python src/workload.py --schema optimized_indexed --label x --read-rate 300 --write-rate 2000
"""
import argparse
import csv
import json
import multiprocessing as mp
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from itertools import islice

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import (CARDS_CSV, DB_NAME, MCC_JSON, MONGO_URI, RESULTS_DIR,
                    SPLIT_ROW, TRANSACTIONS_CSV, USERS_CSV)
from schemas import SCHEMAS

WRITE_BATCH = 100
SETTLE_SECONDS = 30
# A query slower than this is aborted and counted as a timeout (its latency is
# recorded as the timeout, so percentiles stay conservative).
READ_TIMEOUT_MS = 60_000
# Open loop: threads per reader process, i.e. how many of its requests can be
# in flight at once; further requests wait in the executor queue (and that
# wait counts in their latency).
READER_THREADS = 16
SEED = 42
READ_OPS = ("client_history", "card_transactions", "spend_per_mcc", "errors_on_day")
# Imported transactions span 2010-01-01 .. 2017-12-04.
FIRST_DAY, LAST_DAY = datetime(2010, 1, 1), datetime(2017, 11, 30)


def split_point():
    """Byte offset of the first held-back row and id of the last imported row (cached)."""
    cache = RESULTS_DIR / "split_point.json"
    if cache.exists():
        return json.loads(cache.read_text())
    with open(TRANSACTIONS_CSV, "rb") as f:
        f.readline()  # header
        for _ in range(SPLIT_ROW - 1):
            f.readline()
        last_id = int(f.readline().split(b",", 1)[0])
        point = {"offset": f.tell(), "last_imported_id": last_id}
    RESULTS_DIR.mkdir(exist_ok=True)
    cache.write_text(json.dumps(point))
    return point


def warm_up(coll):
    """Load the collection and each of its indexes into the WiredTiger cache.

    Both schemas don't fit in the cache together, so without this a run would
    start cold or warm depending on which schema ran before."""
    count_all = [{"$group": {"_id": None, "n": {"$sum": 1}}}]
    t = time.perf_counter()
    list(coll.aggregate(count_all, hint={"$natural": 1}))  # full collection scan
    for name, info in coll.index_information().items():
        # a partial index can only be hinted by a query matching its filter
        match = [{"$match": info["partialFilterExpression"]}] if "partialFilterExpression" in info else []
        list(coll.aggregate(match + count_all, hint=name))
    print(f"  warm-up done in {time.perf_counter() - t:.0f}s")


def read_ids(path, column):
    with open(path, newline="", encoding="utf-8") as f:
        return [int(row[column]) for row in csv.DictReader(f)]


def writer(idx, n_writers, schema_name, uri, offset, start_evt, duration, out,
           batch_interval=None, w=None):
    """batch_interval (open loop): seconds between this writer's batches.
    w: write concern (1 or "majority"); None keeps the server default."""
    schema = SCHEMAS[schema_name]
    coll = MongoClient(uri, **({"w": w} if w is not None else {}))[DB_NAME][schema.collection]
    with open(TRANSACTIONS_CSV, newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
        f.seek(offset)
        rows = csv.DictReader(f, fieldnames=header)
        start_evt.wait()
        t0 = time.perf_counter()
        deadline = t0 + duration
        latencies, docs, k, j = [], 0, 0, 0
        while time.perf_counter() < deadline:
            batch = list(islice(rows, WRITE_BATCH))
            if not batch:
                break
            if k % n_writers == idx:  # interleave batches across writers
                docs_batch = [schema.to_doc(r) for r in batch]
                t = time.perf_counter()
                if batch_interval:
                    t = t0 + j * batch_interval  # scheduled time
                    if t >= deadline:
                        break
                    time.sleep(max(0.0, t - time.perf_counter()))
                coll.insert_many(docs_batch, ordered=False)
                latencies.append((time.perf_counter() - t) * 1000)
                docs += len(batch)
                j += 1
            k += 1
    out.put({"role": "writer", "latencies": latencies, "docs": docs,
             "elapsed": time.perf_counter() - t0})


def next_query(rng, params):
    """Pick a random query and its parameters."""
    days = (LAST_DAY - FIRST_DAY).days
    op = rng.choice(READ_OPS)
    if op == "client_history":
        return op, (rng.choice(params["clients"]),)
    if op == "card_transactions":
        return op, (rng.choice(params["cards"]),)
    if op == "spend_per_mcc":
        return op, (rng.choice(params["mccs"]), FIRST_DAY + timedelta(days=rng.randrange(days)))
    return op, (FIRST_DAY + timedelta(days=rng.randrange(days)),)


def reader(idx, schema_name, uri, read_pref, params, start_evt, duration, out,
           interval=None):
    """interval (open loop): seconds between this reader's requests."""
    schema = SCHEMAS[schema_name]
    client = MongoClient(uri, readPreference=read_pref, timeoutMS=READ_TIMEOUT_MS)
    coll = client[DB_NAME][schema.collection]
    rng = random.Random(SEED + idx)
    latencies = {op: [] for op in READ_OPS}
    timeouts = {op: 0 for op in READ_OPS}
    unserved = {op: 0 for op in READ_OPS}

    def run(op, args, t):
        """Execute one query; latency counts from t (send or scheduled time)."""
        try:
            getattr(schema, op)(coll, *args)
            latencies[op].append((time.perf_counter() - t) * 1000)
        except PyMongoError as exc:
            if not exc.timeout:
                raise
            timeouts[op] += 1
            latencies[op].append(READ_TIMEOUT_MS)

    start_evt.wait()
    t0 = time.perf_counter()
    deadline = t0 + duration
    if interval is None:
        while time.perf_counter() < deadline:
            op, args = next_query(rng, params)
            run(op, args, time.perf_counter())
    else:
        pool = ThreadPoolExecutor(READER_THREADS)
        submitted, i = [], 0
        while (scheduled := t0 + i * interval) < deadline:
            time.sleep(max(0.0, scheduled - time.perf_counter()))
            op, args = next_query(rng, params)
            submitted.append((pool.submit(run, op, args, scheduled), op, scheduled))
            i += 1
        # Requests still queued at the deadline were never served: drop them,
        # but record how long they had already waited (a lower bound).
        pool.shutdown(wait=True, cancel_futures=True)
        for fut, op, scheduled in submitted:
            if fut.cancelled():
                unserved[op] += 1
                latencies[op].append((deadline - scheduled) * 1000)
            else:
                fut.result()  # re-raise unexpected errors
    out.put({"role": "reader", "latencies": latencies, "timeouts": timeouts,
             "unserved": unserved})


def sample_replication_lag(admin, stop, lags):
    """Every 5 s, record how many seconds each secondary lags the primary."""
    while not stop.wait(5):
        members = admin.command("replSetGetStatus")["members"]
        primary = next(m for m in members if m["stateStr"] == "PRIMARY")
        lags += [(primary["optimeDate"] - m["optimeDate"]).total_seconds()
                 for m in members if m["stateStr"] == "SECONDARY"]


def percentiles(values):
    if not values:
        return {"count": 0}
    q = statistics.quantiles(values, n=100, method="inclusive") if len(values) > 1 else values * 99
    return {"count": len(values), "mean_ms": round(statistics.fmean(values), 2),
            "p50_ms": round(q[49], 2), "p95_ms": round(q[94], 2), "p99_ms": round(q[98], 2)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="baseline", choices=SCHEMAS)
    ap.add_argument("--label", required=True, help="name of the result file")
    ap.add_argument("--uri", default=MONGO_URI)
    ap.add_argument("--read-pref", default="primary")
    ap.add_argument("--writers", type=int, default=2)
    ap.add_argument("--readers", type=int, default=4)
    ap.add_argument("--duration", type=int, default=120)
    ap.add_argument("--read-rate", type=float, help="open loop: total reads/s")
    ap.add_argument("--write-rate", type=float, help="open loop: total docs/s")
    ap.add_argument("--write-concern", choices=["1", "majority"],
                    help="writers' write concern (default: server default, which is "
                         "majority on a replica set)")
    args = ap.parse_args()
    w = int(args.write_concern) if args.write_concern == "1" else args.write_concern
    batch_interval = args.writers * WRITE_BATCH / args.write_rate if args.write_rate else None
    read_interval = args.readers / args.read_rate if args.read_rate else None

    schema = SCHEMAS[args.schema]
    point = split_point()
    coll = MongoClient(args.uri)[DB_NAME][schema.collection]
    print("Resetting collection to post-import state...")
    removed = coll.delete_many(schema.live_filter(point["last_imported_id"])).deleted_count
    print(f"  removed {removed:,} docs from previous runs; {coll.estimated_document_count():,} docs")
    # A large delete leaves WiredTiger flushing and evicting in the background:
    # force a checkpoint and let the server settle so it doesn't bias the run.
    admin = MongoClient(args.uri).admin
    admin.command("fsync")
    time.sleep(SETTLE_SECONDS)
    # Warm the member(s) the readers will hit: a secondary with secondaryPreferred,
    # both members with nearest.
    for pref in (("primary", "secondary") if args.read_pref == "nearest" else (args.read_pref,)):
        warm_up(MongoClient(args.uri, readPreference=pref)[DB_NAME][schema.collection])
    cache = admin.command("serverStatus")["wiredTiger"]["cache"]
    cache_state = {"used_mb": round(cache["bytes currently in the cache"] / 2**20),
                   "max_mb": round(cache["maximum bytes configured"] / 2**20),
                   "dirty_mb": round(cache["tracked dirty bytes in the cache"] / 2**20)}
    print(f"  settled; cache {cache_state}")

    with open(MCC_JSON, encoding="utf-8") as f:
        params = {"clients": read_ids(USERS_CSV, "id"), "cards": read_ids(CARDS_CSV, "id"),
                  "mccs": [int(k) for k in json.load(f)]}

    start_evt, out = mp.Event(), mp.Queue()
    procs = [mp.Process(target=writer, args=(i, args.writers, args.schema, args.uri,
                                             point["offset"], start_evt, args.duration, out,
                                             batch_interval, w))
             for i in range(args.writers)]
    procs += [mp.Process(target=reader, args=(i, args.schema, args.uri, args.read_pref,
                                              params, start_evt, args.duration, out,
                                              read_interval))
              for i in range(args.readers)]
    for p in procs:
        p.start()
    time.sleep(3)  # let workers connect and seek before the clock starts
    mode = "open" if args.read_rate or args.write_rate else "closed"
    print(f"Running {args.writers} writers + {args.readers} readers for {args.duration}s "
          f"({mode} loop)...")
    lags, stop_lag = [], threading.Event()
    is_replica_set = "setName" in admin.command("hello")
    if is_replica_set:
        threading.Thread(target=sample_replication_lag, args=(admin, stop_lag, lags),
                         daemon=True).start()
    start_evt.set()
    parts = [out.get() for _ in procs]  # drain before join to avoid queue deadlock
    stop_lag.set()
    for p in procs:
        p.join()

    write_lat = [x for p in parts if p["role"] == "writer" for x in p["latencies"]]
    docs = sum(p["docs"] for p in parts if p["role"] == "writer")
    # Writers stop early once the held-back 20% is exhausted.
    write_secs = max(p["elapsed"] for p in parts if p["role"] == "writer")
    reads = {op: [x for p in parts if p["role"] == "reader" for x in p["latencies"][op]]
             for op in READ_OPS}
    unserved = {op: sum(p["unserved"][op] for p in parts if p["role"] == "reader")
                for op in READ_OPS}
    total_reads = sum(len(v) for v in reads.values()) - sum(unserved.values())
    result = {
        "label": args.label, "schema": args.schema, "uri": args.uri,
        "read_pref": args.read_pref, "writers": args.writers, "readers": args.readers,
        "duration_s": args.duration, "removed_before_run": removed,
        "mode": mode, "target_read_rate": args.read_rate, "target_write_rate": args.write_rate,
        "write_concern": args.write_concern or "server default",
        "replication_lag_s": ({"max": max(lags), "mean": round(statistics.fmean(lags), 2),
                               "samples": len(lags)} if lags else None),
        "cache_at_start": cache_state,
        "writes": {"docs": docs, "seconds": round(write_secs, 1),
                   "docs_per_sec": round(docs / write_secs),
                   "batch_latency": percentiles(write_lat)},
        "reads": {"ops": total_reads, "ops_per_sec": round(total_reads / args.duration, 2),
                  "per_query": {op: percentiles(v) for op, v in reads.items()},
                  "timeouts": {op: sum(p["timeouts"][op] for p in parts if p["role"] == "reader")
                               for op in READ_OPS},
                  "unserved": unserved},
    }
    out_file = RESULTS_DIR / f"workload_{args.label}.json"
    out_file.write_text(json.dumps(result, indent=2))

    print(f"\nWrites: {docs:,} docs in {write_secs:.0f}s ({result['writes']['docs_per_sec']:,} docs/s), "
          f"batch p50 {result['writes']['batch_latency'].get('p50_ms')} ms")
    print(f"Reads:  {total_reads:,} ops ({result['reads']['ops_per_sec']} ops/s), "
          f"unserved {sum(unserved.values()):,}")
    for op, s in result["reads"]["per_query"].items():
        print(f"  {op:18} n={s['count']:<6} p50={s.get('p50_ms')} p95={s.get('p95_ms')} ms")
    print(f"Results written to {out_file}")


if __name__ == "__main__":
    main()

"""Import the dataset with a given schema adapter. Only the oldest 80% of
transactions are imported; the rest is replayed by the workload.

  baseline  : CSV loaded as-is (every value a string), no index besides _id
  optimized : native types, natural keys as _id (see schemas.Optimized)

Usage: .venv/Scripts/python src/import_data.py --schema baseline
"""
import argparse
import csv
import json
import time
from itertools import islice

from pymongo import MongoClient

from config import (BATCH_SIZE, CARDS_CSV, DB_NAME, MCC_JSON, MONGO_URI,
                    RESULT_PREFIX, RESULTS_DIR, SPLIT_ROW, TRANSACTIONS_CSV, USERS_CSV)
from schemas import SCHEMAS


def import_csv(coll, path, to_doc, limit=None):
    """Insert rows of `path` into `coll` converted by `to_doc`; return (count, seconds)."""
    start = time.perf_counter()
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        rows = islice(csv.DictReader(f), limit)
        while batch := list(islice(rows, BATCH_SIZE)):
            coll.insert_many([to_doc(r) for r in batch], ordered=False)
            count += len(batch)
            if count % 1_000_000 < BATCH_SIZE <= count:
                print(f"  {coll.name}: {count:,} docs ({time.perf_counter() - start:.0f}s)")
    return count, time.perf_counter() - start


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="baseline", choices=SCHEMAS)
    args = ap.parse_args()
    schema = SCHEMAS[args.schema]
    names = schema.collections

    db = MongoClient(MONGO_URI)[DB_NAME]
    for name in names.values():
        db.drop_collection(name)

    results = {}
    for key, path, to_doc, limit in (("users", USERS_CSV, schema.user_doc, None),
                                     ("cards", CARDS_CSV, schema.card_doc, None),
                                     ("transactions", TRANSACTIONS_CSV, schema.to_doc, SPLIT_ROW)):
        count, secs = import_csv(db[names[key]], path, to_doc, limit)
        results[key] = {"docs": count, "seconds": round(secs, 2),
                        "docs_per_sec": round(count / secs)}
        print(f"{names[key]}: {count:,} docs in {secs:.1f}s")

    with open(MCC_JSON, encoding="utf-8") as f:
        mcc = json.load(f)
    db[names["mcc_codes"]].insert_many([schema.mcc_doc(k, v) for k, v in mcc.items()])
    results["mcc_codes"] = {"docs": len(mcc)}

    for key, name in names.items():
        stats = db.command("collStats", name)
        results[key].update(collection=name,
                            avg_doc_bytes=stats.get("avgObjSize", 0),
                            data_size_mb=round(stats["size"] / 2**20, 1),
                            storage_size_mb=round(stats["storageSize"] / 2**20, 1),
                            index_size_mb=round(stats["totalIndexSize"] / 2**20, 1))

    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"{RESULT_PREFIX}import_{args.schema}.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"Results written to {out}")


if __name__ == "__main__":
    main()

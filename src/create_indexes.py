"""Create the workload indexes on a schema and record explain() statistics for
each query before and after, plus build time and index sizes.

The collection is first reset to its post-import state (documents inserted by
earlier workload runs are removed) so "before" matches the import.

Usage: .venv/Scripts/python src/create_indexes.py --schema optimized
"""
import argparse
import json
import time
from datetime import datetime

from pymongo import MongoClient

from config import DB_NAME, MONGO_URI, RESULT_PREFIX, RESULTS_DIR
from schemas import SCHEMAS
from workload import split_point

# Fixed parameters so before/after explain() are comparable.
PARAMS = {"client_history": (825,), "card_transactions": (4524,),
          "spend_per_mcc": (5411, datetime(2015, 6, 1)),
          "errors_on_day": (datetime(2016, 3, 15),)}


class _Recorder:
    """Stand-in collection that captures the command a query method would run,
    so explain() uses exactly the workload's query code."""

    def __init__(self, name):
        self.name, self.cmd = name, None

    def find(self, filter_):
        self.cmd = {"find": self.name, "filter": filter_}
        return self

    def sort(self, key, direction):
        self.cmd["sort"] = {key: direction}
        return self

    def limit(self, n):
        self.cmd["limit"] = n
        return self

    def aggregate(self, pipeline):
        self.cmd = {"aggregate": self.name, "pipeline": pipeline, "cursor": {}}
        return []

    def __iter__(self):
        return iter(())


def explain_queries(db, schema):
    out = {}
    for op, args in PARAMS.items():
        rec = _Recorder(schema.collection)
        getattr(schema, op)(rec, *args)
        out[op] = summarize(db.command("explain", rec.cmd, verbosity="executionStats"))
    return out


def find_key(obj, key):
    """Depth-first search for the first value stored under `key`."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        obj = list(obj.values())
    if isinstance(obj, list):
        for v in obj:
            found = find_key(v, key)
            if found is not None:
                return found
    return None


def stages(plan):
    """Flatten a winning plan into its stage names, e.g. LIMIT > FETCH > IXSCAN."""
    names = []
    while isinstance(plan, dict):
        names.append(plan.get("stage", "?"))
        plan = plan.get("inputStage") or (plan.get("inputStages") or [None])[0]
    return " > ".join(names)


def summarize(explain):
    stats = find_key(explain, "executionStats")
    winning = find_key(explain, "winningPlan")
    winning = winning.get("queryPlan", winning)  # SBE plans nest the tree
    return {"plan": stages(winning),
            "keys_examined": stats["totalKeysExamined"],
            "docs_examined": stats["totalDocsExamined"],
            "returned": stats["nReturned"],
            "time_ms": stats["executionTimeMillis"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", required=True, choices=SCHEMAS)
    schema = SCHEMAS[ap.parse_args().schema]
    db = MongoClient(MONGO_URI)[DB_NAME]
    coll = db[schema.collection]

    removed = coll.delete_many(schema.live_filter(split_point()["last_imported_id"])).deleted_count
    print(f"Reset: removed {removed:,} workload docs; {coll.estimated_document_count():,} docs")
    for name in coll.index_information():
        if name != "_id_":
            coll.drop_index(name)

    print("Explain without indexes...")
    before = explain_queries(db, schema)

    build = {}
    for name, keys, opts in schema.INDEXES:
        t = time.perf_counter()
        coll.create_index(keys, name=name, **opts)
        build[name] = round(time.perf_counter() - t, 1)
        print(f"  built {name} in {build[name]}s")

    print("Explain with indexes...")
    after = explain_queries(db, schema)

    sizes = db.command("collStats", coll.name)["indexSizes"]
    result = {"schema": schema.name, "build_seconds": build,
              "index_size_mb": {k: round(v / 2**20, 1) for k, v in sizes.items()},
              "explain_before": before, "explain_after": after}
    out = RESULTS_DIR / f"{RESULT_PREFIX}indexes_{schema.name}.json"
    out.write_text(json.dumps(result, indent=2))

    for op in before:
        b, a = before[op], after[op]
        print(f"{op:18} {b['plan']:<20} docs {b['docs_examined']:>10,} {b['time_ms']:>6} ms"
              f"  ->  {a['plan']:<36} keys {a['keys_examined']:>6,} docs {a['docs_examined']:>6,}"
              f" {a['time_ms']:>4} ms")
    print(f"Index sizes (MB): {result['index_size_mb']}")
    print(f"Results written to {out}")


if __name__ == "__main__":
    main()

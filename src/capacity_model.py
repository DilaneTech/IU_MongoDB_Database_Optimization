"""Scale-up projection from measured numbers: how fast the data grows, how many
bytes each transaction costs (documents, indexes) and when the indexes stop
fitting in the WiredTiger cache of each tested environment.

Index size is the critical quantity: our queries hit random clients/cards over
the whole history, so the whole index is "hot". Once it outgrows the cache,
lookups start reading from disk.

Usage: .venv/Scripts/python src/capacity_model.py  -> results/capacity_model.json
"""
import json

import pandas as pd

from config import RESULTS_DIR, TRANSACTIONS_CSV

# WiredTiger cache sizes measured in each environment (MB).
CACHES_MB = {"on-premise (7.4 GB)": 7558, "docker 6g": 2560, "docker 3g": 1024,
             "docker 1-1.5g": 256}
# Business growth scenarios: transaction volume multiplied by this factor
# (e.g. 10x more customers than the 2,000 in the dataset).
GROWTH_FACTORS = (1, 10, 100)


def load(name):
    return json.loads((RESULTS_DIR / name).read_text())


def main():
    dates = pd.read_csv(TRANSACTIONS_CSV, usecols=["date"], dtype=str)["date"]
    per_month = dates.str[:7].value_counts().sort_index()
    per_year = int(per_month.iloc[-12:].sum())  # last 12 months of the dataset

    model = {"transactions_per_year": per_year,
             "transactions_per_month": round(per_year / 12),
             "schemas": {}}
    for schema in ("baseline", "optimized"):
        imp = load(f"import_{schema}.json")["transactions"]
        idx = load(f"indexes_{schema}.json")["index_size_mb"]
        docs = imp["docs"]
        index_mb = sum(idx.values())
        b_index = index_mb * 2**20 / docs
        s = {"docs": docs,
             "data_bytes_per_doc": round(imp["data_size_mb"] * 2**20 / docs),
             "disk_bytes_per_doc": round(imp["storage_size_mb"] * 2**20 / docs),
             "index_bytes_per_doc": round(b_index),
             "index_mb_now": round(index_mb),
             "index_growth_mb_per_year": round(per_year * b_index / 2**20),
             "years_until_index_exceeds_cache": {}}
        for growth in GROWTH_FACTORS:
            yearly = per_year * growth * b_index / 2**20
            s["years_until_index_exceeds_cache"][f"x{growth}"] = {
                env: (0 if cache <= index_mb else round((cache - index_mb) / yearly, 1))
                for env, cache in CACHES_MB.items()}
        model["schemas"][schema] = s

    out = RESULTS_DIR / "capacity_model.json"
    out.write_text(json.dumps(model, indent=2))

    print(f"Growth: {per_year:,} transactions/year ({per_year // 12:,}/month)")
    for schema, s in model["schemas"].items():
        print(f"\n{schema}: {s['data_bytes_per_doc']} B data, {s['disk_bytes_per_doc']} B disk, "
              f"{s['index_bytes_per_doc']} B index per transaction; indexes now {s['index_mb_now']} MB, "
              f"+{s['index_growth_mb_per_year']} MB/year")
        for growth, envs in s["years_until_index_exceeds_cache"].items():
            print(f"  {growth:>5} growth: " + ", ".join(f"{e}: {y} y" for e, y in envs.items()))
    print(f"\nResults written to {out}")


if __name__ == "__main__":
    main()

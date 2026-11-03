"""Shared settings for import, workload and benchmarks."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RESULTS_DIR = ROOT / "results"

# Overridden in the Docker app container (see docker-compose.yml).
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
# Prefix for import/index result files, so Docker runs don't overwrite on-premise ones.
RESULT_PREFIX = os.environ.get("RESULT_PREFIX", "")
DB_NAME = "DatabaseOptimization"

TRANSACTIONS_CSV = DATA_DIR / "transactions_data.csv"
USERS_CSV = DATA_DIR / "users_data.csv"
CARDS_CSV = DATA_DIR / "cards_data.csv"
MCC_JSON = DATA_DIR / "mcc_codes.json"

# transactions_data.csv is sorted by date: the first 80% of rows are imported,
# the remaining 20% (most recent) are replayed by the workload as live inserts.
TOTAL_TRANSACTIONS = 13_305_915
SPLIT_ROW = int(TOTAL_TRANSACTIONS * 0.8)  # 10_644_732

BATCH_SIZE = 10_000

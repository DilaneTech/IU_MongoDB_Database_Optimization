"""Schema adapters: how CSV rows become documents, and how each workload query
is expressed for that schema. Import and workload are identical across
schemas; only these adapters change.

Query parameters arrive as plain Python values (int ids, datetime bounds) and
each adapter converts them to its own representation.
"""
from datetime import datetime, timedelta

from bson.decimal128 import Decimal128

DATE_FMT = "%Y-%m-%d %H:%M:%S"


class Baseline:
    """Raw import: every field is a string, exactly as in the CSV."""
    name = "baseline"
    collections = {"users": "users", "cards": "cards",
                   "mcc_codes": "mcc_codes", "transactions": "transactions"}
    collection = collections["transactions"]

    @staticmethod
    def to_doc(row):
        return row

    user_doc = card_doc = to_doc

    @staticmethod
    def mcc_doc(code, description):
        return {"code": code, "description": description}

    @staticmethod
    def live_filter(last_imported_id):
        """Match documents inserted by the workload (id beyond the import)."""
        return {"$expr": {"$gt": [{"$toLong": "$id"}, last_imported_id]}}

    @staticmethod
    def client_history(coll, client_id):
        return list(coll.find({"client_id": str(client_id)})
                    .sort("date", -1).limit(20))

    @staticmethod
    def card_transactions(coll, card_id):
        return list(coll.find({"card_id": str(card_id)}).limit(50))

    @staticmethod
    def spend_per_mcc(coll, mcc, start):
        # Amounts are strings like "$-77.00": strip "$" and convert per document.
        end = start + timedelta(days=30)
        return list(coll.aggregate([
            {"$match": {"mcc": str(mcc),
                        "date": {"$gte": start.strftime(DATE_FMT),
                                 "$lt": end.strftime(DATE_FMT)}}},
            {"$group": {"_id": "$mcc", "total": {"$sum": {"$toDouble": {
                "$substrCP": ["$amount", 1, {"$strLenCP": "$amount"}]}}},
                "count": {"$sum": 1}}},
        ]))

    @staticmethod
    def errors_on_day(coll, day):
        # {"$gt": ""} is equivalent to {"$ne": ""} on these string fields, but
        # unlike $ne it is allowed in a partial index filter (see INDEXES).
        end = day + timedelta(days=1)
        return list(coll.find({"date": {"$gte": day.strftime(DATE_FMT),
                                        "$lt": end.strftime(DATE_FMT)},
                               "errors": {"$gt": ""}}))

    # Same keys as Optimized.INDEXES; only the partial filter differs.
    INDEXES = [
        ("client_date", [("client_id", 1), ("date", -1)], {}),
        ("card_date", [("card_id", 1), ("date", -1)], {}),
        ("mcc_date_amount", [("mcc", 1), ("date", 1), ("amount", 1)], {}),
        ("date_errors_partial", [("date", 1)],
         {"partialFilterExpression": {"errors": {"$gt": ""}}}),
    ]


def _money(value):
    """'$-77.00' / '$24295' -> Decimal128 (exact, no rounding errors)."""
    return Decimal128(value.lstrip("$"))


def _month(value):
    """'09/2002' -> datetime(2002, 9, 1)."""
    month, year = value.split("/")
    return datetime(int(year), int(month), 1)


class Optimized:
    """Native types, natural keys as _id, empty fields omitted. Collections stay
    separate and linked by reference: transactions.client_id -> users._id,
    transactions.card_id -> cards._id, transactions.mcc -> mcc_codes._id,
    cards.client_id -> users._id."""
    name = "optimized"
    collections = {"users": "users_opt", "cards": "cards_opt",
                   "mcc_codes": "mcc_codes_opt", "transactions": "transactions_opt"}
    collection = collections["transactions"]

    @staticmethod
    def to_doc(row):
        doc = {"_id": int(row["id"]),
               "date": datetime.fromisoformat(row["date"]),
               "client_id": int(row["client_id"]),
               "card_id": int(row["card_id"]),
               "amount": _money(row["amount"]),
               "use_chip": row["use_chip"],
               "merchant_id": int(row["merchant_id"]),
               "merchant_city": row["merchant_city"],
               "mcc": int(row["mcc"])}
        if row["merchant_state"]:
            doc["merchant_state"] = row["merchant_state"]
        if row["zip"]:
            doc["zip"] = int(float(row["zip"]))
        if row["errors"]:
            doc["errors"] = row["errors"].split(",")
        return doc

    @staticmethod
    def user_doc(row):
        return {"_id": int(row["id"]),
                "current_age": int(row["current_age"]),
                "retirement_age": int(row["retirement_age"]),
                "birth_year": int(row["birth_year"]),
                "birth_month": int(row["birth_month"]),
                "gender": row["gender"],
                "address": row["address"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "per_capita_income": _money(row["per_capita_income"]),
                "yearly_income": _money(row["yearly_income"]),
                "total_debt": _money(row["total_debt"]),
                "credit_score": int(row["credit_score"]),
                "num_credit_cards": int(row["num_credit_cards"])}

    @staticmethod
    def card_doc(row):
        return {"_id": int(row["id"]),
                "client_id": int(row["client_id"]),
                "card_brand": row["card_brand"],
                "card_type": row["card_type"],
                "card_number": row["card_number"],  # identifier, not a quantity
                "expires": _month(row["expires"]),
                "cvv": row["cvv"],  # may have leading zeros
                "has_chip": row["has_chip"].upper() == "YES",
                "num_cards_issued": int(row["num_cards_issued"]),
                "credit_limit": _money(row["credit_limit"]),
                "acct_open_date": _month(row["acct_open_date"]),
                "year_pin_last_changed": int(row["year_pin_last_changed"]),
                "card_on_dark_web": row["card_on_dark_web"].upper() == "YES"}

    @staticmethod
    def mcc_doc(code, description):
        return {"_id": int(code), "description": description}

    @staticmethod
    def live_filter(last_imported_id):
        return {"_id": {"$gt": last_imported_id}}

    @staticmethod
    def client_history(coll, client_id):
        return list(coll.find({"client_id": client_id}).sort("date", -1).limit(20))

    @staticmethod
    def card_transactions(coll, card_id):
        return list(coll.find({"card_id": card_id}).limit(50))

    @staticmethod
    def spend_per_mcc(coll, mcc, start):
        end = start + timedelta(days=30)
        return list(coll.aggregate([
            {"$match": {"mcc": mcc, "date": {"$gte": start, "$lt": end}}},
            {"$group": {"_id": "$mcc", "total": {"$sum": "$amount"}, "count": {"$sum": 1}}},
        ]))

    @staticmethod
    def errors_on_day(coll, day):
        end = day + timedelta(days=1)
        return list(coll.find({"date": {"$gte": day, "$lt": end},
                               "errors": {"$exists": True}}))

    INDEXES = [
        # equality on client, then sort on date: no in-memory sort
        ("client_date", [("client_id", 1), ("date", -1)], {}),
        ("card_date", [("card_id", 1), ("date", -1)], {}),
        # ESR rule + amount included so the aggregation is covered by the index
        ("mcc_date_amount", [("mcc", 1), ("date", 1), ("amount", 1)], {}),
        # only transactions with errors (~1.6%) are indexed
        ("date_errors_partial", [("date", 1)],
         {"partialFilterExpression": {"errors": {"$exists": True}}}),
    ]


class BaselineIndexed(Baseline):
    """Same as Baseline, in its own collection so it can keep its indexes
    while Baseline stays unindexed (avoids rebuilding indexes between runs)."""
    name = "baseline_indexed"
    collections = {**Baseline.collections, "transactions": "transactions_idx"}
    collection = collections["transactions"]


class OptimizedIndexed(Optimized):
    name = "optimized_indexed"
    collections = {**Optimized.collections, "transactions": "transactions_opt_idx"}
    collection = collections["transactions"]


SCHEMAS = {s.name: s for s in (Baseline, Optimized, BaselineIndexed, OptimizedIndexed)}

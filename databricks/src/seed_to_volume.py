# Databricks notebook source
# MAGIC %md
# MAGIC # Seed -> Unity Catalog Volume
# MAGIC
# MAGIC Writes the 124 payments as Debezium `op='r'` envelopes (one JSON per line) into the
# MAGIC landing Volume, replacing Postgres/Debezium/Kafka. The DLT pipeline's Bronze table
# MAGIC then ingests these files with Auto Loader. Self-contained (no sibling imports) because
# MAGIC Free Edition serverless intermittently fails FUSE reads of workspace files. The seed
# MAGIC logic mirrors `common.py`, which is unit-tested in `tests/test_databricks_helpers.py`.

# COMMAND ----------

import json
import os
from datetime import datetime, timedelta, timezone

CATALOG, SCHEMA, VOLUME = "workspace", "analytics", "landing"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SEED_DIR = f"{VOLUME_ROOT}/payments"
EXPECTED_SEED_COUNT = 124


def _micros(moment):
    return int(moment.timestamp() * 1_000_000)


def _payment(payment_id, merchant_id, shopper_id, amount, currency, payment_method,
             payment_status, country_code, created_at, updated_at):
    return {
        "payment_id": payment_id, "merchant_id": merchant_id, "shopper_id": shopper_id,
        "amount": amount, "currency": currency, "payment_method": payment_method,
        "payment_status": payment_status, "country_code": country_code,
        "created_at": created_at, "updated_at": updated_at,
    }


def generate_seed_payments(now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    payments = [
        _payment(1001, 1, 501, 149.99, "EUR", "card", "authorized", "NL",
                 now - timedelta(days=2), now - timedelta(days=2)),
        _payment(1002, 2, 502, 499.50, "USD", "paypal", "failed", "US",
                 now - timedelta(days=1), now - timedelta(days=1)),
        _payment(1003, 1, 503, 89.00, "EUR", "card", "authorized", "BE",
                 now - timedelta(hours=12), now - timedelta(hours=12)),
        _payment(1004, 3, 504, 44.25, "EUR", "card", "refunded", "DE",
                 now - timedelta(hours=6), now - timedelta(hours=2)),
    ]
    currencies = ("EUR", "USD", "GBP", "CAD")
    methods = ("card", "paypal", "apple_pay", "bank_transfer", "google_pay")
    statuses = ("authorized", "failed", "authorized", "pending", "refunded", "authorized", "chargeback", "cancelled")
    countries = ("NL", "US", "DE", "BE", "FR", "GB", "CA", "ES")
    for gs in range(1, 121):
        payment_id = 2000 + gs
        amount = round(25 + ((gs * 17) % 475) + (((gs * 13) % 100) / 100), 2)
        created = (now - timedelta(hours=(gs - 1) % 168)).replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=((gs - 1) % 4) * 15)
        updated = created + timedelta(hours=(payment_id % 6) + 1)
        payments.append(_payment(
            payment_id, ((gs - 1) % 10) + 1, 700 + gs, amount,
            currencies[(gs - 1) % 4], methods[(gs - 1) % 5],
            statuses[(gs - 1) % 8], countries[(gs - 1) % 8], created, updated,
        ))
    return payments


def to_debezium_envelope(payment):
    after = dict(payment)
    after["created_at"] = _micros(payment["created_at"])
    after["updated_at"] = _micros(payment["updated_at"])
    return {"op": "r", "before": None, "after": after}

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

os.makedirs(SEED_DIR, exist_ok=True)
target = f"{SEED_DIR}/seed_snapshot.jsonl"
with open(target, "w", encoding="utf-8") as handle:
    for payment in generate_seed_payments():
        handle.write(json.dumps(to_debezium_envelope(payment)) + "\n")

print(f"Wrote {EXPECTED_SEED_COUNT} payment envelopes to {target}")

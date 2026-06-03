"""Shared constants and pure-Python helpers for the Databricks payments pipeline.

This module is intentionally free of PySpark imports so it can be unit-tested
without a Spark session (see tests/test_databricks_helpers.py). The medallion
jobs (02_bronze .. 04_gold) import it for table names, paths, and the PII mask;
the seed job (01_seed_to_volume) imports the seed generator and envelope builder.

The transformation contract mirrors the local Spark jobs in config/spark/jobs/*:
the seed produces Debezium-shaped 'r' (read/snapshot) envelopes so the Silver and
Gold logic ports over unchanged -- only the catalog (Unity Catalog), table format
(Delta), source (files via Auto Loader), and storage paths (Volumes) differ.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

# --- Unity Catalog layout -------------------------------------------------
# Free Edition ships the `workspace` catalog; change CATALOG if you use another.
CATALOG = "workspace"
SCHEMA = "analytics"
VOLUME = "landing"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.payments_bronze"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.payments_silver"
DLQ_TABLE = f"{CATALOG}.{SCHEMA}.payments_silver_dlq"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.payment_metrics_gold"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SEED_DIR = f"{VOLUME_ROOT}/payments"
CHECKPOINT_ROOT = f"{VOLUME_ROOT}/_checkpoints"
SCHEMA_ROOT = f"{VOLUME_ROOT}/_schemas"

# Synthetic CDC source identity (the file-based stand-in for the Kafka topic).
CDC_TOPIC = "cdc.public.payments"

# --- Data contract (kept in sync with config/spark/jobs/silver_payments.py) -
PII_FIELDS = {"shopper_id"}

ALLOWED_PAYMENT_METHODS = (
    "apple_pay",
    "bank_transfer",
    "card",
    "google_pay",
    "paypal",
)

ALLOWED_PAYMENT_STATUSES = (
    "authorized",
    "cancelled",
    "chargeback",
    "failed",
    "pending",
    "refunded",
)

EXPECTED_SEED_COUNT = 124


def mask_pii_fields(value: str | None) -> str | None:
    """Hash PII fields in both `before` and `after` of a Debezium envelope.

    Identical behaviour to config/spark/jobs/bronze_from_kafka.py so PII never
    lands in the lakehouse. Non-JSON input is passed through unchanged.
    """
    if value is None:
        return None
    try:
        envelope = json.loads(value)
        for section in ("before", "after"):
            if isinstance(envelope.get(section), dict):
                for field in PII_FIELDS:
                    if field in envelope[section] and envelope[section][field] is not None:
                        raw = str(envelope[section][field]).encode()
                        envelope[section][field] = hashlib.sha256(raw).hexdigest()
        return json.dumps(envelope)
    except (json.JSONDecodeError, TypeError):
        return value


def _micros(moment: datetime) -> int:
    """Microseconds since epoch, matching Debezium's io.debezium.time.MicroTimestamp.

    Silver parses these back with `get_json_object(...).cast(double) / 1_000_000`.
    """
    return int(moment.timestamp() * 1_000_000)


def generate_seed_payments(now: datetime | None = None) -> list[dict[str, Any]]:
    """Reproduce config/postgres/init/002_seed_data.sql in Python.

    4 explicit payments (1001-1004) + 120 deterministic generated payments
    (2001-2120) = 124 rows, all drawing from the allowed value sets so every
    row passes the Silver data-quality checks. `now` is injectable for tests.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    payments: list[dict[str, Any]] = [
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
    statuses = ("authorized", "failed", "authorized", "pending",
                "refunded", "authorized", "chargeback", "cancelled")
    countries = ("NL", "US", "DE", "BE", "FR", "GB", "CA", "ES")

    for gs in range(1, 121):
        payment_id = 2000 + gs
        amount = round(25 + ((gs * 17) % 475) + (((gs * 13) % 100) / 100), 2)
        # date_trunc('hour', now - ((gs-1)%168) hours) - ((gs-1)%4)*15 minutes
        created = (now - timedelta(hours=(gs - 1) % 168)).replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=((gs - 1) % 4) * 15)
        updated = created + timedelta(hours=(payment_id % 6) + 1)
        payments.append(
            _payment(
                payment_id,
                ((gs - 1) % 10) + 1,
                700 + gs,
                amount,
                currencies[(gs - 1) % 4],
                methods[(gs - 1) % 5],
                statuses[(gs - 1) % 8],
                countries[(gs - 1) % 8],
                created,
                updated,
            )
        )

    return payments


def _payment(payment_id, merchant_id, shopper_id, amount, currency,
             payment_method, payment_status, country_code, created_at, updated_at):
    return {
        "payment_id": payment_id,
        "merchant_id": merchant_id,
        "shopper_id": shopper_id,
        "amount": amount,
        "currency": currency,
        "payment_method": payment_method,
        "payment_status": payment_status,
        "country_code": country_code,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def to_debezium_envelope(payment: dict[str, Any]) -> dict[str, Any]:
    """Wrap a payment dict as a Debezium snapshot ('r') envelope.

    Timestamps are emitted as microsecond epoch ints (MicroTimestamp), which is
    what Silver/Gold expect when they divide by 1_000_000.
    """
    after = dict(payment)
    after["created_at"] = _micros(payment["created_at"])
    after["updated_at"] = _micros(payment["updated_at"])
    return {"op": "r", "before": None, "after": after}


def seed_jsonl(now: datetime | None = None) -> str:
    """Full seed as newline-delimited JSON (one Debezium envelope per line)."""
    lines = [
        json.dumps(to_debezium_envelope(payment))
        for payment in generate_seed_payments(now)
    ]
    return "\n".join(lines) + "\n"

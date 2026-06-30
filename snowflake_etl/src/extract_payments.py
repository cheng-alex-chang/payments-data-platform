"""Extract the payments table from Postgres -- the second source feeding the Snowflake ELT.

Phase 1 of the Snowflake ELT branch. Uses a server-side (named) cursor so a large table streams
to the client in bounded batches instead of buffering every row in memory at once. Rows are
yielded as plain dicts; serializing Decimal/timestamp values to JSON is the staging layer's
job (Phase 2), so this module stays a pure reader.
"""
from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Iterator
from typing import Any

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

PAYMENT_COLUMNS = (
    "payment_id", "merchant_id", "shopper_id", "amount", "currency",
    "payment_method", "payment_status", "country_code", "created_at", "updated_at",
)


def connect_from_env() -> Any:
    """Connect using PG*/POSTGRES_* env vars. Defaults match the local Compose stack
    (host=localhost:5432) and the Airflow network alias can override via PGHOST=postgres."""
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "payments"),
        user=os.getenv("POSTGRES_USER", "dataeng"),
        password=os.getenv("POSTGRES_PASSWORD", "dataeng"),
    )


def fetch_payments(conn: Any, *, itersize: int = 5000) -> Iterator[dict]:
    """Stream payments rows as dicts via a server-side cursor (bounded memory at any volume).

    A *named* cursor keeps the result set on the Postgres server and ships ``itersize`` rows
    per round trip, so extracting 50k (or 5M) rows never materializes them all client-side.
    """
    columns = ", ".join(PAYMENT_COLUMNS)
    with conn.cursor(name="payments_extract") as cursor:
        cursor.itersize = itersize
        cursor.execute(f"SELECT {columns} FROM payments ORDER BY payment_id")
        for row in cursor:
            yield dict(zip(PAYMENT_COLUMNS, row))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract payments from Postgres.")
    parser.add_argument("--dry-run", action="store_true", help="Print the row count and a sample")
    args = parser.parse_args(argv)

    conn = connect_from_env()
    try:
        count = 0
        sample: list[dict] = []
        for row in fetch_payments(conn):
            if len(sample) < 2:
                sample.append(row)
            count += 1
    finally:
        conn.close()

    LOGGER.info("Extracted %d payments rows from Postgres", count)
    if args.dry_run:
        for row in sample:
            print(row)
    return count


if __name__ == "__main__":  # pragma: no cover
    main()

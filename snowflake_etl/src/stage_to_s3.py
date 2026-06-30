"""Stage extracted records to AWS S3 as newline-delimited JSON (Phase 2).

Consumes the Phase-1 extractor streams and writes one object per dataset per run under a
date-partitioned prefix -- ``raw/<dataset>/dt=<run_date>/<dataset>-<run_date>.jsonl`` -- which
is exactly what a Snowflake external stage + ``COPY INTO`` expects to read (Phase 3).

This is also where the serialization the extractors deferred happens: ``Decimal`` money values
are written as JSON *strings* to preserve exact NUMERIC precision (never as floats), and
timestamps as ISO-8601.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import decimal
import io
import json
import logging
import os
from collections.abc import Iterable
from typing import Any

import boto3

from snowflake_etl.src import extract_fx_rates, extract_payments

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

DEFAULT_PREFIX = "raw"


def _json_default(value: Any) -> str:
    """Serialize the non-JSON-native types the extractors emit."""
    if isinstance(value, decimal.Decimal):
        return str(value)  # exact precision for money -- never coerce to float
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    raise TypeError(f"Cannot serialize value of type {type(value).__name__}")


def serialize_jsonl(records: Iterable[dict]) -> tuple[bytes, int]:
    """Render records as newline-delimited JSON bytes; returns (body, row_count)."""
    buffer = io.StringIO()
    count = 0
    for record in records:
        buffer.write(json.dumps(record, default=_json_default))
        buffer.write("\n")
        count += 1
    return buffer.getvalue().encode("utf-8"), count


def stage_dataset(
    s3_client: Any,
    bucket: str,
    dataset: str,
    records: Iterable[dict],
    *,
    run_date: dt.date | None = None,
    prefix: str = DEFAULT_PREFIX,
) -> tuple[str, int]:
    """Serialize ``records`` and PUT them as a single object; returns (s3_key, row_count)."""
    run_date = run_date or dt.date.today()
    body, count = serialize_jsonl(records)
    key = f"{prefix}/{dataset}/dt={run_date.isoformat()}/{dataset}-{run_date.isoformat()}.jsonl"
    s3_client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson"
    )
    LOGGER.info("Staged %d %s rows to s3://%s/%s", count, dataset, bucket, key)
    return key, count


def s3_client_from_env() -> Any:
    """Standard boto3 S3 client; credentials/region come from the usual AWS_* env / profile."""
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))


def _fx_records() -> Iterable[dict]:
    start, end = extract_fx_rates.default_window()
    return (dataclasses.asdict(rate) for rate in extract_fx_rates.fetch_fx_rates(start, end))


def _payment_records() -> Iterable[dict]:
    conn = extract_payments.connect_from_env()
    try:
        yield from extract_payments.fetch_payments(conn)
    finally:
        conn.close()


# dataset name -> zero-arg factory. Called lazily so staging one source never triggers the
# other's extract (e.g. staging fx_rates alone won't open a Postgres connection).
DATASET_FACTORIES = {
    "fx_rates": _fx_records,
    "payments": _payment_records,
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Stage FX rates + payments to S3.")
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET"), help="Target S3 bucket")
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=sorted(DATASET_FACTORIES),
        default=sorted(DATASET_FACTORIES),
        help="Which datasets to extract + stage (default: all). Lets the DAG run them in parallel.",
    )
    parser.add_argument(
        "--run-date",
        default=None,
        help="Partition date YYYY-MM-DD (default: today). The DAG passes Airflow's {{ ds }}.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Serialize from real sources and print a sample, without touching S3",
    )
    args = parser.parse_args(argv)
    run_date = dt.date.fromisoformat(args.run_date) if args.run_date else None

    if args.dry_run:
        for name in args.datasets:
            body, count = serialize_jsonl(DATASET_FACTORIES[name]())
            preview = body.decode("utf-8").splitlines()[:2]
            LOGGER.info("[dry-run] %s: %d rows, %d bytes", name, count, len(body))
            for line in preview:
                print(f"{name}: {line}")
        return

    if not args.bucket:
        raise SystemExit("--bucket (or S3_BUCKET) is required when not in --dry-run")
    client = s3_client_from_env()
    for name in args.datasets:
        stage_dataset(client, args.bucket, name, DATASET_FACTORIES[name](), run_date=run_date)


if __name__ == "__main__":  # pragma: no cover
    main()

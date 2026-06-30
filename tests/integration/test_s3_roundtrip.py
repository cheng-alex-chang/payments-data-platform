"""Real-S3 round-trip for the Phase-2 stager (gated; skipped without AWS creds).

The mocked sibling (tests/test_stage_to_s3.py, moto) proves the serialization + key layout
logic offline and always runs. This one proves the *real* boto3 PUT/GET path against a live
bucket -- the thing moto can't certify. It is opt-in: it stays SKIPPED unless both
AWS credentials and S3_BUCKET are present, so CI and a plain ``pytest`` clone stay green.

Run it in the cloud session with, e.g.::

    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1 \
    S3_BUCKET=my-payments-lake pytest -m integration tests/integration/test_s3_roundtrip.py
"""
from __future__ import annotations

import datetime as dt
import json
import os
import uuid

import pytest

pytestmark = pytest.mark.integration

_HAS_AWS = bool(os.getenv("AWS_ACCESS_KEY_ID")) and bool(os.getenv("S3_BUCKET"))
_skip = pytest.mark.skipif(
    not _HAS_AWS, reason="set AWS_* credentials and S3_BUCKET to run the real S3 round-trip"
)


@_skip
def test_stage_dataset_roundtrips_through_real_s3() -> None:
    from snowflake_etl.src import stage_to_s3

    bucket = os.environ["S3_BUCKET"]
    client = stage_to_s3.s3_client_from_env()
    records = [{"rate_date": "2026-06-29", "currency": "EUR", "rate_to_usd": 1.08}]
    # Unique prefix so concurrent/repeat runs never clobber each other or real data.
    prefix = f"itest/{uuid.uuid4().hex[:8]}"

    key, count = stage_to_s3.stage_dataset(
        client, bucket, "fx_rates", records, run_date=dt.date(2026, 6, 29), prefix=prefix
    )
    try:
        assert count == 1
        assert key == f"{prefix}/fx_rates/dt=2026-06-29/fx_rates-2026-06-29.jsonl"
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        assert json.loads(body.strip())["currency"] == "EUR"
    finally:
        client.delete_object(Bucket=bucket, Key=key)  # leave the bucket as we found it

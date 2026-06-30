from __future__ import annotations

import datetime as dt
import decimal
import json

import boto3
import pytest
from moto import mock_aws

from snowflake_etl.src import stage_to_s3 as module

BUCKET = "test-payments-lake"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def test_serialize_jsonl_handles_decimal_and_datetime() -> None:
    records = [
        {
            "payment_id": 1,
            "amount": decimal.Decimal("149.99"),
            "created_at": dt.datetime(2025, 6, 30, 12, 0, 0),
        }
    ]

    body, count = module.serialize_jsonl(records)

    assert count == 1
    parsed = json.loads(body.decode("utf-8").strip())
    assert parsed["amount"] == "149.99"  # Decimal -> string preserves exact money precision
    assert parsed["created_at"] == "2025-06-30T12:00:00"  # datetime -> ISO-8601


def test_serialize_raises_on_unsupported_type() -> None:
    with pytest.raises(TypeError):
        module.serialize_jsonl([{"x": object()}])


def test_stage_dataset_writes_partitioned_object(s3_client) -> None:  # noqa: ANN001
    records = [{"rate_date": "2025-06-30", "currency": "EUR", "rate_to_usd": 1.08}]

    key, count = module.stage_dataset(
        s3_client, BUCKET, "fx_rates", records, run_date=dt.date(2026, 6, 29)
    )

    assert count == 1
    assert key == "raw/fx_rates/dt=2026-06-29/fx_rates-2026-06-29.jsonl"

    body = s3_client.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
    assert json.loads(body.strip())["currency"] == "EUR"


def test_main_stages_only_requested_datasets(s3_client, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(module, "s3_client_from_env", lambda: s3_client)
    monkeypatch.setitem(
        module.DATASET_FACTORIES,
        "fx_rates",
        lambda: [{"rate_date": "2026-06-29", "currency": "EUR", "rate_to_usd": 1.08}],
    )
    payments_called = {"v": False}

    def payments_factory() -> list:
        payments_called["v"] = True  # must NOT run when only fx_rates is requested
        return []

    monkeypatch.setitem(module.DATASET_FACTORIES, "payments", payments_factory)

    module.main(["--bucket", BUCKET, "--datasets", "fx_rates", "--run-date", "2026-06-29"])

    keys = [obj["Key"] for obj in s3_client.list_objects_v2(Bucket=BUCKET).get("Contents", [])]
    assert any("raw/fx_rates/dt=2026-06-29/" in k for k in keys)  # honors --run-date partition
    assert not any("raw/payments/" in k for k in keys)
    assert payments_called["v"] is False  # the unselected source never extracted

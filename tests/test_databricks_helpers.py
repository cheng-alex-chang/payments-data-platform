from __future__ import annotations

import importlib.util
import json
import pathlib
from datetime import datetime, timezone

import pytest


def _load_common():
    path = pathlib.Path(__file__).resolve().parents[1] / "databricks" / "src" / "common.py"
    spec = importlib.util.spec_from_file_location("databricks_common", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


common = _load_common()
FIXED_NOW = datetime(2026, 6, 3, 12, 0, 0, tzinfo=timezone.utc)


def test_mask_pii_fields_hashes_shopper_id_in_both_sections() -> None:
    envelope = json.dumps(
        {"op": "u", "before": {"shopper_id": 501, "amount": 1},
         "after": {"shopper_id": 502, "amount": 2}}
    )

    masked = json.loads(common.mask_pii_fields(envelope))

    assert masked["before"]["shopper_id"] == common.hashlib.sha256(b"501").hexdigest()
    assert masked["after"]["shopper_id"] == common.hashlib.sha256(b"502").hexdigest()
    # Non-PII fields are untouched.
    assert masked["before"]["amount"] == 1
    assert masked["after"]["amount"] == 2


def test_mask_pii_fields_is_none_and_non_json_safe() -> None:
    assert common.mask_pii_fields(None) is None
    assert common.mask_pii_fields("not json") == "not json"


def test_generate_seed_payments_count_and_ids() -> None:
    payments = common.generate_seed_payments(FIXED_NOW)

    assert len(payments) == common.EXPECTED_SEED_COUNT == 124
    ids = [p["payment_id"] for p in payments]
    assert len(set(ids)) == 124
    assert {1001, 1002, 1003, 1004}.issubset(set(ids))
    assert max(p["payment_id"] for p in payments if p["payment_id"] >= 2000) == 2120


def test_generate_seed_payments_all_rows_pass_silver_contract() -> None:
    for p in common.generate_seed_payments(FIXED_NOW):
        assert p["payment_method"] in common.ALLOWED_PAYMENT_METHODS
        assert p["payment_status"] in common.ALLOWED_PAYMENT_STATUSES
        assert len(p["currency"]) == 3 and p["currency"].isupper()
        assert len(p["country_code"]) == 2 and p["country_code"].isupper()
        assert p["amount"] >= 0
        assert p["updated_at"] >= p["created_at"]


def test_to_debezium_envelope_shape_and_micro_timestamps() -> None:
    payment = common.generate_seed_payments(FIXED_NOW)[0]
    envelope = common.to_debezium_envelope(payment)

    assert envelope["op"] == "r"
    assert envelope["before"] is None
    after = envelope["after"]
    assert isinstance(after["created_at"], int)
    assert isinstance(after["updated_at"], int)
    # Microseconds, not seconds/millis: round-trips to the original instant.
    assert after["created_at"] == int(payment["created_at"].timestamp() * 1_000_000)


def test_seed_jsonl_is_124_parseable_envelopes() -> None:
    lines = common.seed_jsonl(FIXED_NOW).strip().splitlines()

    assert len(lines) == 124
    for line in lines:
        envelope = json.loads(line)
        assert envelope["op"] == "r"
        assert "payment_id" in envelope["after"]

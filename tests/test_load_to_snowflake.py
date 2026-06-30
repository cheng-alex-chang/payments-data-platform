from __future__ import annotations

import datetime as dt
from unittest import mock

from snowflake_etl.src import load_to_snowflake as module


def test_create_raw_table_sql_is_idempotent_variant_landing() -> None:
    sql = module.create_raw_table_sql("RAW.RAW_FX_RATES")

    assert "CREATE TABLE IF NOT EXISTS RAW.RAW_FX_RATES" in sql  # safe to re-run
    assert "raw         VARIANT" in sql                          # whole JSON line, untyped
    assert "source_file STRING" in sql                           # lineage column
    assert "loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()" in sql


def test_copy_into_sql_targets_partitioned_stage_path() -> None:
    sql = module.copy_into_sql(
        "RAW.RAW_FX_RATES", "PAYMENTS_LAKE_STAGE", "fx_rates", dt.date(2026, 6, 29)
    )

    # Stage location must mirror the Phase-2 S3 key layout exactly.
    assert "@PAYMENTS_LAKE_STAGE/raw/fx_rates/dt=2026-06-29/" in sql
    assert "COPY INTO RAW.RAW_FX_RATES (raw, source_file)" in sql
    assert "SELECT $1, METADATA$FILENAME" in sql                 # raw VARIANT + lineage
    assert "TYPE = JSON" in sql
    assert "ON_ERROR = ABORT_STATEMENT" in sql                   # fail loud on bad rows


def test_copy_into_sql_accepts_templated_run_date() -> None:
    # The Airflow DAG passes "{{ ds }}" so SnowflakeOperator renders the partition at runtime.
    sql = module.copy_into_sql("RAW.RAW_PAYMENTS", "PAYMENTS_LAKE_STAGE", "payments", "{{ ds }}")
    assert "@PAYMENTS_LAKE_STAGE/raw/payments/dt={{ ds }}/" in sql


def test_sum_rows_loaded_counts_loaded_skips_already_loaded() -> None:
    result = [
        ("fx-a.jsonl", "LOADED", 1536, 1536, 0, None),
        ("fx-b.jsonl", "LOAD_SKIPPED", None, None, None, None),  # already loaded -> 0
        ("fx-c.jsonl", "LOADED", 10, 10, 0, None),
    ]

    assert module._sum_rows_loaded(result) == 1546


def test_load_dataset_runs_ddl_then_copy_and_returns_rows() -> None:
    cursor = mock.MagicMock()
    cursor.fetchall.return_value = [("payments.jsonl", "LOADED", 50004, 50004, 0, None)]
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor

    loaded = module.load_dataset(
        conn,
        table="RAW.RAW_PAYMENTS",
        stage="PAYMENTS_LAKE_STAGE",
        dataset="payments",
        run_date=dt.date(2026, 6, 29),
    )

    assert loaded == 50004
    # DDL first, then COPY -- in that order.
    executed = [call.args[0] for call in cursor.execute.call_args_list]
    assert executed[0].startswith("CREATE TABLE IF NOT EXISTS RAW.RAW_PAYMENTS")
    assert executed[1].startswith("COPY INTO RAW.RAW_PAYMENTS")
    cursor.close.assert_called_once()  # cursor always closed, even though we asserted success


def test_main_dry_run_prints_sql_without_connecting(capsys) -> None:  # noqa: ANN001
    # connect_from_env would raise if called (no creds / no driver); dry-run must not call it.
    with mock.patch.object(module, "connect_from_env", side_effect=AssertionError("connected!")):
        module.main(["--dry-run", "--datasets", "fx_rates", "--run-date", "2026-06-29"])

    out = capsys.readouterr().out
    assert "CREATE TABLE IF NOT EXISTS RAW.RAW_FX_RATES" in out
    assert "@PAYMENTS_LAKE_STAGE/raw/fx_rates/dt=2026-06-29/" in out

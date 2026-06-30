from __future__ import annotations

from unittest import mock

from snowflake_etl.src import transform as module


def test_transform_files_exist_and_are_dependency_ordered() -> None:
    # Every referenced model is present on disk...
    for name in (*module.TRANSFORM_FILES, module.VALIDATE_FILE):
        assert (module.SQL_DIR / name).is_file(), name

    # ...and the order respects dependencies: staging -> dim -> fact -> aggregate.
    order = module.TRANSFORM_FILES
    assert order.index("stg_payments.sql") < order.index("dim_fx_rates.sql")
    assert order.index("stg_fx_rates.sql") < order.index("dim_fx_rates.sql")
    assert order.index("dim_fx_rates.sql") < order.index("fct_payments_usd.sql")
    assert order.index("fct_payments_usd.sql") < order.index("agg_payments_by_currency.sql")


def test_dim_fx_rates_forward_fills_business_day_gaps() -> None:
    sql = module.read_sql("dim_fx_rates.sql")
    assert "LAST_VALUE(rate_to_usd) IGNORE NULLS" in sql   # carry last known rate forward
    assert "FIRST_VALUE(rate_to_usd) IGNORE NULLS" in sql  # cover the leading edge
    assert "is_filled" in sql                              # gaps are flagged, not hidden


def test_fct_left_joins_and_computes_usd_amount() -> None:
    sql = module.read_sql("fct_payments_usd.sql")
    # LEFT JOIN keeps unmatched payments so validate can catch them (vs. silent INNER drop).
    assert "LEFT JOIN ANALYTICS.DIM_FX_RATES" in sql
    assert "ROUND(p.amount * d.rate_to_usd, 2) AS usd_amount" in sql


def test_validate_reconciles_and_labels_pass_fail() -> None:
    sql = module.read_sql("validate.sql")
    assert "fact_reconciles_to_payments" in sql
    assert "no_unmatched_usd_amount" in sql
    assert "IFF(expected = actual, 'PASS', 'FAIL')" in sql


def test_run_transforms_executes_each_model_in_order() -> None:
    cursor = mock.MagicMock()
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor

    module.run_transforms(conn, files=("stg_payments.sql", "dim_fx_rates.sql"))

    executed = [call.args[0] for call in cursor.execute.call_args_list]
    assert len(executed) == 2
    assert "CREATE OR REPLACE VIEW ANALYTICS.STG_PAYMENTS" in executed[0]
    assert "CREATE OR REPLACE TABLE ANALYTICS.DIM_FX_RATES" in executed[1]
    cursor.close.assert_called_once()


def test_run_validation_raises_on_failure() -> None:
    cursor = mock.MagicMock()
    cursor.fetchall.return_value = [
        ("fact_reconciles_to_payments", 50004, 50000, "FAIL"),
        ("usd_payments_unchanged", 0, 0, "PASS"),
    ]
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor

    try:
        module.run_validation(conn)
        raise AssertionError("expected RuntimeError on a FAIL check")
    except RuntimeError as exc:
        assert "fact_reconciles_to_payments" in str(exc)


def test_run_validation_returns_rows_when_all_pass() -> None:
    cursor = mock.MagicMock()
    rows = [("fact_reconciles_to_payments", 50004, 50004, "PASS")]
    cursor.fetchall.return_value = rows
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor

    assert module.run_validation(conn) == rows


def test_main_transform_only_runs_models_and_skips_validation() -> None:
    with (
        mock.patch.object(module, "connect_from_env", return_value=mock.MagicMock()) as factory,
        mock.patch.object(module, "run_transforms") as run_transforms,
        mock.patch.object(module, "run_validation") as run_validation,
    ):
        module.main(["--transform-only"])

    run_transforms.assert_called_once()
    run_validation.assert_not_called()
    factory.return_value.close.assert_called_once()  # connection closed even on the split path


def test_main_validate_only_skips_models() -> None:
    with (
        mock.patch.object(module, "connect_from_env", return_value=mock.MagicMock()),
        mock.patch.object(module, "run_transforms") as run_transforms,
        mock.patch.object(module, "run_validation", return_value=[]) as run_validation,
    ):
        module.main(["--validate-only"])

    run_transforms.assert_not_called()
    run_validation.assert_called_once()

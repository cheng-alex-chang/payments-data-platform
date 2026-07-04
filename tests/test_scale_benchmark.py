from __future__ import annotations

import pytest

from snowflake_etl.benchmark import scale_benchmark as sb


# ---------------------------------------------------------------------------
# chunk planning
# ---------------------------------------------------------------------------

def test_chunk_plan_splits_evenly() -> None:
    plan = sb.chunk_plan(2_500_000_000, 250_000_000)
    assert len(plan) == 10
    assert plan[0] == {"index": 0, "rows": 250_000_000, "id_offset": 0, "n_chunks": 10}
    # ids are contiguous and non-overlapping across chunks
    assert plan[1]["id_offset"] == 250_000_000
    assert sum(c["rows"] for c in plan) == 2_500_000_000


def test_chunk_plan_handles_remainder() -> None:
    plan = sb.chunk_plan(100, 30)
    assert [c["rows"] for c in plan] == [30, 30, 30, 10]
    assert [c["id_offset"] for c in plan] == [0, 30, 60, 90]
    assert all(c["n_chunks"] == 4 for c in plan)


def test_chunk_plan_resume_skips_but_keeps_offsets() -> None:
    plan = sb.chunk_plan(100, 30, start_chunk=2)
    assert [c["index"] for c in plan] == [2, 3]
    # offset of chunk 2 still reflects the two skipped chunks — ids never collide on resume
    assert plan[0]["id_offset"] == 60


def test_chunk_plan_rejects_nonpositive() -> None:
    with pytest.raises(ValueError):
        sb.chunk_plan(0, 100)
    with pytest.raises(ValueError):
        sb.chunk_plan(100, 0)


# ---------------------------------------------------------------------------
# generated SQL contracts
# ---------------------------------------------------------------------------

_STG_PAYMENT_FIELDS = [
    "payment_id", "merchant_id", "shopper_id", "amount", "currency",
    "payment_method", "payment_status", "country_code", "created_at", "updated_at",
]


def test_payments_sql_has_every_stg_field_and_generator() -> None:
    sql = sb.generate_payments_sql("PAYMENTS_SCALE", rows=1000, id_offset=0, window_days=365)
    for field in _STG_PAYMENT_FIELDS:
        assert f"'{field}'" in sql, field
    assert "OBJECT_CONSTRUCT" in sql
    assert "GENERATOR(ROWCOUNT => 1000)" in sql
    assert "PAYMENTS_SCALE.RAW.RAW_PAYMENTS" in sql


def test_payments_amount_is_serialized_as_a_json_string() -> None:
    # stg casts raw:amount::NUMBER(12,2) from a STRING — money must not go through a float.
    sql = sb.generate_payments_sql("DB", rows=10, id_offset=0, window_days=365)
    assert "'amount', TO_VARCHAR(CAST(" in sql


def test_payments_id_offset_makes_ids_unique_across_chunks() -> None:
    sql = sb.generate_payments_sql("DB", rows=10, id_offset=250_000_000, window_days=365)
    # ROW_NUMBER (not bare SEQ4) so ids can't collide under parallel GENERATOR execution
    assert "250000000 + ROW_NUMBER() OVER (ORDER BY SEQ4()) AS pid" in sql


def test_dup_sql_forces_a_newer_version_for_dedup() -> None:
    sql = sb.generate_payment_dups_sql("DB", dup_rows=50, id_offset=0, window_days=365)
    # updated_at is pushed to tomorrow so QUALIFY (updated_at DESC) keeps the dup;
    # a distinct source_file gives a later loaded_at tiebreak too.
    assert "DATEADD('day', 1, CURRENT_DATE())" in sql
    assert "'benchmark-dup'" in sql
    assert "GENERATOR(ROWCOUNT => 50)" in sql


def test_delta_uses_future_updated_at_above_the_watermark(monkeypatch) -> None:
    # The base dup rows set the fact watermark to tomorrow; the delta must sit ABOVE it
    # (day-after-tomorrow) or the incremental model filters every delta row out.
    conn = _patch_connect(monkeypatch)
    sb.main(["delta", "--rows", "100", "--id-offset", "3000000000"])
    inserts = [s for s in conn.cursor_obj.executed if "RAW_PAYMENTS" in s]
    assert len(inserts) == 1
    assert "DATEADD('day', 2, CURRENT_DATE())" in inserts[0]
    assert "3000000000 + ROW_NUMBER() OVER (ORDER BY SEQ4()) AS pid" in inserts[0]


def test_base_generate_updated_at_equals_created_at() -> None:
    # base rows: updated_at derives from ts (created_at), NOT a future constant
    sql = sb.generate_payments_sql("DB", rows=10, id_offset=0, window_days=365)
    assert "DATEADD('day', 2, CURRENT_DATE())" not in sql


def test_fx_sql_covers_currencies_and_leaves_weekend_gaps() -> None:
    sql = sb.generate_fx_sql("DB", window_days=365)
    for ccy, _ in sb._FX_BASE_RATES:
        assert f"'{ccy}'" in sql
    assert "'rate_date'" in sql and "'rate_to_usd'" in sql
    # business days only — weekend rows omitted so the forward-fill has real holes
    assert "DAYOFWEEKISO(d) < 6" in sql
    assert "DB.RAW.RAW_FX_RATES" in sql
    # USD must stay exactly 1.0 (identity) so the usd_payments_unchanged gate holds
    assert "IFF(currency = 'USD', 0," in sql


# ---------------------------------------------------------------------------
# infra DDL
# ---------------------------------------------------------------------------

def test_setup_sql_creates_isolated_db_warehouse_and_landing_tables() -> None:
    stmts = sb.setup_sql("PAYMENTS_SCALE", "BENCH_WH", "MEDIUM")
    joined = "\n".join(stmts)
    assert "CREATE DATABASE IF NOT EXISTS PAYMENTS_SCALE" in joined
    assert "CREATE SCHEMA IF NOT EXISTS PAYMENTS_SCALE.RAW" in joined
    assert "CREATE SCHEMA IF NOT EXISTS PAYMENTS_SCALE.ANALYTICS" in joined
    assert "WAREHOUSE_SIZE = 'MEDIUM'" in joined and "AUTO_SUSPEND = 60" in joined
    # landing tables reuse the loader's exact VARIANT DDL
    assert joined.count("raw         VARIANT") == 2
    assert "PAYMENTS_SCALE.RAW.RAW_PAYMENTS" in joined
    assert "PAYMENTS_SCALE.RAW.RAW_FX_RATES" in joined


def test_teardown_drops_exactly_the_two_scale_objects() -> None:
    stmts = sb.teardown_sqls("PAYMENTS_SCALE", "BENCH_WH")
    assert stmts == [
        "DROP DATABASE IF EXISTS PAYMENTS_SCALE",
        "DROP WAREHOUSE IF EXISTS BENCH_WH",
    ]


def test_measure_reports_logical_json_bytes() -> None:
    sql = sb.measure_sql("PAYMENTS_SCALE")
    assert "SUM(LENGTH(TO_JSON(raw)))" in sql
    assert "PAYMENTS_SCALE.RAW.RAW_PAYMENTS" in sql


def test_storage_sql_scopes_to_the_database() -> None:
    # TABLE_STORAGE_METRICS returns rows for other databases the role can see, so the
    # catalog filter is what keeps the demo and scale databases out of each other's report.
    sql = sb.storage_sql("PAYMENTS_SCALE")
    assert "TABLE_CATALOG = 'PAYMENTS_SCALE'" in sql
    assert "TABLE_DROPPED IS NULL" in sql


def test_report_scopes_to_the_benchmark_warehouse() -> None:
    sql = sb.report_sql("BENCH_WH")
    assert "QUERY_HISTORY_BY_WAREHOUSE" in sql
    assert "WAREHOUSE_NAME => 'BENCH_WH'" in sql
    assert "BYTES_SCANNED" in sql


# ---------------------------------------------------------------------------
# runner wiring (mocked connection — no driver, no account)
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self._result: list[tuple] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchall(self) -> list[tuple]:
        return self._result

    def close(self) -> None:
        pass


class FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = FakeCursor()
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


def _patch_connect(monkeypatch) -> FakeConn:
    conn = FakeConn()
    # connect_from_env is imported lazily inside the module's helpers
    monkeypatch.setattr(
        "snowflake_etl.src.load_to_snowflake.connect_from_env",
        lambda: conn,
    )
    return conn


def test_setup_runs_ddl_without_touching_context(monkeypatch) -> None:
    conn = _patch_connect(monkeypatch)
    sb.main(["setup"])
    executed = conn.cursor_obj.executed
    assert any("CREATE DATABASE IF NOT EXISTS PAYMENTS_SCALE" in s for s in executed)
    # setup must NOT issue USE DATABASE PAYMENTS_SCALE (it doesn't exist at connect time)
    assert not any(s.startswith("USE DATABASE") for s in executed)
    assert conn.closed


def test_generate_dry_run_does_not_connect(monkeypatch, capsys) -> None:
    called = {"connect": False}

    def _boom():
        called["connect"] = True
        raise AssertionError("dry-run must not connect")

    monkeypatch.setattr("snowflake_etl.src.load_to_snowflake.connect_from_env", _boom)
    sb.main(["generate", "--rows", "1000", "--chunk-rows", "500", "--dry-run"])
    out = capsys.readouterr().out
    assert "GENERATOR(ROWCOUNT => 500)" in out
    assert called["connect"] is False


def test_generate_pins_context_and_emits_chunks_plus_dups(monkeypatch) -> None:
    conn = _patch_connect(monkeypatch)
    sb.main(["generate", "--rows", "1000", "--chunk-rows", "500", "--dup-fraction", "0.05"])
    executed = conn.cursor_obj.executed
    # session pinned to the benchmark warehouse + db before generating
    assert "USE WAREHOUSE BENCH_WH" in executed
    assert "USE DATABASE PAYMENTS_SCALE" in executed
    # fx once + 2 chunk inserts + 2 dup inserts (25 dups per 500-row chunk)
    assert sum("RAW_FX_RATES" in s for s in executed) == 1
    assert sum("RAW_PAYMENTS" in s and "benchmark-dup" not in s for s in executed) == 2
    assert sum("benchmark-dup" in s for s in executed) == 2


def test_teardown_runs_drops(monkeypatch) -> None:
    conn = _patch_connect(monkeypatch)
    sb.main(["teardown"])
    executed = conn.cursor_obj.executed
    assert "DROP DATABASE IF EXISTS PAYMENTS_SCALE" in executed
    assert "DROP WAREHOUSE IF EXISTS BENCH_WH" in executed

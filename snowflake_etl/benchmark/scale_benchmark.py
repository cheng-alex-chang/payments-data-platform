"""Generate a TB-scale synthetic payments dataset *inside* Snowflake and benchmark the
dbt star schema against it.

Why in-warehouse generation: a real terabyte cannot be produced on a laptop and shipped
over a home uplink to S3. Snowflake's ``GENERATOR`` table function fabricates billions of
rows server-side with no network transfer, matching the exact RAW VARIANT contract the
staging models already parse — so the *same* dbt project builds the *same* star schema,
just at ~2.5B rows / ~1 TB of logical JSON instead of 50K.

The work lands in a **separate** ``PAYMENTS_SCALE`` database and ``BENCH_WH`` warehouse so
it never touches the real ``PAYMENTS`` demo, and ``teardown`` drops both when done. dbt is
retargeted at the scale objects by env vars only (``SNOWFLAKE_DATABASE`` / ``_WAREHOUSE``)
— no model changes.

SQL builders are pure functions (testable offline, no driver, no account); the thin runner
reuses ``load_to_snowflake.create_raw_table_sql`` (the exact landing-table DDL) and
``connect_from_env`` (key-pair auth) so nothing is duplicated.

Subcommands: ``setup``, ``generate``, ``delta``, ``measure``, ``report``, ``teardown``.
``--dry-run`` prints every statement without connecting.
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any

from snowflake_etl.src.load_to_snowflake import create_raw_table_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

DEFAULT_DATABASE = "PAYMENTS_SCALE"
DEFAULT_WAREHOUSE = "BENCH_WH"
DEFAULT_WAREHOUSE_SIZE = "MEDIUM"
DEFAULT_CHUNK_ROWS = 250_000_000
DEFAULT_DUP_FRACTION = 0.05
# Keep every generated created_at inside a 12-month window so dim_date's GENERATOR spine
# (capped at 3660 rows) always covers the span — dense history, not a decade-wide sprawl.
DEFAULT_WINDOW_DAYS = 365

RAW_PAYMENTS = "RAW.RAW_PAYMENTS"
RAW_FX_RATES = "RAW.RAW_FX_RATES"

# Same 6 currencies as the real seed, with plausible "USD per 1 unit" base rates the
# extractor would have produced (it inverts the ECB quote). USD is the identity.
_FX_BASE_RATES = [
    ("EUR", "1.08"),
    ("USD", "1.00"),
    ("GBP", "1.27"),
    ("CAD", "0.73"),
    ("AUD", "0.66"),
    ("CHF", "1.12"),
]


# ---------------------------------------------------------------------------
# Chunk planning (pure)
# ---------------------------------------------------------------------------

def chunk_plan(total_rows: int, chunk_rows: int, start_chunk: int = 0) -> list[dict]:
    """Split ``total_rows`` into ``ceil(total/chunk)`` resumable chunks.

    Each chunk carries its ``id_offset`` (so payment_ids stay globally unique and contiguous
    across chunks) and its row count. ``start_chunk`` skips already-loaded chunks on resume;
    the offsets of skipped chunks are still accounted for so ids never collide.
    """
    if total_rows <= 0 or chunk_rows <= 0:
        raise ValueError("total_rows and chunk_rows must be positive")
    n_chunks = -(-total_rows // chunk_rows)  # integer ceil — no float rounding at billions
    plan = []
    for i in range(n_chunks):
        offset = i * chunk_rows
        rows = min(chunk_rows, total_rows - offset)
        if i >= start_chunk:
            plan.append({"index": i, "rows": rows, "id_offset": offset, "n_chunks": n_chunks})
    return plan


# ---------------------------------------------------------------------------
# DDL / infra (pure)
# ---------------------------------------------------------------------------

def setup_sql(database: str, warehouse: str, warehouse_size: str, auto_suspend: int = 60) -> list[str]:
    """Create the isolated scale database (RAW + ANALYTICS schemas), the benchmark
    warehouse, and the two VARIANT landing tables (reusing the loader's exact DDL)."""
    return [
        f"CREATE DATABASE IF NOT EXISTS {database}",
        f"CREATE SCHEMA IF NOT EXISTS {database}.RAW",
        f"CREATE SCHEMA IF NOT EXISTS {database}.ANALYTICS",
        (
            f"CREATE WAREHOUSE IF NOT EXISTS {warehouse} "
            f"WITH WAREHOUSE_SIZE = '{warehouse_size}' "
            f"AUTO_SUSPEND = {auto_suspend} AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE"
        ),
        create_raw_table_sql(f"{database}.{RAW_PAYMENTS}"),
        create_raw_table_sql(f"{database}.{RAW_FX_RATES}"),
    ]


def teardown_sqls(database: str, warehouse: str) -> list[str]:
    return [
        f"DROP DATABASE IF EXISTS {database}",
        f"DROP WAREHOUSE IF EXISTS {warehouse}",
    ]


def use_context_sqls(database: str, warehouse: str, schema: str = "RAW") -> list[str]:
    return [
        f"USE WAREHOUSE {warehouse}",
        f"USE DATABASE {database}",
        f"USE SCHEMA {schema}",
    ]


# ---------------------------------------------------------------------------
# Data generation (pure)
# ---------------------------------------------------------------------------

def _payment_object(status_expr: str, updated_expr: str) -> str:
    """The OBJECT_CONSTRUCT that mirrors the exact stg_payments contract.

    Critically, ``amount`` is a JSON *string* (TO_VARCHAR of a NUMBER(12,2)) — the real
    extractor serialized money as a string to keep exact precision, and stg casts
    ``raw:amount::NUMBER(12,2)`` straight back. Currency/method/status/country are indexed
    out of literal arrays by the row sequence; timestamps are ISO-8601 the ::TIMESTAMP_NTZ
    cast in staging accepts.
    """
    return (
        "OBJECT_CONSTRUCT("
        "'payment_id', pid, "
        "'merchant_id', MOD(s, 10) + 1, "
        "'shopper_id', MOD(s, 8000) + 1, "
        "'amount', TO_VARCHAR(CAST(10 + UNIFORM(0, 990, RANDOM()) "
        "+ UNIFORM(0, 99, RANDOM()) / 100.0 AS NUMBER(12,2))), "
        "'currency', ARRAY_CONSTRUCT('EUR','USD','GBP','CAD','AUD','CHF')[MOD(s, 6)]::STRING, "
        "'payment_method', ARRAY_CONSTRUCT('card','paypal','apple_pay','bank_transfer','google_pay')[MOD(s, 5)]::STRING, "
        f"'payment_status', {status_expr}, "
        "'country_code', ARRAY_CONSTRUCT('NL','US','DE','BE','FR','GB','CA','ES','AU','CH')[MOD(s, 10)]::STRING, "
        "'created_at', TO_VARCHAR(ts, 'YYYY-MM-DD\"T\"HH24:MI:SS'), "
        f"'updated_at', {updated_expr}"
        ")"
    )


def generate_payments_sql(
    database: str, rows: int, id_offset: int, window_days: int, *, updated_at_expr: str | None = None
) -> str:
    """INSERT ``rows`` synthetic payments via GENERATOR, ids offset for global uniqueness.

    created_at is spread uniformly across the trailing ``window_days``. By default
    ``updated_at == created_at`` (the base version; a later dup version is added separately).
    The delta path overrides ``updated_at_expr`` with a *future* timestamp so its rows sit
    above the fact's watermark and are actually picked up by the incremental model."""
    status = "ARRAY_CONSTRUCT('authorized','failed','authorized','pending','refunded','authorized','chargeback','cancelled')[MOD(s, 8)]::STRING"
    updated = updated_at_expr or "TO_VARCHAR(ts, 'YYYY-MM-DD\"T\"HH24:MI:SS')"
    obj = _payment_object(status_expr=status, updated_expr=updated)
    return (
        f"INSERT INTO {database}.{RAW_PAYMENTS} (raw, source_file)\n"
        f"SELECT {obj}, 'benchmark'\n"
        f"FROM (\n"
        f"  SELECT\n"
        # ROW_NUMBER (not bare SEQ4) is unique within the query even under parallel
        # GENERATOR execution; SEQ4 alone can repeat on a larger warehouse, which would
        # duplicate payment_ids and silently shrink the deduped fact.
        f"    {id_offset} + ROW_NUMBER() OVER (ORDER BY SEQ4()) AS pid,\n"
        f"    SEQ4() AS s,\n"
        f"    DATEADD('second', UNIFORM(0, {window_days} * 86400, RANDOM()),\n"
        f"            DATEADD('day', -{window_days}, CURRENT_DATE())::TIMESTAMP_NTZ) AS ts\n"
        f"  FROM TABLE(GENERATOR(ROWCOUNT => {rows}))\n"
        f")"
    )


def generate_payment_dups_sql(database: str, dup_rows: int, id_offset: int, window_days: int) -> str:
    """Re-emit the first ``dup_rows`` payment_ids of a chunk as a *newer* version.

    updated_at is forced to tomorrow — guaranteed later than any base row's updated_at
    (all within the trailing window) — and loaded in a separate statement (later loaded_at),
    so the stg QUALIFY dedup (ORDER BY updated_at DESC, loaded_at DESC) deterministically
    keeps this version. This makes RAW carry duplicate versions per key, exactly like the
    real accumulating daily snapshots, so the dedup does genuine work at scale.
    """
    obj = _payment_object(
        status_expr="'refunded'",
        updated_expr="TO_VARCHAR(DATEADD('day', 1, CURRENT_DATE())::TIMESTAMP_NTZ, 'YYYY-MM-DD\"T\"HH24:MI:SS')",
    )
    return (
        f"INSERT INTO {database}.{RAW_PAYMENTS} (raw, source_file)\n"
        f"SELECT {obj}, 'benchmark-dup'\n"
        f"FROM (\n"
        f"  SELECT\n"
        f"    {id_offset} + ROW_NUMBER() OVER (ORDER BY SEQ4()) AS pid,\n"
        f"    SEQ4() AS s,\n"
        f"    DATEADD('second', UNIFORM(0, {window_days} * 86400, RANDOM()),\n"
        f"            DATEADD('day', -{window_days}, CURRENT_DATE())::TIMESTAMP_NTZ) AS ts\n"
        f"  FROM TABLE(GENERATOR(ROWCOUNT => {dup_rows}))\n"
        f")"
    )


def generate_fx_sql(database: str, window_days: int) -> str:
    """One row per (business day, currency) across the window — weekend gaps left in so the
    dim_fx_rates forward-fill has real holes to carry across."""
    values = ", ".join(f"('{ccy}', {rate})" for ccy, rate in _FX_BASE_RATES)
    return (
        f"INSERT INTO {database}.{RAW_FX_RATES} (raw, source_file)\n"
        f"SELECT OBJECT_CONSTRUCT(\n"
        f"    'rate_date', TO_VARCHAR(d, 'YYYY-MM-DD'),\n"
        f"    'currency', currency,\n"
        # USD is the identity (exactly 1.0) so USD payments stay unchanged; other currencies
        # drift a little around their base so the FX-over-time story is real.
        f"    'rate_to_usd', ROUND(base + IFF(currency = 'USD', 0, UNIFORM(-3, 3, RANDOM()) / 100.0), 6)\n"
        f"), 'benchmark'\n"
        f"FROM (\n"
        f"  SELECT DATEADD('day', SEQ4(), DATEADD('day', -{window_days}, CURRENT_DATE())) AS d\n"
        f"  FROM TABLE(GENERATOR(ROWCOUNT => {window_days} + 1))\n"
        f") days\n"
        f"CROSS JOIN (SELECT * FROM VALUES {values} AS t(currency, base))\n"
        f"WHERE DAYOFWEEKISO(d) < 6"
    )


# ---------------------------------------------------------------------------
# Measurement (pure)
# ---------------------------------------------------------------------------

def measure_sql(database: str) -> str:
    """Row counts + true logical JSON size (SUM(LENGTH(TO_JSON(raw)))) — the honest '~1 TB'
    number, independent of Snowflake's columnar compression."""
    return (
        "SELECT\n"
        f"  (SELECT COUNT(*) FROM {database}.{RAW_PAYMENTS}) AS raw_payment_rows,\n"
        f"  (SELECT COUNT(*) FROM {database}.{RAW_FX_RATES}) AS raw_fx_rows,\n"
        f"  (SELECT SUM(LENGTH(TO_JSON(raw))) FROM {database}.{RAW_PAYMENTS}) AS logical_json_bytes"
    )


def storage_sql(database: str) -> str:
    """Compressed on-disk bytes per table from Snowflake's storage metrics view."""
    return (
        "SELECT TABLE_NAME, ACTIVE_BYTES\n"
        f"FROM {database}.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS\n"
        # TABLE_DROPPED IS NULL excludes prior table versions still held in Time-Travel/
        # Fail-safe after a TRUNCATE, which otherwise show up as stale duplicate rows.
        "WHERE TABLE_SCHEMA = 'RAW' AND TABLE_DROPPED IS NULL"
    )


def report_sql(warehouse: str, limit: int = 40) -> str:
    """Per-statement elapsed time + bytes scanned for the benchmark warehouse, newest first."""
    return (
        "SELECT QUERY_ID, LEFT(QUERY_TEXT, 60) AS query_text,\n"
        "       EXECUTION_TIME / 1000 AS exec_seconds, BYTES_SCANNED, ROWS_PRODUCED\n"
        "FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(\n"
        f"    WAREHOUSE_NAME => '{warehouse}',\n"
        f"    RESULT_LIMIT => {limit}))\n"
        "WHERE EXECUTION_STATUS = 'SUCCESS'\n"
        "ORDER BY START_TIME DESC"
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def _execute(cur: Any, sql: str, label: str) -> list[tuple]:
    start = time.monotonic()
    cur.execute(sql)
    rows = cur.fetchall()
    LOGGER.info("%s: %.1fs", label, time.monotonic() - start)
    return rows


def _connect_and_use(database: str, warehouse: str, *, use_context: bool):
    """Reuse the loader's key-pair connection, then pin the session to the benchmark
    warehouse/db (so it works regardless of the SNOWFLAKE_DATABASE env default)."""
    from snowflake_etl.src.load_to_snowflake import connect_from_env

    conn = connect_from_env()
    if use_context:
        cur = conn.cursor()
        try:
            for stmt in use_context_sqls(database, warehouse):
                cur.execute(stmt)
        finally:
            cur.close()
    return conn


def _run_statements(conn: Any, statements: list[str]) -> None:
    cur = conn.cursor()
    try:
        for sql in statements:
            _execute(cur, sql, sql.split("\n", 1)[0][:50])
    finally:
        cur.close()


def cmd_setup(args) -> None:
    statements = setup_sql(args.database, args.warehouse, args.warehouse_size)
    if args.dry_run:
        _print(statements)
        return
    # setup connects to the default (existing) DB, then creates the scale objects.
    conn = _connect_and_use(args.database, args.warehouse, use_context=False)
    try:
        _run_statements(conn, statements)
    finally:
        conn.close()
    LOGGER.info("Setup complete: %s + %s", args.database, args.warehouse)


def cmd_generate(args) -> None:
    plan = chunk_plan(args.rows, args.chunk_rows, args.start_chunk)
    dup_rows_per_chunk = [round(c["rows"] * args.dup_fraction) for c in plan]
    if args.dry_run:
        stmts = [generate_fx_sql(args.database, args.window_days)]
        for chunk, dups in zip(plan, dup_rows_per_chunk):
            stmts.append(generate_payments_sql(args.database, chunk["rows"], chunk["id_offset"], args.window_days))
            if dups:
                stmts.append(generate_payment_dups_sql(args.database, dups, chunk["id_offset"], args.window_days))
        _print(stmts)
        return

    conn = _connect_and_use(args.database, args.warehouse, use_context=True)
    try:
        cur = conn.cursor()
        try:
            # FX first (small), only when starting fresh.
            if args.start_chunk == 0:
                _execute(cur, generate_fx_sql(args.database, args.window_days), "generate fx")
            n = plan[0]["n_chunks"] if plan else 0
            for chunk, dups in zip(plan, dup_rows_per_chunk):
                _execute(
                    cur,
                    generate_payments_sql(args.database, chunk["rows"], chunk["id_offset"], args.window_days),
                    f"chunk {chunk['index'] + 1}/{n}: {chunk['rows']:,} payments",
                )
                if dups:
                    _execute(
                        cur,
                        generate_payment_dups_sql(args.database, dups, chunk["id_offset"], args.window_days),
                        f"chunk {chunk['index'] + 1}/{n}: {dups:,} dup versions",
                    )
        finally:
            cur.close()
    finally:
        conn.close()


def cmd_delta(args) -> None:
    """One 'next day' increment: brand-new payment_ids (offset past the base range) with a
    *future* updated_at (day-after-tomorrow), to time the incremental fct_payments_usd path.

    The future updated_at is deliberate: the base dup rows set the fact's watermark to
    tomorrow, so anything at/below that is skipped by the incremental filter. Day-after-
    tomorrow guarantees the delta sits above the watermark and is the only thing processed."""
    future = "TO_VARCHAR(DATEADD('day', 2, CURRENT_DATE())::TIMESTAMP_NTZ, 'YYYY-MM-DD\"T\"HH24:MI:SS')"
    stmt = generate_payments_sql(
        args.database, args.rows, args.id_offset, args.window_days, updated_at_expr=future
    )
    if args.dry_run:
        _print([stmt])
        return
    conn = _connect_and_use(args.database, args.warehouse, use_context=True)
    try:
        _run_statements(conn, [stmt])
    finally:
        conn.close()


def cmd_measure(args) -> None:
    statements = [measure_sql(args.database), storage_sql(args.database)]
    if args.dry_run:
        _print(statements)
        return
    conn = _connect_and_use(args.database, args.warehouse, use_context=True)
    try:
        cur = conn.cursor()
        try:
            for sql in statements:
                for row in _execute(cur, sql, "measure"):
                    print(row)
        finally:
            cur.close()
    finally:
        conn.close()


def cmd_report(args) -> None:
    stmt = report_sql(args.warehouse)
    if args.dry_run:
        _print([stmt])
        return
    conn = _connect_and_use(args.database, args.warehouse, use_context=True)
    try:
        cur = conn.cursor()
        try:
            print("query_id | text | exec_s | bytes_scanned | rows")
            for row in _execute(cur, stmt, "report"):
                print(" | ".join(str(c) for c in row))
        finally:
            cur.close()
    finally:
        conn.close()


def cmd_teardown(args) -> None:
    statements = teardown_sqls(args.database, args.warehouse)
    if args.dry_run:
        _print(statements)
        return
    conn = _connect_and_use(args.database, args.warehouse, use_context=False)
    try:
        _run_statements(conn, statements)
    finally:
        conn.close()
    LOGGER.info("Teardown complete: dropped %s + %s", args.database, args.warehouse)


def _print(statements: list[str]) -> None:
    for sql in statements:
        print(sql.rstrip() + ";\n")


def build_parser() -> argparse.ArgumentParser:
    # Shared options via a parent so they work in either position
    # (e.g. `generate --rows N --dry-run`, not only `--dry-run generate ...`).
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--database", default=DEFAULT_DATABASE)
    common.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    common.add_argument("--dry-run", action="store_true", help="print SQL without connecting")

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0], parents=[common])
    sub = parser.add_subparsers(dest="command", required=True)

    p_setup = sub.add_parser("setup", parents=[common], help="create scale db, warehouse, landing tables")
    p_setup.add_argument("--warehouse-size", default=DEFAULT_WAREHOUSE_SIZE)
    p_setup.set_defaults(func=cmd_setup)

    p_gen = sub.add_parser("generate", parents=[common], help="generate N synthetic payments (+fx) in chunks")
    p_gen.add_argument("--rows", type=int, required=True)
    p_gen.add_argument("--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS)
    p_gen.add_argument("--dup-fraction", type=float, default=DEFAULT_DUP_FRACTION)
    p_gen.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    p_gen.add_argument("--start-chunk", type=int, default=0, help="resume from this chunk index")
    p_gen.set_defaults(func=cmd_generate)

    p_delta = sub.add_parser("delta", parents=[common], help="add a small batch of new payments (incremental test)")
    p_delta.add_argument("--rows", type=int, default=5_000_000)
    p_delta.add_argument("--id-offset", type=int, required=True, help="start id above the base range")
    p_delta.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    p_delta.set_defaults(func=cmd_delta)

    p_measure = sub.add_parser("measure", parents=[common], help="row counts + logical/compressed bytes")
    p_measure.set_defaults(func=cmd_measure)

    p_report = sub.add_parser("report", parents=[common], help="per-query timings + bytes scanned")
    p_report.set_defaults(func=cmd_report)

    p_teardown = sub.add_parser("teardown", parents=[common], help="drop the scale db + warehouse")
    p_teardown.set_defaults(func=cmd_teardown)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()

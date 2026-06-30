"""Load the S3-staged JSON into Snowflake RAW tables (Phase 3).

This is the ``L`` in ELT: it pulls the date-partitioned objects written by Phase 2
(``raw/<dataset>/dt=<run_date>/...jsonl``) off an external stage and ``COPY INTO`` a
landing table that stores each JSON line *untouched* in a single ``VARIANT`` column.
No reshaping happens here -- the transform (split VARIANT into typed columns, join FX to
payments, normalize to USD) is deferred to Snowflake SQL in Phase 4.

Design notes:
* **SQL builders are pure functions** (``create_raw_table_sql`` / ``copy_into_sql``) that
  return strings, so the statement shape is unit-testable without a live warehouse or even
  the Snowflake driver installed.
* **The driver is imported lazily** (only inside ``connect_from_env``), so the mocked unit
  tests -- and anyone running ``pytest`` after a plain clone -- never need
  ``snowflake-connector-python``. The real driver is only pulled in for the live load.
* **COPY INTO is idempotent by default**: Snowflake records which files a table has already
  loaded and silently skips them on re-run, so re-triggering the DAG won't double-load.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

DEFAULT_PREFIX = "raw"

# dataset name (matches the Phase-2 S3 prefix) -> fully-qualified RAW landing table.
RAW_TABLES = {
    "fx_rates": "RAW.RAW_FX_RATES",
    "payments": "RAW.RAW_PAYMENTS",
}


def create_raw_table_sql(table: str) -> str:
    """DDL for a VARIANT landing table; ``IF NOT EXISTS`` keeps the loader idempotent.

    ``raw`` holds each JSON line verbatim; ``source_file`` (from ``METADATA$FILENAME``) and
    ``loaded_at`` give lineage for debugging and replay auditing.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n"
        f"  raw         VARIANT,\n"
        f"  source_file STRING,\n"
        f"  loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()\n"
        f")"
    )


def copy_into_sql(
    table: str,
    stage: str,
    dataset: str,
    run_date: dt.date,
    *,
    prefix: str = DEFAULT_PREFIX,
) -> str:
    """Build the ``COPY INTO`` that reads one date partition off the external stage.

    The stage location mirrors the Phase-2 key layout exactly
    (``@<stage>/raw/<dataset>/dt=<run_date>/``). ``$1`` is the whole JSON object parsed as a
    VARIANT; ``METADATA$FILENAME`` is captured for lineage. ``ON_ERROR = ABORT_STATEMENT``
    means a single malformed row fails the load loudly rather than silently dropping data.
    """
    location = f"@{stage}/{prefix}/{dataset}/dt={run_date.isoformat()}/"
    return (
        f"COPY INTO {table} (raw, source_file)\n"
        f"FROM (\n"
        f"  SELECT $1, METADATA$FILENAME\n"
        f"  FROM {location}\n"
        f")\n"
        f"FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)\n"
        f"ON_ERROR = ABORT_STATEMENT"
    )


def _sum_rows_loaded(copy_result: list[tuple]) -> int:
    """Sum the ``rows_loaded`` column from a COPY INTO result set.

    Snowflake returns one row per source file: (file, status, rows_parsed, rows_loaded, ...).
    Skip rows that were already loaded on a previous run (status ``LOAD_SKIPPED``), which
    report no ``rows_loaded``.
    """
    total = 0
    for row in copy_result:
        if len(row) > 3 and isinstance(row[3], int):
            total += row[3]
    return total


def load_dataset(
    conn: Any,
    *,
    table: str,
    stage: str,
    dataset: str,
    run_date: dt.date,
    prefix: str = DEFAULT_PREFIX,
) -> int:
    """Ensure the landing table exists, COPY the partition in, return rows loaded."""
    cursor = conn.cursor()
    try:
        cursor.execute(create_raw_table_sql(table))
        cursor.execute(copy_into_sql(table, stage, dataset, run_date, prefix=prefix))
        result = cursor.fetchall()
    finally:
        cursor.close()
    loaded = _sum_rows_loaded(result)
    LOGGER.info("Loaded %d rows into %s from dataset %s (dt=%s)", loaded, table, dataset, run_date)
    return loaded


def connect_from_env() -> Any:
    """Open a Snowflake connection from SNOWFLAKE_* env vars.

    The driver is imported here -- not at module top -- so unit tests that only exercise the
    SQL builders never require ``snowflake-connector-python``.
    """
    import snowflake.connector  # lazy: keeps the mocked test suite driver-free

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "PAYMENTS_ETL_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PAYMENTS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "PAYMENTS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="COPY S3-staged JSON into Snowflake RAW tables.")
    parser.add_argument(
        "--stage",
        default=os.getenv("SNOWFLAKE_STAGE", "PAYMENTS_LAKE_STAGE"),
        help="Snowflake external stage name (created by Terraform in Phase 6)",
    )
    parser.add_argument(
        "--run-date",
        default=dt.date.today().isoformat(),
        help="Partition date to load (YYYY-MM-DD); defaults to today",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=sorted(RAW_TABLES),
        default=sorted(RAW_TABLES),
        help="Which datasets to load (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the DDL + COPY statements without connecting to Snowflake",
    )
    args = parser.parse_args(argv)
    run_date = dt.date.fromisoformat(args.run_date)

    if args.dry_run:
        for dataset in args.datasets:
            table = RAW_TABLES[dataset]
            print(f"-- {dataset} -> {table}")
            print(create_raw_table_sql(table) + ";")
            print(copy_into_sql(table, args.stage, dataset, run_date) + ";\n")
        return

    conn = connect_from_env()
    try:
        for dataset in args.datasets:
            load_dataset(
                conn,
                table=RAW_TABLES[dataset],
                stage=args.stage,
                dataset=dataset,
                run_date=run_date,
            )
    finally:
        conn.close()


if __name__ == "__main__":  # pragma: no cover
    main()

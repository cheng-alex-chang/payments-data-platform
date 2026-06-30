"""Run the Snowflake SQL ELT transforms in dependency order (Phase 4).

This is the ``T`` in ELT: it executes the ``.sql`` models that turn the RAW VARIANT landing
tables into typed staging views, a gap-free FX dimension, the USD-normalized payments fact, and
the monthly aggregate -- then runs validate.sql and fails if any data-quality check fails.

The SQL itself lives in ``snowflake_etl/sql/`` so it reads as plain, reviewable SQL. This module
is just the ordered executor: the file ordering and the failure logic are unit-testable with a
fake cursor, and the Snowflake driver is imported lazily (reused from load_to_snowflake) so the
tests need no driver and no warehouse.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from snowflake_etl.src.load_to_snowflake import connect_from_env

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# Dependency order: staging views -> forward-filled dimension -> USD fact -> monthly aggregate.
TRANSFORM_FILES = (
    "stg_payments.sql",
    "stg_fx_rates.sql",
    "dim_fx_rates.sql",
    "fct_payments_usd.sql",
    "agg_payments_by_currency.sql",
)
VALIDATE_FILE = "validate.sql"


def read_sql(name: str, *, sql_dir: Path = SQL_DIR) -> str:
    """Read one SQL model by filename."""
    return (sql_dir / name).read_text(encoding="utf-8")


def run_transforms(
    conn: Any, *, sql_dir: Path = SQL_DIR, files: tuple[str, ...] = TRANSFORM_FILES
) -> None:
    """Execute each transform model once, in dependency order."""
    cursor = conn.cursor()
    try:
        for name in files:
            LOGGER.info("Running transform %s", name)
            cursor.execute(read_sql(name, sql_dir=sql_dir))
    finally:
        cursor.close()


def run_validation(conn: Any, *, sql_dir: Path = SQL_DIR) -> list[tuple]:
    """Run validate.sql; raise if any named check reports FAIL. Returns the full check table."""
    cursor = conn.cursor()
    try:
        cursor.execute(read_sql(VALIDATE_FILE, sql_dir=sql_dir))
        rows = cursor.fetchall()
    finally:
        cursor.close()
    failures = [row for row in rows if row and row[-1] == "FAIL"]
    if failures:
        raise RuntimeError(f"Validation failed: {failures}")
    LOGGER.info("All %d validation checks passed", len(rows))
    return rows


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the Snowflake SQL ELT transforms.")
    parser.add_argument(
        "--validate-only", action="store_true", help="Skip transforms; just run validate.sql"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the models that would run, in order, without connecting to Snowflake",
    )
    args = parser.parse_args(argv)

    if args.dry_run:
        if not args.validate_only:
            for name in TRANSFORM_FILES:
                print(f"transform: {name}")
        print(f"validate:  {VALIDATE_FILE}")
        return

    conn = connect_from_env()
    try:
        if not args.validate_only:
            run_transforms(conn)
        for row in run_validation(conn):
            print(row)
    finally:
        conn.close()


if __name__ == "__main__":  # pragma: no cover
    main()

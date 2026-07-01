"""Snowflake FX ELT DAG -- the batch + cloud-warehouse sibling of payments_pipeline.

Operator choices:
* TaskFlow ``@task`` for the Python S3 staging (boto3 work, runs in-process).
* ``SnowflakeOperator`` for the RAW load (COPY INTO), so the warehouse load runs through an
  Airflow **Connection** (``snowflake_default``) -- credentials live in Airflow's connection
  store / secrets backend, not in the DAG.
* ``BashOperator`` running **dbt** for the transform + tests: dbt owns the model DAG
  (ref()-derived ordering) and the data-quality gates, so Airflow just invokes
  ``dbt run`` / ``dbt test`` and fails the task on a non-zero exit. (astronomer-cosmos could
  render model-level tasks later; one task per dbt command keeps this image-light.)

Runtime prerequisites (Airflow worker image): ``apache-airflow-providers-snowflake`` and
``dbt-snowflake`` installed, a ``snowflake_default`` connection, ``SNOWFLAKE_*`` env vars for
dbt's profile (see snowflake_etl/dbt/profiles.yml), and ``S3_BUCKET`` / ``SNOWFLAKE_STAGE``
available to the workers.
"""
from __future__ import annotations

import os
import shlex
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from snowflake_etl.src import load_to_snowflake, stage_to_s3

SNOWFLAKE_CONN_ID = "snowflake_default"
S3_BUCKET = os.getenv("S3_BUCKET", "payments-lake")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE", "PAYMENTS_LAKE_STAGE")
# Resolved relative to this file (repo root is two levels up from airflow/dags/) so the
# command works in any worker checkout layout; shell-quoted in case the path has spaces.
DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "snowflake_etl" / "dbt"
DBT_FLAGS = f"--project-dir {shlex.quote(str(DBT_PROJECT_DIR))} --profiles-dir {shlex.quote(str(DBT_PROJECT_DIR))}"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def _load_raw_sql() -> list[str]:
    """DDL + COPY statements for both RAW tables, partition templated to Airflow's {{ ds }}.

    Reuses the Phase-3 pure SQL builders; SnowflakeOperator renders the {{ ds }} at runtime.
    """
    statements: list[str] = []
    for dataset, table in load_to_snowflake.RAW_TABLES.items():
        statements.append(load_to_snowflake.create_raw_table_sql(table))
        statements.append(load_to_snowflake.copy_into_sql(table, SNOWFLAKE_STAGE, dataset, "{{ ds }}"))
    return statements


with DAG(
    dag_id="snowflake_fx_etl",
    default_args=default_args,
    description="Stage FX rates + payments to S3, load Snowflake RAW, dbt transform + test",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["payments", "snowflake", "s3", "fx", "elt", "dbt"],
) as dag:

    @task(task_id="stage_fx_rates")
    def stage_fx_rates(ds: str | None = None) -> None:
        args = ["--bucket", S3_BUCKET, "--datasets", "fx_rates"]
        if ds:
            args += ["--run-date", ds]
        stage_to_s3.main(args)

    @task(task_id="stage_payments")
    def stage_payments(ds: str | None = None) -> None:
        args = ["--bucket", S3_BUCKET, "--datasets", "payments"]
        if ds:
            args += ["--run-date", ds]
        stage_to_s3.main(args)

    load_raw = SnowflakeOperator(
        task_id="load_raw",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=_load_raw_sql(),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run {DBT_FLAGS}",
    )

    # Data-quality gates: schema tests + the singular reconcile/identity tests. retries=0 --
    # a deterministic data-quality failure should page, not retry.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_FLAGS}",
        retries=0,
    )

    [stage_fx_rates(), stage_payments()] >> load_raw >> dbt_run >> dbt_test

"""Snowflake FX ELT DAG -- the batch + cloud-warehouse sibling of payments_pipeline.

Idiomatic operator choices:
* TaskFlow ``@task`` for the Python S3 staging (boto3 work, runs in-process).
* ``SnowflakeOperator`` for the SQL steps (RAW load via COPY, the transform models), so the
  warehouse work runs through an Airflow **Connection** (``snowflake_default``) rather than env
  vars -- credentials live in Airflow's connection store / secrets backend, not in the DAG.
* A ``SnowflakeHook``-backed ``@task`` for validation, reusing transform.run_validation so a
  failed data-quality check raises and fails the task with a clear message.

Runtime prerequisites (Airflow worker image): ``apache-airflow-providers-snowflake`` installed,
a ``snowflake_default`` connection (account/user/password/warehouse/database/role), and
``S3_BUCKET`` / ``SNOWFLAKE_STAGE`` available to the workers.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from snowflake_etl.src import load_to_snowflake, stage_to_s3, transform

SNOWFLAKE_CONN_ID = "snowflake_default"
S3_BUCKET = os.getenv("S3_BUCKET", "payments-lake")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE", "PAYMENTS_LAKE_STAGE")

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
    description="Stage FX rates + payments to S3, load Snowflake RAW, transform to USD, validate",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["payments", "snowflake", "s3", "fx", "elt"],
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

    transform_sql = SnowflakeOperator(
        task_id="transform_sql",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=[transform.read_sql(name) for name in transform.TRANSFORM_FILES],
    )

    @task(task_id="validate")
    def validate() -> None:
        # Reuse the tested gate (raises on any FAIL) over the Airflow-managed connection.
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
        try:
            transform.run_validation(conn)
        finally:
            conn.close()

    [stage_fx_rates(), stage_payments()] >> load_raw >> transform_sql >> validate()

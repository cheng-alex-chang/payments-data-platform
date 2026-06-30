from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest


class FakeNode:
    """Shared base for every fake task: registers itself and tracks downstream edges."""

    current_dag: "FakeDAG | None" = None

    def __init__(self, task_id: str, **attrs: object) -> None:
        self.task_id = task_id
        self.downstream_task_ids: set[str] = set()
        for key, value in attrs.items():
            setattr(self, key, value)
        assert FakeNode.current_dag is not None
        FakeNode.current_dag.tasks[task_id] = self

    def __rshift__(self, other: "FakeNode") -> "FakeNode":
        self.downstream_task_ids.add(other.task_id)
        return other

    def __rrshift__(self, others: list["FakeNode"]) -> "FakeNode":
        for upstream in others:  # supports `[a, b] >> self`
            upstream.downstream_task_ids.add(self.task_id)
        return self


class FakeDAG:
    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        self.schedule_interval = kwargs.get("schedule")
        self.max_active_runs = kwargs.get("max_active_runs")
        self.tags = kwargs.get("tags")
        self.tasks: dict[str, FakeNode] = {}

    def __enter__(self) -> "FakeDAG":
        FakeNode.current_dag = self
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        FakeNode.current_dag = None

    @property
    def task_ids(self) -> set[str]:
        return set(self.tasks)

    def get_task(self, task_id: str) -> FakeNode:
        return self.tasks[task_id]


def _fake_task(*dargs, **dkwargs):  # noqa: ANN002, ANN003
    """Stand-in for airflow.decorators.task: @task(task_id=...) -> callable -> node-on-call."""
    def decorator(fn):  # noqa: ANN001, ANN202
        task_id = dkwargs.get("task_id", fn.__name__)

        def make(*args, **kwargs) -> FakeNode:  # noqa: ANN002, ANN003
            return FakeNode(task_id, kind="taskflow")

        return make

    if dargs and callable(dargs[0]) and not dkwargs:
        return decorator(dargs[0])  # bare @task usage
    return decorator


class FakeSnowflakeOperator(FakeNode):
    def __init__(self, *, task_id: str, sql=None, snowflake_conn_id=None, **kwargs) -> None:  # noqa: ANN001, ANN003
        super().__init__(task_id, sql=sql, snowflake_conn_id=snowflake_conn_id, kind="snowflake")


def _load_dag(monkeypatch: pytest.MonkeyPatch) -> FakeDAG:
    airflow_module = types.ModuleType("airflow")
    airflow_module.DAG = FakeDAG
    decorators_module = types.ModuleType("airflow.decorators")
    decorators_module.task = _fake_task
    providers = types.ModuleType("airflow.providers")
    sf = types.ModuleType("airflow.providers.snowflake")
    sf_ops = types.ModuleType("airflow.providers.snowflake.operators")
    sf_ops_sf = types.ModuleType("airflow.providers.snowflake.operators.snowflake")
    sf_ops_sf.SnowflakeOperator = FakeSnowflakeOperator

    for name, mod in {
        "airflow": airflow_module,
        "airflow.decorators": decorators_module,
        "airflow.providers": providers,
        "airflow.providers.snowflake": sf,
        "airflow.providers.snowflake.operators": sf_ops,
        "airflow.providers.snowflake.operators.snowflake": sf_ops_sf,
    }.items():
        monkeypatch.setitem(sys.modules, name, mod)

    path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "snowflake_fx_etl.py"
    spec = importlib.util.spec_from_file_location("repo_snowflake_fx_etl", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_snowflake_fx_etl_dag_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag(monkeypatch)

    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
    assert dag.task_ids == {
        "stage_fx_rates", "stage_payments", "load_raw", "transform_sql", "validate",
    }

    # Two source extracts stage to S3 in parallel, then fan in to load -> transform -> validate.
    assert dag.get_task("stage_fx_rates").downstream_task_ids == {"load_raw"}
    assert dag.get_task("stage_payments").downstream_task_ids == {"load_raw"}
    assert dag.get_task("load_raw").downstream_task_ids == {"transform_sql"}
    assert dag.get_task("transform_sql").downstream_task_ids == {"validate"}
    assert dag.get_task("validate").downstream_task_ids == set()


def test_snowflake_tasks_use_managed_connection_and_templated_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag = _load_dag(monkeypatch)

    load_raw = dag.get_task("load_raw")
    transform_sql = dag.get_task("transform_sql")

    # SQL steps run through the Airflow-managed Snowflake connection, not env vars.
    assert load_raw.snowflake_conn_id == "snowflake_default"
    assert transform_sql.snowflake_conn_id == "snowflake_default"

    # load_raw COPYs the partition Airflow renders at runtime, into the RAW VARIANT tables.
    assert any("dt={{ ds }}/" in stmt for stmt in load_raw.sql)
    assert any("COPY INTO RAW.RAW_PAYMENTS" in stmt for stmt in load_raw.sql)

    # transform_sql runs the model files in dependency order.
    assert any("CREATE OR REPLACE VIEW ANALYTICS.STG_PAYMENTS" in stmt for stmt in transform_sql.sql)
    assert any("CREATE OR REPLACE TABLE ANALYTICS.FCT_PAYMENTS_USD" in stmt for stmt in transform_sql.sql)


def test_staging_tasks_are_taskflow(monkeypatch: pytest.MonkeyPatch) -> None:
    dag = _load_dag(monkeypatch)
    assert dag.get_task("stage_fx_rates").kind == "taskflow"
    assert dag.get_task("stage_payments").kind == "taskflow"
    assert dag.get_task("validate").kind == "taskflow"  # SnowflakeHook-backed @task

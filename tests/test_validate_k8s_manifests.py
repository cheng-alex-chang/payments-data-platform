from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts import validate_k8s_manifests as module


MANIFEST = """
apiVersion: v1
kind: Namespace
metadata:
  name: data-pipeline
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  namespace: data-pipeline
---
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-bronze
  namespace: data-pipeline
spec:
  suspend: true
"""


def test_parse_objects_extracts_identity_and_namespace() -> None:
    objects = module.parse_objects(MANIFEST)

    assert objects[0] == module.K8sObject(
        kind="Namespace",
        name="data-pipeline",
        namespace=None,
        raw=objects[0].raw,
    )
    assert objects[1].kind == "StatefulSet"
    assert objects[1].name == "postgres"
    assert objects[1].namespace == "data-pipeline"


def test_find_missing_required_reports_absent_objects() -> None:
    objects = module.parse_objects(MANIFEST)

    missing = module.find_missing_required(objects)

    assert ("StatefulSet", "metastore-db") in missing
    assert ("StatefulSet", "postgres") not in missing


def test_find_unsuspended_template_jobs_flags_only_template_jobs() -> None:
    manifest = """
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-bronze
  namespace: data-pipeline
spec: {}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: airflow-init
  namespace: data-pipeline
spec: {}
"""

    unsuspended = module.find_unsuspended_template_jobs(module.parse_objects(manifest))

    assert unsuspended == ["spark-bronze"]


def test_find_namespaced_workload_gaps_reports_missing_namespace() -> None:
    manifest = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: trino
---
apiVersion: v1
kind: Service
metadata:
  name: trino
"""

    gaps = module.find_namespaced_workload_gaps(module.parse_objects(manifest))

    assert gaps == ["Deployment/trino"]


def test_find_airflow_migration_wait_gaps_flags_missing_init_container() -> None:
    manifest = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: data-pipeline
spec:
  template:
    spec:
      initContainers:
        - name: wait-for-airflow-postgres
"""

    gaps = module.find_airflow_migration_wait_gaps(module.parse_objects(manifest))

    assert gaps == ["Deployment/airflow-webserver"]


def test_find_dead_service_precondition_envs_flags_ignored_hadoop_env() -> None:
    manifest = """
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: datanode
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: datanode
          env:
            - name: SERVICE_PRECONDITION
              value: namenode:9870
"""

    gaps = module.find_dead_service_precondition_envs(module.parse_objects(manifest))

    assert gaps == ["StatefulSet/datanode/datanode"]


def test_find_airflow_dag_directory_mounts_flags_missing_subpath() -> None:
    manifest = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-scheduler
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: scheduler
          volumeMounts:
            - name: airflow-dags
              mountPath: /opt/airflow/dags
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: webserver
          volumeMounts:
            - name: airflow-dags
              mountPath: /opt/airflow/dags/payments_pipeline.py
              subPath: payments_pipeline.py
"""

    gaps = module.find_airflow_dag_directory_mounts(module.parse_objects(manifest))

    assert gaps == ["Deployment/airflow-scheduler/scheduler"]


def test_find_spark_hadoop_directory_mounts_flags_conf_replacement() -> None:
    manifest = """
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-bronze
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: spark
          volumeMounts:
            - name: hadoop-config
              mountPath: /opt/spark/conf
"""

    gaps = module.find_spark_hadoop_directory_mounts(module.parse_objects(manifest))

    assert gaps == ["Job/spark-bronze"]


def test_find_hdfs_init_readiness_gaps_requires_datanode_wait() -> None:
    manifest = """
apiVersion: batch/v1
kind: Job
metadata:
  name: hdfs-init
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: hdfs-init
          command:
            - /bin/bash
            - -lc
            - hdfs dfs -mkdir -p /warehouse
"""

    gaps = module.find_hdfs_init_readiness_gaps(module.parse_objects(manifest))

    assert gaps == ["Job/hdfs-init"]


def test_find_trino_memory_config_gaps_requires_per_node_caps() -> None:
    manifest = """
apiVersion: v1
kind: ConfigMap
metadata:
  name: trino-etc
  namespace: data-pipeline
data:
  config.properties: |
    coordinator=true
"""

    gaps = module.find_trino_memory_config_gaps(module.parse_objects(manifest))

    assert "ConfigMap/trino-etc:memory.heap-headroom-per-node" in gaps
    assert "ConfigMap/trino-etc:query.max-memory-per-node" in gaps


def test_find_connector_retry_gaps_requires_create_http_error_handling() -> None:
    manifest = """
apiVersion: batch/v1
kind: Job
metadata:
  name: register-postgres-cdc
  namespace: data-pipeline
spec:
  suspend: true
  template:
    spec:
      containers:
        - name: register
          command:
            - python
            - -c
            - urllib.request.urlopen(req)
"""

    gaps = module.find_connector_retry_gaps(module.parse_objects(manifest))

    assert gaps == ["Job/register-postgres-cdc"]


def test_validate_combines_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "REQUIRED_OBJECTS", {("StatefulSet", "postgres")})
    objects = module.parse_objects(
        """
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-gold
  namespace: default
spec: {}
"""
    )

    errors = module.validate(objects)

    assert "Missing required objects" in errors[0]
    assert "Template jobs must be suspended" in errors[1]
    assert "Workloads missing data-pipeline namespace" in errors[2]


def test_render_kustomize_invokes_kubectl(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(command, check, capture_output, text):  # noqa: ANN001
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="kind: List\n")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    rendered = module.render_kustomize(Path("k8s/overlays/local"))

    assert rendered == "kind: List\n"
    assert calls == [["kubectl", "kustomize", "k8s/overlays/local"]]


def test_main_exits_when_validation_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "render_kustomize", lambda: MANIFEST)

    with pytest.raises(SystemExit, match="Missing required objects"):
        module.main()


def test_main_prints_success(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    specialized_objects = {
        ("Deployment", "airflow-webserver"),
        ("Deployment", "airflow-scheduler"),
        ("Job", "hdfs-init"),
        ("Job", "register-postgres-cdc"),
    }
    required_manifest = "\n---\n".join(
        f"""
apiVersion: v1
kind: {kind}
metadata:
  name: {name}
  namespace: data-pipeline
spec:
  suspend: true
"""
        for kind, name in module.REQUIRED_OBJECTS
        if (kind, name) not in specialized_objects
    )
    manifest = required_manifest + """
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: data-pipeline
spec:
  template:
    spec:
      initContainers:
        - name: wait-for-airflow-migrations
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-scheduler
  namespace: data-pipeline
spec:
  template:
    spec:
      initContainers:
        - name: wait-for-airflow-migrations
---
apiVersion: batch/v1
kind: Job
metadata:
  name: hdfs-init
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: hdfs-init
          command:
            - hdfs dfsadmin -report
            - Live datanodes
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: trino-etc
  namespace: data-pipeline
data:
  config.properties: |
    query.max-memory=256MB
    query.max-memory-per-node=128MB
    memory.heap-headroom-per-node=128MB
---
apiVersion: batch/v1
kind: Job
metadata:
  name: register-postgres-cdc
  namespace: data-pipeline
spec:
  suspend: true
  template:
    spec:
      containers:
        - name: register
          command:
            - except urllib.error.HTTPError as create_exc
            - create_exc.code == 409
"""
    monkeypatch.setattr(module, "render_kustomize", lambda: manifest)

    module.main()

    assert "Validated" in capsys.readouterr().out

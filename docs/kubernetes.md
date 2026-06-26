# Local Kubernetes

This project keeps Docker Compose as the fastest local runtime and adds Kubernetes as an optional local orchestration path. The Kubernetes path should grow toward parity with the Compose platform without changing the data contracts described in [design.md](design.md).

## Current Scope

The current Kubernetes path has a full local-platform manifest set:

- `scripts/k8s_up.sh` creates a local `kind` cluster (via the `kind` CLI).
- Kustomize applies the `data-pipeline` namespace.
- Kustomize generates shared ConfigMaps and Secrets from the existing repo config files and local overlay values.
- Kubernetes defines source Postgres, metastore database, HDFS NameNode/DataNode, Hive Metastore, Trino, Kafka (KRaft)/Kafka Connect, connector registration, Spark bronze/silver/gold job templates, Airflow, Prometheus/exporters, Grafana, and Metabase.
- An `hdfs-init` Job prepares local warehouse and checkpoint directories for Spark writes.
- Connector registration and Spark job manifests are suspended by default so they do not run before their dependencies are Ready.

The Kustomize base keeps mechanical copies of selected files from `config/` under `k8s/base/config/` because standard `kubectl apply -k` does not load files outside the kustomization tree.

The kind cluster does not reserve application ports up front. As services are added, use `kubectl port-forward` for the specific UI or API you want to inspect.

## Runtime Status

The manifest set renders and is structurally validated with:

```bash
python scripts/validate_k8s_manifests.py
```

The Kubernetes path has been smoke-tested locally through:

- all core pods Ready in the `data-pipeline` namespace
- HDFS warehouse/checkpoint initialization
- source Postgres seed validation (`124` payments)
- Debezium connector registration with connector and task `RUNNING`
- Bronze, Silver, and Gold Spark Jobs completing successfully
- Trino queries over Iceberg returning Bronze `124`, Silver `124`, and Gold total payments `124`

Docker Compose remains the fastest local runtime. Kubernetes is the local orchestration path for practicing cluster operations and migration patterns.

## Prerequisites

- Docker Desktop
- kind
- kubectl

## Start the Local Cluster

```bash
bash scripts/k8s_up.sh
```

The script runs (idempotently — safe to re-run):

```bash
# create the cluster only if it does not already exist
kind get clusters | grep -qx data-pipeline \
  || kind create cluster --name data-pipeline \
       --config k8s/kind-config.yaml --kubeconfig .kind/kubeconfig --wait 120s

# Jobs have immutable pod templates, so delete any existing ones before re-applying
KUBECONFIG=.kind/kubeconfig kubectl delete jobs --all -n data-pipeline --ignore-not-found

KUBECONFIG=.kind/kubeconfig kubectl apply -k k8s/overlays/local
```

The script also builds and loads the local Airflow, Hive Metastore, and Trino exporter images into the kind cluster.

## Inspect

```bash
export KUBECONFIG=.kind/kubeconfig
kubectl get ns
kubectl get configmaps -n data-pipeline
kubectl get secrets -n data-pipeline
kubectl get deployments,statefulsets,jobs,pods,svc,pvc -n data-pipeline
python scripts/validate_k8s_manifests.py
```

Expected baseline result after dependencies pull and start:

```text
statefulset.apps/postgres        1/1
statefulset.apps/metastore-db    1/1
statefulset.apps/namenode        1/1
statefulset.apps/datanode        1/1
persistentvolumeclaim/...        Bound
```

You can verify the seeded source table with:

```bash
kubectl exec -n data-pipeline postgres-0 -- \
  psql -U dataeng -d payments -c "SELECT COUNT(*) AS payments_count FROM payments;"
```

The expected count is `124` payments from the seed scripts.

Connector registration and Spark job templates are suspended by default. Start or recreate them only after their dependencies are Ready.

```bash
kubectl patch job register-postgres-cdc -n data-pipeline -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete job/register-postgres-cdc -n data-pipeline --timeout=180s

kubectl patch job spark-bronze -n data-pipeline -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete job/spark-bronze -n data-pipeline --timeout=600s

kubectl patch job spark-silver -n data-pipeline -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete job/spark-silver -n data-pipeline --timeout=600s

kubectl patch job spark-gold -n data-pipeline -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete job/spark-gold -n data-pipeline --timeout=600s
```

Validate the serving layer with:

```bash
kubectl exec -n data-pipeline deploy/trino -- \
  trino --execute "SELECT count(*) FROM iceberg.analytics.payments_bronze"
kubectl exec -n data-pipeline deploy/trino -- \
  trino --execute "SELECT count(*) FROM iceberg.analytics.payments_silver"
kubectl exec -n data-pipeline deploy/trino -- \
  trino --execute "SELECT count(*), sum(payment_count) FROM iceberg.analytics.payment_metrics_gold"
```

## Stop

```bash
bash scripts/k8s_down.sh
```

## Next Runtime Hardening Steps

The manifests exist for the full local stack and the manual runtime smoke path succeeds. Harden the operating workflow in this order:

1. Add dependency-aware readiness waits to `scripts/k8s_verify.sh`.
2. Convert the suspended Debezium and Spark Jobs into explicit run commands or verification steps.
3. Replace Docker-oriented Airflow helper scripts with Kubernetes-aware equivalents.
4. Add service-specific port-forward examples for Airflow, Trino, Prometheus, Grafana, and Metabase.
5. Move Spark jobs from local-mode Job templates toward Kubernetes-managed Spark driver/executor pods.

The goal is for Kubernetes to mirror the existing Compose architecture first. After that, Spark jobs can move from local-mode Job templates to Kubernetes-managed Spark driver/executor pods.

## Documentation Rules For New Manifests

Keep this page accurate as the Kubernetes path grows:

- List workloads under current scope only after the manifest exists and `kubectl apply -k k8s/overlays/local` creates it.
- Keep port-forward commands service-specific, because the kind cluster does not reserve fixed application ports.
- Prefer dependency-focused verification steps over broad claims. For example, check that Hive Metastore can reach its database, Trino can query Iceberg metadata, Kafka Connect has the Debezium connector, and Airflow can trigger the same bronze/silver/gold sequence used locally.
- Update [design.md](design.md) only when runtime behavior or limitations change, not for manifest inventory churn.

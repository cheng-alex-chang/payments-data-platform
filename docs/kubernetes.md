# Local Kubernetes

This project keeps Docker Compose as the fastest local runtime and adds Kubernetes as an optional local orchestration path.

The first Kubernetes slice is intentionally small:

- Terraform creates a local `kind` cluster.
- Kustomize applies the `data-pipeline` namespace.
- Kustomize generates ConfigMaps from the existing repo config files.
- Kubernetes runs the source Postgres database as the first StatefulSet workload.

That gives the project a Kubernetes foundation without duplicating service configuration by hand.

The Kustomize base keeps mechanical copies of selected files from `config/` under `k8s/base/config/` because standard `kubectl apply -k` does not load files outside the kustomization tree.

The kind cluster does not reserve application ports up front. As services are added, use `kubectl port-forward` for the specific UI or API you want to inspect.

## Prerequisites

- Docker Desktop
- Terraform
- kind
- kubectl

## Start the Local Cluster

```bash
bash scripts/k8s_up.sh
```

The script runs:

```bash
cd infra/terraform/local-kind
terraform init
terraform apply -auto-approve

KUBECONFIG=.kind/kubeconfig kubectl apply -k k8s/overlays/local
```

## Inspect

```bash
export KUBECONFIG=.kind/kubeconfig
kubectl get ns
kubectl get configmaps -n data-pipeline
kubectl get statefulsets,pods,svc,pvc -n data-pipeline
```

Expected first-slice result:

```text
pod/postgres-0                 1/1 Running
statefulset.apps/postgres      1/1
persistentvolumeclaim/...      Bound
```

You can verify the seeded source table with:

```bash
kubectl exec -n data-pipeline postgres-0 -- \
  psql -U dataeng -d payments -c "SELECT COUNT(*) AS payments_count FROM payments;"
```

The expected count is `124` payments from the seed scripts.

## Stop

```bash
bash scripts/k8s_down.sh
```

## Next Workloads To Add

Add Kubernetes manifests in this order:

1. Metastore database as a StatefulSet.
2. Zookeeper, Kafka, and Kafka Connect.
3. HDFS NameNode and DataNode.
4. Hive Metastore and Trino.
5. Airflow webserver, scheduler, and init Job.
6. Spark submit Jobs.
7. Prometheus, Grafana, and Metabase.

The goal is for Kubernetes to mirror the existing Compose architecture first. After that, Spark jobs can move from `docker exec dp-spark` to Kubernetes-managed Spark driver/executor pods.

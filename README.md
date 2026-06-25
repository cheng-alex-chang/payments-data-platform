# Local CDC Data Platform

A local data engineering project built around:

`Postgres -> Debezium/Kafka Connect -> Kafka -> PySpark -> Iceberg on HDFS -> Trino`

with:

- `Airflow` for orchestration
- `Hive Metastore` as the Iceberg catalog
- `Metabase` for ad hoc dashboards
- `Prometheus + Grafana` for platform monitoring and a seeded payments demo dashboard
- `pytest` for tests

See [docs/design.md](docs/design.md) for layer contracts, incremental processing, CDC delete handling, and known limitations.

## Deployment targets

The same medallion contract (bronze → silver → gold, CDC-aware) runs three ways:

| Target | What it is | Entry point |
|--------|------------|-------------|
| **Docker Compose** | Fastest local runtime — the full stack with one command | `docker compose up -d` |
| **Kubernetes (kind)** | The platform as Kustomize manifests (HDFS, Hive Metastore, Trino, Kafka/Debezium, Spark, Airflow, observability) on a `kind` cluster | `bash scripts/k8s_up.sh` |
| **Databricks (Lakeflow)** | Serverless Unity Catalog + Delta port as a Lakeflow Declarative Pipeline — Auto Loader, AUTO CDC, expectations — deployed via an Asset Bundle | [`databricks/`](databricks/README.md) |

## Architecture

```text
Postgres
  -> Debezium / Kafka Connect            CDC via logical replication
  -> Kafka topic: cdc.public.payments
  -> Spark bronze job                    Structured Streaming -> Iceberg append
  -> Spark silver job                    foreachBatch -> Iceberg MERGE / DELETE
  -> Spark gold job                      Batch SQL -> Iceberg INSERT OVERWRITE
  -> Trino                               SQL over Iceberg via Hive Metastore
```

## Bronze, Silver, Gold

`Bronze`
- Raw Kafka envelope written to Iceberg, append-only
- Streaming with `trigger(availableNow=True)` and HDFS checkpoint
- PII fields (`shopper_id`) hashed with SHA-256 before the write so PII never lands in the lakehouse

`Silver`
- Canonical payment records, typed and normalised
- Dedup by `payment_id` (latest `updated_at`, tiebroken by `kafka_offset`) before MERGE so reruns and full Kafka replays converge to the same current state
- `foreachBatch` issues `MERGE INTO` for upserts (`op` in c, u, r) and `DELETE FROM` for Debezium deletes (`op=d`)

`Gold`
- Hourly aggregates per country and payment method (count, gross volume, auth rate)
- Reads only silver — strictly linear `bronze → silver → gold` lineage, no bronze or Debezium-envelope access
- Full idempotent recompute: one `GROUP BY` over silver, `INSERT OVERWRITE` replaces the whole table every run (so hours emptied by deletes drop out)

## Repo Layout

```text
airflow/dags/                  Airflow DAGs
config/airflow/                Airflow Docker image
config/connect/                Debezium connector config
config/grafana/                Grafana provisioning and dashboards
config/hadoop/                 Hadoop config
config/hive-metastore/         Hive metastore config
config/postgres/init/          Postgres schema and seed data
config/prometheus/             Prometheus config
config/spark/jobs/             Spark jobs
config/statsd/                 StatsD exporter mapping for Airflow metrics
config/trino/                  Trino config and Iceberg catalog
config/trino-exporter/         Custom Trino REST -> Prometheus exporter
databricks/                    Databricks Lakeflow (DLT) port + Asset Bundle
docs/                          Design docs
infra/terraform/databricks/    Terraform for Databricks Unity Catalog governance (schema, volume, grants)
k8s/                           Kubernetes manifests, local overlay, and kind cluster config
scripts/                       Helper scripts
sql/trino/                     Trino validation SQL
tests/                         Unit tests
```

## Run Locally

### Prerequisites

- `Docker Desktop`
- `docker compose`
- `Python 3`

### Start the platform

```bash
docker compose up -d
```

All long-running services use `restart: unless-stopped` and expose healthchecks (Kafka, Zookeeper, NameNode, DataNode, Trino, Airflow, Postgres variants). Dependent services wait on `condition: service_healthy` before starting, so the stack self-recovers from individual container crashes without manual intervention.

### Register or refresh the Debezium connector

```bash
bash scripts/register_connector.sh
```

### Main URLs

- Airflow: `http://localhost:8088`
- Kafka Connect: `http://localhost:8083`
- Trino: `http://localhost:8080`
- HDFS NameNode UI: `http://localhost:9870`
- Metabase: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001`

### Load richer demo data

Fresh environments get the expanded demo seed automatically. If your containers and volumes already exist, reload the sample dataset into Postgres with:

```bash
python3 scripts/load_demo_data.py
```

Then trigger the Airflow DAG again so bronze, silver, and gold pick up the new CDC events.

### Run tests

```bash
source .venv/bin/activate
pytest --cov --cov-report=term-missing
```

## Optional Local Kubernetes

Docker Compose remains the fastest local runtime. A no-cost Kubernetes path is also available for local infrastructure and orchestration workflows: `scripts/k8s_up.sh` creates a local `kind` cluster and applies the Kustomize overlay. The overlay renders the full platform shape, including stateful databases, HDFS, Hive Metastore, Trino, Kafka/Connect, Spark job templates, Airflow, observability, and analytics UI workloads.

```bash
bash scripts/k8s_up.sh
export KUBECONFIG=.kind/kubeconfig
kubectl get statefulsets,pods,svc,pvc -n data-pipeline
python scripts/validate_k8s_manifests.py
```

Stop and remove the local cluster with:

```bash
bash scripts/k8s_down.sh
```

See [docs/kubernetes.md](docs/kubernetes.md) for the current Kubernetes scope, verification commands, and remaining runtime caveats.

The Kubernetes path has been verified end-to-end locally — Debezium connector registration, Bronze/Silver/Gold Spark Jobs, and Trino row-count validation all reconcile (124 → 124 → 124).

## Databricks (Lakeflow Declarative Pipeline)

The medallion is also ported to **Databricks** as a serverless **Lakeflow Declarative Pipeline** on Unity Catalog + Delta: Auto Loader ingestion, `apply_changes` (AUTO CDC) for the silver upsert/delete, and expectations for data quality — deployed as a Databricks Asset Bundle and orchestrated by a Workflow (seed → pipeline → validate).

Governance (the `analytics` schema and the `landing` volume) is provisioned declaratively with **Terraform** — a clean infra-vs-workload split: Terraform owns the Unity Catalog objects, the Asset Bundle owns the pipeline and Workflow. Terraform must `apply` first, because the seed job no longer self-creates the schema/volume:

```bash
terraform -chdir=infra/terraform/databricks init
terraform -chdir=infra/terraform/databricks apply   # creates workspace.analytics + landing volume

cd databricks
databricks bundle deploy -t dev
databricks bundle run payments_pipeline -t dev
```

Verified on Databricks Free Edition: the Delta tables reconcile 124 → 124 → 124 and all silver expectations report 124 passed / 0 failed. **Free Edition caveat:** `workspace` is a built-in catalog (not created by Terraform) and grants are restricted, so the Terraform scope is the schema + volume; the full grants showcase (`-var 'enable_grants=true'`) needs a standard workspace. See [databricks/README.md](databricks/README.md) for setup, the architecture diagram, and design notes.

## Airflow Pipeline

The main DAG is `airflow/dags/payments_pipeline.py`.

It runs:

1. `init_hdfs`
2. `validate_connector`
3. `bronze_load`
4. `silver_transform`
5. `gold_transform`
6. `publish_trino_tables`
7. `validate_trino`

Manual trigger:

```bash
docker exec dp-airflow-webserver airflow dags trigger payments_pipeline
```

## Demo Flow

### 1. Show the source data in Postgres

```bash
docker exec dp-postgres psql -U dataeng -d payments -c "SELECT payment_id, amount, payment_status, updated_at FROM payments ORDER BY payment_id;"
```

### 2. Change a source row

```bash
docker exec dp-postgres psql -U dataeng -d payments -c "UPDATE payments SET amount = 149.99, payment_status = 'authorized', updated_at = NOW() WHERE payment_id = 1001;"
```

### 3. Show the CDC event in Kafka

```bash
docker exec dp-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic cdc.public.payments \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000
```

### 4. Trigger the Airflow DAG

```bash
docker exec dp-airflow-webserver airflow dags trigger payments_pipeline
```

### 5. Confirm the DAG run

```bash
docker exec dp-airflow-webserver airflow dags list-runs -d payments_pipeline
```

### 6. Query the silver table in Trino

```bash
docker exec dp-trino trino --execute "SELECT payment_id, amount, payment_method, payment_status, created_at, updated_at FROM iceberg.analytics.payments_silver"
```

### 7. Query the gold table in Trino

```bash
docker exec dp-trino trino --execute "SELECT * FROM iceberg.analytics.payment_metrics_gold ORDER BY payment_hour, country_code, payment_method"
```

## Visualization

Grafana provisions a `Payments Demo Overview` dashboard. The six payment-aggregate panels read the curated **gold** layer (`iceberg.analytics.payment_metrics_gold`) through **Trino**, so the dashboard reflects the pipeline's actual output instead of querying the source database directly. The two refund panels still read source Postgres — `refunds` is tracked for schema-drift only and has no silver/gold layer yet (a separate refunds medallion is future work). Open `http://localhost:3001`, go to the `Data Platform` folder, and you should see charts for:

- total payments _(gold)_
- gross volume _(gold)_
- authorization rate _(gold, payment-weighted across hourly groups)_
- refund events _(source Postgres)_
- hourly volume trend _(gold)_
- payment method mix _(gold)_
- gross volume by country _(gold)_
- refunds over time _(source Postgres)_

Two states are expected and **not** bugs:
- **On first container start** Grafana downloads the `trino-datasource` plugin (`GF_INSTALL_PLUGINS`), which needs internet access.
- **Until the `payments_pipeline` DAG has run**, gold is empty, so the six gold panels render blank. That is by design — the dashboard now mirrors pipeline output, so no run means no data. Trigger the DAG and the panels populate.

## Project Tour

This project is easiest to understand when viewed from three angles:

- orchestration in Airflow
- CDC and analytics results in Kafka and Trino
- business-facing metrics in Grafana

Airflow shows the pipeline running from source validation through bronze, silver, gold, and downstream validation.

![Airflow DAG showing the payments CDC pipeline run](docs/images/airflow-payments-pipeline.png)

Grafana shows the seeded demo data as business-facing metrics, including volume, authorization rate, refunds, and payment method mix.

![Grafana dashboard showing payment volume, authorization rate, refunds, and payment mix](docs/images/grafana-payments-demo-overview.png)

Trino shows the materialized gold layer directly, making it easy to inspect the hourly aggregates produced by the pipeline.

![Trino query results for the payment_metrics_gold Iceberg table](docs/images/trino-gold-metrics-query.png)

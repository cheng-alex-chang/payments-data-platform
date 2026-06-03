# Payments Pipeline on Databricks (Free Edition)

A serverless, Unity Catalog + Delta port of the local payments medallion
pipeline. It preserves the data contract from [../docs/design.md](../docs/design.md)
— the seed files carry Debezium-shaped envelopes so the Silver/Gold logic is a
1:1 port of [../config/spark/jobs](../config/spark/jobs) — while swapping the
infrastructure for what Databricks Free Edition provides.

## What maps to what

| Local (Compose / Kubernetes) | Databricks Free Edition |
|---|---|
| Postgres + Debezium + Kafka CDC | `01_seed_to_volume.py` writes Debezium `op='r'` envelopes to a UC Volume |
| Bronze reads the Kafka topic | Bronze reads the Volume with **Auto Loader** (`cloudFiles`) |
| Iceberg on HDFS | **Delta** managed tables in **Unity Catalog** |
| Hive Metastore + Trino | Unity Catalog (ambient `spark`) |
| Airflow DAG | **Databricks Workflow** (`resources/payments_pipeline.job.yml`) |
| Terraform + kind | **Databricks Asset Bundle** (`databricks.yml`) |
| Trino row-count validation | `05_validate.py` |

Tables land in `workspace.analytics`:
`payments_bronze`, `payments_silver`, `payments_silver_dlq`, `payment_metrics_gold`.
Seed files + streaming checkpoints live under the `workspace.analytics.landing` Volume.

## Layout

```
databricks/
  databricks.yml                       bundle config (set your workspace host)
  resources/payments_pipeline.job.yml  the Workflow (one serverless notebook task)
  src/payments_pipeline.py             the notebook: setup -> seed -> bronze ->
                                       silver -> gold -> validate (all stages)
  src/common.py                        pure-Python reference of the seed/mask/
                                       constants, unit-tested off-cluster
```

### Why one notebook instead of a task-per-stage

The natural design is six tasks (`setup`, `seed`, `bronze`, `silver`, `gold`,
`validate`) wired in a Workflow DAG. On **Free Edition serverless** that fails
intermittently: both `spark_python_task` (which `exec()`s the file) and a runtime
`import common` read the workspace `.py` over a FUSE mount that throws
`OSError [Errno 5]` at random. A notebook's source is delivered by the notebook
service (not FUSE), so consolidating every stage into one notebook is reliable.
`common.py` is kept as the tested, single-source reference that the notebook mirrors.

## Run it

1. Create a **Databricks Free Edition** account (free, serverless): https://www.databricks.com/learn/free-edition
2. Install the **new** Databricks CLI (v0.205+, the one with `bundle`; *not*
   the legacy `pip install databricks-cli`) and log in:
   ```bash
   brew tap databricks/tap && brew install databricks   # macOS
   # or: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   databricks auth login --host https://<your-workspace>.cloud.databricks.com
   ```
3. Set your workspace host in [databricks.yml](databricks.yml) (`targets.dev.workspace.host`).
4. Deploy and run the bundle:
   ```bash
   cd databricks
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run payments_pipeline -t dev
   ```
5. Inspect the results:
   - **Catalog Explorer** → `workspace.analytics` for the four Delta tables.
   - **SQL Editor**:
     ```sql
     SELECT count(*) FROM workspace.analytics.payments_bronze;        -- 124
     SELECT count(*) FROM workspace.analytics.payments_silver;        -- 124
     SELECT count(*), sum(payment_count)
     FROM workspace.analytics.payment_metrics_gold;                   -- sum = 124
     ```

The notebook's final cell asserts those counts, so a green run already proves the
124 → 124 → 124 reconciliation (DLQ empty).

### Manual fallback (no CLI)

Import `src/payments_pipeline.py` into the workspace as a notebook and Run All on
serverless. It is fully self-contained (no imports of sibling files), so it needs
nothing else deployed.

## Tests

Pure-Python helpers (seed math, PII masking, envelope shape) are unit-tested
without Spark:

```bash
pytest tests/test_databricks_helpers.py
```

## Notes / limits

- Free Edition is **serverless only** — there is no Kafka/Debezium, so CDC is
  simulated by seed files. The envelope shape is identical, so moving to a real
  source later (Auto Loader from a CDC export, or Lakeflow Connect) would not
  touch Silver/Gold.
- Tables use **Liquid Clustering** (`CLUSTER BY`) instead of Iceberg's
  `days(col)` hidden partitioning.
- Re-running is idempotent: the seed overwrites one file path and Auto Loader
  tracks processed files, so Bronze will not double-ingest.

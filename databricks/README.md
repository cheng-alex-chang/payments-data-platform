# Payments Pipeline on Databricks (Free Edition)

A serverless, Unity Catalog + Delta port of the local payments medallion pipeline,
expressed as a **Lakeflow Declarative Pipeline (DLT)**. It preserves the data
contract from [../docs/design.md](../docs/design.md) — the seed carries
Debezium-shaped envelopes so the Silver/Gold transforms match
[../config/spark/jobs](../config/spark/jobs) — while swapping the infrastructure
for what Databricks provides.

## Architecture

```
  Databricks Asset Bundle (databricks.yml)  ──deploy──▶  Workspace
                                                            │
   ┌──────────────────── Workflow: payments-medallion ──────────────────────┐
   │                                                                         │
   │   seed ───────────▶  medallion  ───────────▶  validate                 │
   │  (notebook)        (DLT pipeline)             (notebook)                │
   └────────┼───────────────┼────────────────────────┼──────────────────────┘
            │               │                         │
   write 124 Debezium       │                  assert 124/124/124
   'r' envelopes            ▼
            ▼     ┌──────── Lakeflow Declarative Pipeline ─────────┐
   /Volumes/.../  │  Bronze ───────▶ Silver ───────▶ Gold          │
     landing  ───▶│  Auto Loader     AUTO CDC         hourly        │
   (JSONL files)  │  + PII mask      + expectations   metrics       │
                  └────────────────────────────────────────────────┘
                     Unity Catalog · Delta · workspace.analytics
```

## What maps to what

| Local (Compose / Kubernetes) | Databricks |
|---|---|
| Postgres + Debezium + Kafka CDC | `seed_to_volume.py` writes Debezium `op='r'` envelopes to a UC Volume |
| Bronze reads the Kafka topic | DLT Bronze reads the Volume with **Auto Loader** (`cloudFiles`) |
| Spark `MERGE` upsert + `op='d'` delete | DLT **AUTO CDC** (`apply_changes`, SCD type 1) |
| Hand-coded data-quality checks + DLQ | DLT **expectations** (`expect_all_or_drop`) tracked in the pipeline UI |
| Iceberg on HDFS / Hive Metastore / Trino | **Delta** tables in **Unity Catalog** |
| Airflow DAG | **Databricks Workflow** orchestrating seed → DLT pipeline → validate |
| Terraform + kind | **Databricks Asset Bundle** (`databricks.yml`) |
| Trino row-count validation | `validate_counts.py` |

Tables publish to `workspace.analytics`:
`payments_bronze`, `payments_silver`, `payment_metrics_gold`.
The seed lands under the `workspace.analytics.landing` Volume.

## Layout

```
databricks/
  databricks.yml                       bundle config (set your workspace host)
  resources/payments_pipeline.job.yml  Workflow (seed -> DLT pipeline -> validate)
                                       + the DLT pipeline resource
  src/seed_to_volume.py                land 124 Debezium envelopes in the Volume
  src/dlt_pipeline.py                  the Lakeflow pipeline: bronze / silver / gold
  src/validate_counts.py               reconcile 124 / 124 / 124
  src/common.py                        pure-Python reference (seed/mask/constants),
                                       unit-tested off-cluster
```

### Design notes

- **Why DLT.** The medallion is declarative: `@dlt.table` for bronze/gold, a
  parsed change-feed view, and `apply_changes` for the Silver upsert/delete. The
  data-quality rules become **expectations**, so DLT reports passed/dropped
  records and renders a lineage graph — the modern, idiomatic Databricks pattern.
- **Why the seed/validate tasks are self-contained.** Free Edition serverless
  intermittently fails to read sibling workspace `.py` files over its FUSE mount
  (`OSError [Errno 5]`), which breaks runtime `import`. The notebook/DLT sources
  are delivered by the notebook service (not FUSE), so each file is kept
  self-contained. `common.py` remains the tested single-source reference that
  the seed mirrors.

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
   - **Pipelines** → the DLT pipeline for the lineage graph + per-expectation
     pass/drop counts.
   - **Catalog Explorer** → `workspace.analytics` for the three Delta tables.
   - **SQL Editor**:
     ```sql
     SELECT count(*) FROM workspace.analytics.payments_bronze;        -- 124
     SELECT count(*) FROM workspace.analytics.payments_silver;        -- 124
     SELECT count(*), sum(payment_count)
     FROM workspace.analytics.payment_metrics_gold;                   -- sum = 124
     ```

The `validate` task asserts those counts, so a green run already proves the
124 → 124 → 124 reconciliation.

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
  touch the DLT transforms.
- The Silver upsert is SCD type 1 keyed on `payment_id`, sequenced by
  `updated_at`, with `op='d'` applied as deletes — matching the local MERGE/delete.
- Re-running is idempotent: the seed overwrites one file path and Auto Loader
  tracks processed files, so Bronze will not double-ingest.
- If you previously created non-DLT tables in `workspace.analytics`, drop them
  first — a DLT pipeline will not publish over tables it does not own.

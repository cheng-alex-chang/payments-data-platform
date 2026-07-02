# Production-Readiness Notes

The pipelines are verified end-to-end (offline tests + one live Snowflake/S3 run), but a few
things would harden them for a real production deployment. These are deliberate, known trade-offs
for a portfolio project on trial infrastructure — not oversights. The streaming/CDC half is
already solid here: its DAG authenticates through an Airflow Connection and k8s uses real `Secret`
objects.

## Security

- **AWS credentials via env vars → IAM roles.** The S3 client relies on `AWS_*`; production should
  use IAM roles (instance profile / IRSA / `AssumeRole`) rather than long-lived access keys.
- ~~Snowflake password auth~~ **Done:** key-pair auth is now the default path — the connector uses
  `SNOWFLAKE_PRIVATE_KEY_PATH` (password only as fallback), dbt has a `trial_keypair` target, and
  the Terraform provider documents `SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT`.

## Operations

- ~~DAG failure alerting~~ **Done:** both DAGs wire `on_failure_callback` to a webhook notifier
  (`airflow/dags/alerts.py`) — POSTs to `ALERT_WEBHOOK_URL` (Slack-compatible) when set, logs a
  warning and no-ops when unset, and never raises.
- ~~Remote Terraform state~~ **Done:** the Snowflake module's state lives in a versioned,
  public-access-blocked S3 bucket with S3-native locking (`use_lockfile`, TF ≥ 1.10 — no
  DynamoDB table). The state bucket itself is bootstrapped outside Terraform (chicken-and-egg).
  The Databricks module stays on local state (free-edition scope).

## Data

- **Schema / data-contract enforcement.** The VARIANT load plus the staging cast can silently
  null-cast if the upstream schema drifts; production would enforce an explicit contract at the
  ingestion boundary.

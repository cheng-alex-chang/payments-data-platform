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

- **DAG failure alerting.** Neither DAG sets `on_failure_callback`, SLAs, or paging/Slack alerts —
  a production pipeline needs failures to be noticed, not silent.
- **Remote Terraform state.** State is local; a team/production setup needs a remote backend
  (S3 + DynamoDB lock) so applies are shared and locked.

## Data

- **Schema / data-contract enforcement.** The VARIANT load plus the staging cast can silently
  null-cast if the upstream schema drifts; production would enforce an explicit contract at the
  ingestion boundary.

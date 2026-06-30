# Production-Readiness Backlog

Gaps flagged during the Snowflake ELT build (deferred, to revisit). The streaming/CDC half is
in good shape: its DAG already authenticates through an Airflow Connection
(`AIRFLOW_CONN_SOURCE_POSTGRES`), Compose injects every password from env (nothing hardcoded),
and k8s uses real `Secret` objects (`secretKeyRef`). The items below are mostly on the new
Snowflake/S3 path and a few cross-cutting concerns.

## P0 — required before real production

1. **Snowflake auth is password-based.** `load_to_snowflake.connect_from_env` uses
   `SNOWFLAKE_PASSWORD`. Production should use **key-pair auth** (or OAuth/SSO); Snowflake is
   phasing out single-factor password auth.
2. **AWS credentials via env vars.** `stage_to_s3.s3_client_from_env` relies on `AWS_*`. Use
   **IAM roles** instead (instance profile / IRSA / `AssumeRole`), not long-lived access keys.
3. **Dependencies are unpinned.** `requirements-ci.txt` has no version constraints, so builds
   aren't reproducible and an upstream release can break CI silently. Pin versions (or add a
   lockfile / `constraints.txt`).

## P1 — important

4. **Credential mechanism is split in the Snowflake DAG.** SQL steps use the Airflow Connection
   (`snowflake_default`), but the staging `@task` still uses env-based S3 creds and the Python
   modules use `connect_from_env`. Unify on Airflow Connections/Hooks (add an AWS connection for
   S3) so there's one credential path.
5. **Coverage gate excludes `snowflake_etl`.** CI's `--cov-fail-under=80` only measures
   `config/spark/jobs`, `scripts`, and `databricks/src/common.py`. Add `--cov=snowflake_etl/src`
   so the new package's coverage is actually enforced.
6. **No DAG failure alerting.** Neither DAG sets `on_failure_callback`, SLAs, or email/Slack
   notifications. Add alerting so prod failures are noticed.
7. **`apache-airflow-providers-snowflake` is undeclared.** The DAG imports it but no manifest
   pins it; it relies on the Airflow image having it. Pin it in the image/constraints and
   document the `snowflake_default` connection setup.

## P2 — hardening / nice-to-have

8. **FX source resilience.** Frankfurter is a single free dependency with no SLA. Add response
   caching and a fallback/paid source for production.
9. **COPY idempotency window.** Replay-safety relies on Snowflake's ~64-day load history;
   document/guard against re-loading a file beyond that window.
10. **Schema / data contract.** The VARIANT load + staging cast can silently null-cast on
    upstream schema drift. Add explicit schema validation at the boundary.
11. **Terraform remote state.** Phase 6 should use a remote backend (S3 + DynamoDB lock), not
    local state, for collaborative/production use.
12. **CDC Spark tasks use `BashOperator` + `spark-submit`.** Optional upgrade to
    `SparkSubmitOperator` for native Airflow integration — not a correctness gap.

# Snowflake authenticates from the standard SNOWFLAKE_* env vars
# (SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD). No secrets live in the
# config. Production should switch to key-pair auth -- see docs/production-readiness.md.
provider "snowflake" {
  role = var.snowflake_admin_role
}

# AWS authenticates from the standard chain (env vars, shared config, or an
# instance/SSO role). Only the region is pinned here.
provider "aws" {
  region = var.aws_region
}

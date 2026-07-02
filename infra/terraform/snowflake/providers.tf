# Snowflake reads credentials from env vars. The 1.x provider wants the account *split*:
# SNOWFLAKE_ORGANIZATION_NAME + SNOWFLAKE_ACCOUNT_NAME (the two halves of the ORG-ACCOUNT
# identifier), plus SNOWFLAKE_USER. No secrets live in the config.
# Key-pair auth (preferred; the user's RSA public key is registered via ALTER USER):
#   export SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
#   export SNOWFLAKE_PRIVATE_KEY=$(cat ~/.snowflake/rsa_key.p8)
# Password fallback: export SNOWFLAKE_PASSWORD=...
provider "snowflake" {
  role = var.snowflake_admin_role

  # The storage integration + external stage are still "preview" resources in the 1.x
  # provider, so they must be explicitly opted into.
  preview_features_enabled = [
    "snowflake_storage_integration_resource",
    "snowflake_stage_resource",
  ]
}

# AWS authenticates from the standard chain (env vars, shared config, or an
# instance/SSO role). Only the region is pinned here.
provider "aws" {
  region = var.aws_region
}

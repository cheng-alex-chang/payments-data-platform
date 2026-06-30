# Snowflake warehouse-side governance + the AWS<->Snowflake wiring for the S3 lake.
# Mirrors infra/terraform/databricks/: Terraform owns the database, schemas, warehouse,
# a functional role with grants, the S3 bucket, and the storage-integration/stage that let
# COPY INTO read the bucket. The pipeline code (loader/DAG) owns the data, not the infra.

# ---------------------------------------------------------------------------
# Warehouse side: database, schemas, compute, functional role
# ---------------------------------------------------------------------------

resource "snowflake_database" "payments" {
  name    = var.database
  comment = "Payments warehouse: RAW landing + ANALYTICS marts. Provisioned by Terraform."
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.payments.name
  name     = "RAW"
  comment  = "VARIANT landing tables loaded by COPY INTO from the S3 stage."
}

resource "snowflake_schema" "analytics" {
  database = snowflake_database.payments.name
  name     = "ANALYTICS"
  comment  = "Typed staging views, the FX dimension, the USD fact, and aggregates."
}

resource "snowflake_warehouse" "etl" {
  name                = var.warehouse
  warehouse_size      = "XSMALL"
  auto_suspend        = 60 # seconds idle before suspend -- keeps a trial near $0
  auto_resume         = true
  initially_suspended = true
  comment             = "FX ELT compute."
}

resource "snowflake_account_role" "etl" {
  name    = var.etl_role
  comment = "Functional role for the FX ELT pipeline."
}

# Least-privilege grants: operate the warehouse, use the database, and read/build in its schemas.
resource "snowflake_grant_privileges_to_account_role" "warehouse" {
  account_role_name = snowflake_account_role.etl.name
  privileges        = ["USAGE", "OPERATE"]

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.etl.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database" {
  account_role_name = snowflake_account_role.etl.name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.payments.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schemas" {
  account_role_name = snowflake_account_role.etl.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]

  on_schema {
    all_schemas_in_database = snowflake_database.payments.name
  }
}

# ---------------------------------------------------------------------------
# AWS side: the S3 lake bucket + the IAM role Snowflake assumes to read it
# ---------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "lake" {
  bucket = var.s3_bucket
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The role ARN is computed independently (account id + name) so the storage integration can
# reference it WITHOUT depending on the role resource. That breaks the otherwise-circular trust:
# the role trusts the integration's external id, which only exists after the integration is made.
locals {
  snowflake_iam_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.iam_role_name}"
}

resource "snowflake_storage_integration" "lake" {
  name                      = "PAYMENTS_LAKE_INT"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = local.snowflake_iam_role_arn
  storage_allowed_locations = ["s3://${var.s3_bucket}/"]
  comment                   = "Lets Snowflake read the S3 raw lake for COPY INTO."
}

# Trust policy: allow Snowflake's generated IAM user to assume the role, scoped to the external
# id the integration hands back (so only this integration can assume it).
data "aws_iam_policy_document" "snowflake_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = [snowflake_storage_integration.lake.storage_aws_iam_user_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [snowflake_storage_integration.lake.storage_aws_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake" {
  name               = var.iam_role_name
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume.json
}

# Read-only access to exactly this bucket.
data "aws_iam_policy_document" "lake_read" {
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
}

resource "aws_iam_role_policy" "lake_read" {
  name   = "lake-read"
  role   = aws_iam_role.snowflake.id
  policy = data.aws_iam_policy_document.lake_read.json
}

# ---------------------------------------------------------------------------
# The external stage the loader / DAG COPY INTO from
# ---------------------------------------------------------------------------

resource "snowflake_stage" "lake" {
  name                = var.stage_name
  database            = snowflake_database.payments.name
  schema              = snowflake_schema.raw.name
  storage_integration = snowflake_storage_integration.lake.name
  url                 = "s3://${var.s3_bucket}/"
  comment             = "External stage over the raw lake; loader COPY INTO @PAYMENTS_LAKE_STAGE."
}

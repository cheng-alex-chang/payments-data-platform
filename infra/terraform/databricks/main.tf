# Unity Catalog governance for the Databricks port: Terraform owns the schema,
# the landing volume, and (optionally) grants. The Asset Bundle owns the workload
# (the DLT pipeline + Workflow). The `workspace` catalog is built in on Free
# Edition and is intentionally NOT managed here.

resource "databricks_schema" "analytics" {
  catalog_name = var.catalog
  name         = var.schema
  comment      = "Payments medallion schema (bronze/silver/gold). Provisioned by Terraform."
}

resource "databricks_volume" "landing" {
  catalog_name = var.catalog
  schema_name  = databricks_schema.analytics.name
  name         = var.volume
  volume_type  = "MANAGED"
  comment      = "Landing volume for seeded Debezium envelopes consumed by the DLT bronze table."
}

# Grants are gated: Free Edition restricts GRANT, so they are off by default.
# Enable on a standard workspace with -var 'enable_grants=true'.
resource "databricks_grants" "schema" {
  count  = var.enable_grants ? 1 : 0
  schema = databricks_schema.analytics.id

  grant {
    principal  = var.grant_principal
    privileges = ["USE_SCHEMA"]
  }
}

resource "databricks_grants" "volume" {
  count  = var.enable_grants ? 1 : 0
  volume = databricks_volume.landing.id

  grant {
    principal  = var.grant_principal
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}

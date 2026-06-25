output "schema_full_name" {
  description = "Fully qualified schema name (catalog.schema)."
  value       = "${var.catalog}.${databricks_schema.analytics.name}"
}

output "volume_full_name" {
  description = "Fully qualified volume name (catalog.schema.volume)."
  value       = "${var.catalog}.${databricks_schema.analytics.name}.${databricks_volume.landing.name}"
}

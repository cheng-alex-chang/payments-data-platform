# Authenticates via the standard Databricks chain: DATABRICKS_HOST / DATABRICKS_TOKEN
# environment variables if set, otherwise the named profile in ~/.databrickscfg.
provider "databricks" {
  profile = var.databricks_config_profile
}

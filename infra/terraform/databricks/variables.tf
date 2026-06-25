variable "databricks_config_profile" {
  description = "Profile in ~/.databrickscfg used to authenticate. Ignored when DATABRICKS_HOST/DATABRICKS_TOKEN env vars are set."
  type        = string
  default     = "free-edition"
}

variable "catalog" {
  description = "Unity Catalog catalog. On Free Edition this is the built-in 'workspace' catalog; Terraform does not create it."
  type        = string
  default     = "workspace"
}

variable "schema" {
  description = "Schema holding the medallion tables and the landing volume."
  type        = string
  default     = "analytics"
}

variable "volume" {
  description = "Managed volume the seed job writes Debezium envelopes into."
  type        = string
  default     = "landing"
}

variable "enable_grants" {
  description = "Create UC grants on the schema and volume. Off by default because Databricks Free Edition restricts grants; enable on a standard workspace."
  type        = bool
  default     = false
}

variable "grant_principal" {
  description = "Principal (group or user) granted schema/volume access when enable_grants is true."
  type        = string
  default     = "account users"
}

variable "cluster_name" {
  description = "Name of the local kind cluster."
  type        = string
  default     = "data-pipeline"
}

variable "kubeconfig_path" {
  description = "Path where kind should write the kubeconfig for this cluster."
  type        = string
  default     = "../../../.kind/kubeconfig"
}

variable "wait_for_ready_timeout" {
  description = "How long to wait for the kind control plane to become ready."
  type        = string
  default     = "120s"
}

output "cluster_name" {
  description = "Local kind cluster name."
  value       = var.cluster_name
}

output "kubeconfig_path" {
  description = "Kubeconfig path for kubectl commands."
  value       = local.kubeconfig_abs_path
}

output "kubectl_context" {
  description = "kubectl context created by kind."
  value       = "kind-${var.cluster_name}"
}

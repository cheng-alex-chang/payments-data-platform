locals {
  repo_root           = abspath("${path.module}/../../..")
  kind_config_path    = abspath("${path.module}/kind-config.yaml")
  kubeconfig_abs_path = abspath("${path.module}/${var.kubeconfig_path}")
}

resource "null_resource" "kind_cluster" {
  triggers = {
    cluster_name    = var.cluster_name
    kind_config_sha = filesha256(local.kind_config_path)
    kubeconfig_path = local.kubeconfig_abs_path
    repo_root       = local.repo_root
  }

  provisioner "local-exec" {
    working_dir = local.repo_root
    command     = "mkdir -p .kind && kind get clusters | grep -qx ${var.cluster_name} || kind create cluster --name ${var.cluster_name} --config ${local.kind_config_path} --kubeconfig ${local.kubeconfig_abs_path} --wait ${var.wait_for_ready_timeout}"
  }

  provisioner "local-exec" {
    when        = destroy
    working_dir = self.triggers.repo_root
    command     = "kind delete cluster --name ${self.triggers.cluster_name}"
  }
}

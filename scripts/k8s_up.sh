#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT_DIR}/infra/terraform/local-kind"
KUBECONFIG_PATH="${ROOT_DIR}/.kind/kubeconfig"

for command in terraform kind kubectl; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Missing required command: ${command}" >&2
    exit 1
  fi
done

cd "${TF_DIR}"
terraform init
terraform apply -auto-approve

cd "${ROOT_DIR}"
KUBECONFIG="${KUBECONFIG_PATH}" kubectl apply -k k8s/overlays/local

echo
echo "Local Kubernetes foundation is ready."
echo "Use: export KUBECONFIG=${KUBECONFIG_PATH}"
echo "Then: kubectl get all -n data-pipeline"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT_DIR}/infra/terraform/local-kind"

for command in terraform kind; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Missing required command: ${command}" >&2
    exit 1
  fi
done

CLUSTER_NAME="${CLUSTER_NAME:-data-pipeline}"

cd "${TF_DIR}"
terraform destroy -auto-approve || terraform_status=$?

if kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  kind delete cluster --name "${CLUSTER_NAME}"
fi

exit "${terraform_status:-0}"

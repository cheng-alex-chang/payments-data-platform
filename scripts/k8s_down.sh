#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT_DIR}/infra/terraform/local-kind"

if ! command -v terraform >/dev/null 2>&1; then
  echo "Missing required command: terraform" >&2
  exit 1
fi

cd "${TF_DIR}"
terraform destroy -auto-approve

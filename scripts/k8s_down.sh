#!/usr/bin/env bash
set -euo pipefail

for command in kind; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Missing required command: ${command}" >&2
    exit 1
  fi
done

CLUSTER_NAME="${CLUSTER_NAME:-data-pipeline}"

if kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  kind delete cluster --name "${CLUSTER_NAME}"
fi

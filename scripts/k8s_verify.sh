#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KUBECONFIG_PATH="${ROOT_DIR}/.kind/kubeconfig"

for command in kubectl; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Missing required command: ${command}" >&2
    exit 1
  fi
done

KUBECTL=(kubectl --kubeconfig "${KUBECONFIG_PATH}")

"${KUBECTL[@]}" get namespace data-pipeline >/dev/null
"${KUBECTL[@]}" get configmap hadoop-config -n data-pipeline >/dev/null
"${KUBECTL[@]}" get secret platform-secrets -n data-pipeline >/dev/null
"${KUBECTL[@]}" get statefulset postgres -n data-pipeline >/dev/null
"${KUBECTL[@]}" get statefulset metastore-db -n data-pipeline >/dev/null
"${KUBECTL[@]}" get statefulset namenode -n data-pipeline >/dev/null
"${KUBECTL[@]}" get statefulset datanode -n data-pipeline >/dev/null
"${KUBECTL[@]}" get deployment hive-metastore -n data-pipeline >/dev/null
"${KUBECTL[@]}" get deployment trino -n data-pipeline >/dev/null
"${KUBECTL[@]}" get deployment kafka-connect -n data-pipeline >/dev/null
"${KUBECTL[@]}" get deployment airflow-webserver -n data-pipeline >/dev/null
"${KUBECTL[@]}" get deployment prometheus -n data-pipeline >/dev/null
"${KUBECTL[@]}" get deployment grafana -n data-pipeline >/dev/null

echo "Kubernetes resources are present."

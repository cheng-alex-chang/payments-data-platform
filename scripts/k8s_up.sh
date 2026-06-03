#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT_DIR}/infra/terraform/local-kind"
KUBECONFIG_PATH="${ROOT_DIR}/.kind/kubeconfig"

for command in docker terraform kind kubectl; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Missing required command: ${command}" >&2
    exit 1
  fi
done

cd "${TF_DIR}"
terraform init
terraform apply -auto-approve

cd "${ROOT_DIR}"
docker build -t local/data-pipeline-airflow:dev -f config/airflow/Dockerfile .
docker build -t local/trino-exporter:dev config/trino-exporter
cp drivers/postgresql-42.7.5.jar config/hive-metastore/postgresql-42.7.5.jar
trap 'rm -f "${ROOT_DIR}/config/hive-metastore/postgresql-42.7.5.jar"' EXIT
docker build -t local/hive-metastore:dev config/hive-metastore
rm -f config/hive-metastore/postgresql-42.7.5.jar
kind load docker-image local/data-pipeline-airflow:dev --name data-pipeline
kind load docker-image local/trino-exporter:dev --name data-pipeline
kind load docker-image local/hive-metastore:dev --name data-pipeline
KUBECONFIG="${KUBECONFIG_PATH}" kubectl apply -k k8s/overlays/local

echo
echo "Local Kubernetes foundation is ready."
echo "Use: export KUBECONFIG=${KUBECONFIG_PATH}"
echo "Then: kubectl get all -n data-pipeline"

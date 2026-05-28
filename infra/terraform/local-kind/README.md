# Local Kind Terraform

This module creates a no-cost local Kubernetes cluster with `kind`.

It intentionally provisions only the cluster. The data platform workloads are applied with Kustomize from `k8s/overlays/local`, which keeps infrastructure lifecycle and workload rollout separate.

## Prerequisites

- Docker Desktop
- Terraform
- kind
- kubectl

## Create the Cluster

```bash
cd infra/terraform/local-kind
terraform init
terraform apply
```

Terraform writes a project-local kubeconfig to `.kind/kubeconfig`.

The local cluster does not reserve application ports up front. Use `kubectl port-forward` for individual services as they are added.

## Apply Kubernetes Resources

From the repo root:

```bash
KUBECONFIG=.kind/kubeconfig kubectl apply -k k8s/overlays/local
```

## Destroy

```bash
cd infra/terraform/local-kind
terraform destroy
```

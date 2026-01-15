# Kubernetes Command Reference

Quick reference for deploying out_clp_s3_v2 on a local Kubernetes cluster using [k3d][k3d]
(Kubernetes in Docker).

> **See also:** [out_clp_s3_v2 README](../README.md) for configuration and concepts |
> [Main README](../../../README.md) for project overview

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│ Application │ ───▶ │  Fluent Bit │ ───▶ │    MinIO    │ ◀─── │ Log Viewer  │
│   (logs)    │      │  (CLP+Zstd) │      │  (S3 API)   │      │   (Web UI)  │
└─────────────┘      └─────────────┘      └─────────────┘      └─────────────┘
                                              │    │
                                         logs/  log-viewer/
                                         bucket    bucket
```

**Flow:**
1. Application writes logs to disk
2. Fluent Bit compresses with CLP IR + Zstd, uploads to `logs` bucket
3. YScope Log Viewer (static site in `log-viewer` bucket) reads and displays compressed logs

## Prerequisites

- [Docker](https://docs.docker.com/engine/install)
- [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) - Kubernetes CLI
- [k3d][k3d] - runs a lightweight Kubernetes cluster (k3s) in Docker containers

[k3d]: https://k3d.io/stable/#installation

## Cluster Setup

```shell
# Download plugins from GitHub Actions (see main README)
# Extract to a local directory, e.g., /path/to/plugins/

# Create cluster with plugin volume and port forwarding
k3d cluster create yscope --servers 1 --agents 1 \
  -v /path/to/plugins:/fluent-bit/plugins \
  -p 9000:30000@agent:0 \
  -p 9001:30001@agent:0

# Delete cluster
k3d cluster delete yscope
```

## Deploy Infrastructure

[MinIO](https://min.io/) provides S3-compatible storage for local development. The same plugin
configuration works with AWS S3 in production.

```shell
# MinIO (S3-compatible storage)
kubectl apply -f minio.yaml

# YScope log viewer (static site for viewing compressed logs)
kubectl apply -f yscope-log-viewer-deployment.yaml -f aws-credentials.yaml

# Create logs bucket
kubectl apply -f logs-bucket-creation.yaml -f aws-credentials.yaml
```

## Deploy Fluent Bit

### Sidecar

```shell
# Basic (3s flush)
kubectl apply -f fluent-bit-sidecar.yaml -f fluent-bit-sidecar-config.yaml -f aws-credentials.yaml

# Full config (30s flush)
kubectl apply -f fluent-bit-sidecar-full.yaml -f fluent-bit-sidecar-config-full.yaml -f aws-credentials.yaml
```

**Test:**
```shell
kubectl exec -it fluent-bit-sidecar -c ubuntu -- bash
mkdir -p /logs/$(whoami)/
echo '{"message": "test", "level": "error"}' > /logs/$(whoami)/test-0.jsonl

# Check logs
kubectl logs fluent-bit-sidecar -c fluent-bit-sidecar
```

### DaemonSet

```shell
kubectl create serviceaccount fluent-bit
kubectl apply -f fluent-bit-daemonset.yaml -f fluent-bit-daemonset-config.yaml -f aws-credentials.yaml -f ubuntu.yaml
```

**Test:**
```shell
kubectl exec -it ubuntu -- bash
mkdir -p /var/log/$(whoami)/
echo '{"message": "test", "level": "error"}' > /var/log/$(whoami)/test-1.jsonl

# Check logs
kubectl logs daemonset/fluent-bit
```

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Log Viewer** | http://localhost:9000/log-viewer/index.html | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO API | http://localhost:9000 | - |

**Viewing logs:**
1. Open Log Viewer URL above
2. Enter S3 path: `s3://logs/<path-to-file>.clp.zst`
3. The viewer decompresses and displays logs in the browser

```shell
# Alternative: port forward
kubectl port-forward minio 9000:9000
```

## Files

| File | Description |
|------|-------------|
| `minio.yaml` | MinIO pod and service |
| `aws-credentials.yaml` | AWS credentials secret (for MinIO) |
| `logs-bucket-creation.yaml` | Job to create logs bucket |
| `yscope-log-viewer-deployment.yaml` | YScope log viewer deployment |
| `fluent-bit-sidecar*.yaml` | Sidecar pod definitions |
| `fluent-bit-daemonset*.yaml` | DaemonSet definitions |
| `ubuntu.yaml` | Test pod for DaemonSet testing |

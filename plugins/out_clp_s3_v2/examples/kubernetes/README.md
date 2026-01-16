# Kubernetes Examples

Deploy out_clp_s3_v2 on Kubernetes clusters.

> **See also:** [Plugin README](../../README.md) for configuration options |
> [Main README](../../../../README.md) for project overview

## Deployment Options

| Environment | Plugin Loading | Directory |
|-------------|----------------|-----------|
| **Production** (EKS, GKE, AKS) | Init container downloads from GitHub Releases | [`production/`](production/) |
| **Local Development** (k3d) | hostPath volume mount | `sidecar/`, `daemonset/` |

**For production clusters**, use the [`production/`](production/) examples which download the plugin
at pod startup—no node configuration required.

## Deployment Patterns

| Pattern | Use Case | Directory |
|---------|----------|-----------|
| [**Sidecar**](sidecar/) | Per-pod log collection via shared volume | `sidecar/` |
| [**DaemonSet**](daemonset/) | Node-level log collection from `/var/log` | `daemonset/` |
| [**Production**](production/) | Production-ready manifests with init containers | `production/` |

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

## Local Development with k3d

The following sections describe how to set up a local development environment using
[k3d][k3d] (Kubernetes in Docker). For production deployments, see the [`production/`](production/)
directory.

### Prerequisites

- [Docker](https://docs.docker.com/engine/install)
- [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) - Kubernetes CLI
- [k3d][k3d] - lightweight Kubernetes cluster in Docker

[k3d]: https://k3d.io/stable/#installation

### Cluster Setup

```shell
# Download plugins from GitHub Actions (see main README)
# Extract to a local directory, e.g., /path/to/plugins/

# Create cluster with plugin volume and port forwarding
k3d cluster create yscope --servers 1 --agents 1 \
  -v /path/to/plugins:/fluent-bit/plugins \
  -p 9000:30000@agent:0 \
  -p 9001:30001@agent:0
```

### Deploy Infrastructure

[MinIO](https://min.io/) provides S3-compatible storage for local development. The same plugin
configuration works with AWS S3 in production.

```shell
# MinIO (S3-compatible storage)
kubectl apply -f minio.yaml -f aws-credentials.yaml
kubectl wait --for=condition=ready pod/minio --timeout=60s

# Create logs bucket
kubectl apply -f logs-bucket-creation.yaml

# YScope log viewer (optional)
kubectl apply -f yscope-log-viewer-deployment.yaml
```

### Deploy Fluent Bit

Choose a deployment pattern:

- **[Sidecar](sidecar/)** - Fluent Bit runs alongside your app in the same pod
- **[DaemonSet](daemonset/)** - Fluent Bit runs once per node, collecting from all pods

See each directory's README for detailed instructions.

### Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Log Viewer** | http://localhost:9000/log-viewer/index.html | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO API | http://localhost:9000 | - |

**Viewing logs:**
1. Open Log Viewer URL above
2. Enter S3 path: `s3://logs/<path-to-file>.clp.zst`
3. The viewer decompresses and displays logs in the browser

### Cleanup

```shell
k3d cluster delete yscope
```

## Directory Structure

```
kubernetes/
├── README.md                         # This file
├── minio.yaml                        # MinIO pod and service
├── aws-credentials.yaml              # AWS credentials secret
├── logs-bucket-creation.yaml         # Job to create logs bucket
├── yscope-log-viewer-deployment.yaml # Log viewer web UI
├── production/                       # Production-ready examples
│   ├── README.md
│   ├── fluent-bit-config.yaml        # ConfigMap for production
│   ├── fluent-bit-sidecar.yaml       # Sidecar with init container
│   └── fluent-bit-daemonset.yaml     # DaemonSet with init container
├── sidecar/                          # Sidecar deployment (k3d)
│   ├── README.md
│   ├── fluent-bit-sidecar.yaml
│   ├── fluent-bit-sidecar-config.yaml
│   └── *-full.yaml                   # Extended config variants
└── daemonset/                        # DaemonSet deployment (k3d)
    ├── README.md
    ├── fluent-bit-daemonset.yaml
    ├── fluent-bit-daemonset-config.yaml
    └── ubuntu.yaml                   # Test pod
```

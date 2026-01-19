# Kubernetes Examples

Deploy out_clp_s3_v2 on Kubernetes clusters.

> **See also:** [Plugin README](../../README.md) for configuration options |
> [Main README](../../../../README.md) for project overview

## Deployment Options

| Directory | Description |
|-----------|-------------|
| [`quickstart/`](quickstart/) | Pre-built image with bundled .so plugin |
| [`init-container/`](init-container/) | Base Fluent Bit image + plugin downloaded at startup |
| [`production/`](production/) | Production-ready examples with both options above |

### Option Comparison

| | Pre-built Image (bundled .so) | Init Container (.so download) |
|-|-------------------------------|-------------------------------|
| **Pros** | Simple setup, no compatibility issues | Flexible plugin versioning, combine with other plugins |
| **Cons** | Fixed plugin version per image tag | Potential GLIBC compatibility issues with some base images |
| **Best for** | Most users, quick deployment | Custom setups, multi-plugin configurations |

## Quick Start

```shell
# 1. Create k3d cluster (or use existing k8s cluster)
k3d cluster create clp-test -p "30000-30001:30000-30001@server:0"

# 2. Build and import pre-built image (or use published image)
./scripts/build-docker.sh --amd64
k3d image import fluent-bit-clp-s3-v2:latest -c clp-test

# 3. Deploy infrastructure
kubectl create secret generic aws-credentials \
  --from-literal=credentials=$'[default]\naws_access_key_id=minioadmin\naws_secret_access_key=minioadmin'
kubectl apply -f minio.yaml
kubectl wait --for=condition=ready pod/minio --timeout=60s
kubectl apply -f logs-bucket-creation.yaml

# 4. Deploy Fluent Bit (choose one)
kubectl apply -f service-account.yaml
kubectl apply -f quickstart/           # Pre-built image
# OR
kubectl apply -f init-container/       # Download plugin at startup
```

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│ Application │ ───▶ │  Fluent Bit │ ───▶ │    MinIO    │
│   (logs)    │      │  (CLP+Zstd) │      │  (S3 API)   │
└─────────────┘      └─────────────┘      └─────────────┘
```

## Deployment Patterns

Each directory contains two deployment patterns. Choose based on your logging needs:

### Sidecar Pattern (`sidecar.yaml`)

A **sidecar** is a helper container that runs alongside your application container in the same Pod.
Both containers share the same network and can share storage volumes.

```
┌─────────────────────────────────────────────────┐
│ Pod                                             │
│  ┌─────────────────┐    ┌─────────────────┐    │
│  │ App Container   │    │ Fluent Bit      │    │
│  │                 │    │ (sidecar)       │    │
│  │ writes to ──────┼───►│ reads from      │───►│──► S3
│  │ /var/log/app/   │    │ /var/log/app/   │    │
│  └─────────────────┘    └─────────────────┘    │
│           └──── shared volume ────┘            │
└─────────────────────────────────────────────────┘
```

**When to use:**
- You want dedicated log collection per application
- Your app writes logs to files (not stdout)
- You need isolation between different applications' logs

**How it works:**
1. App container writes logs to a shared volume (e.g., `/var/log/app/`)
2. Fluent Bit container mounts the same volume and tails the log files
3. Each Pod has its own Fluent Bit instance

### DaemonSet Pattern (`daemonset.yaml`)

A **DaemonSet** ensures one Pod runs on every node in the cluster. This is the standard
pattern for node-level services like log collection, monitoring agents, or network plugins.

```
┌─────────────────────────────────────────────────────────────────┐
│ Node                                                            │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                        │
│  │ App Pod  │ │ App Pod  │ │ App Pod  │  (many app pods)       │
│  │ writes   │ │ writes   │ │ writes   │                        │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘                        │
│       │            │            │                               │
│       ▼            ▼            ▼                               │
│  ┌─────────────────────────────────────────────┐               │
│  │            /var/log/ (host path)            │               │
│  └─────────────────────────────────────────────┘               │
│                        │                                        │
│                        ▼                                        │
│  ┌─────────────────────────────────────────────┐               │
│  │ Fluent Bit DaemonSet Pod (one per node)     │───────────────┼──► S3
│  │ Collects logs from ALL pods on this node    │               │
│  └─────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────┘
```

**When to use:**
- You want centralized log collection for all pods on a node
- You're collecting container stdout/stderr logs
- You want to minimize resource overhead (one collector per node, not per pod)

**How it works:**
1. Kubernetes automatically schedules one Fluent Bit pod per node
2. Fluent Bit mounts the host's `/var/log` directory
3. It collects logs from all containers running on that node

### Pattern Comparison

| Aspect | Sidecar | DaemonSet |
|--------|---------|-----------|
| **Pods per node** | One per application pod | One per node |
| **Resource usage** | Higher (many instances) | Lower (one per node) |
| **Log isolation** | Per-application | All apps mixed |
| **Configuration** | Per-application | Cluster-wide |
| **Best for** | File-based app logs | Container stdout/stderr |

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:30001 | minioadmin / minioadmin |
| MinIO API | http://localhost:30000 | - |

## Customizing the Plugin

The examples use minimal configuration. Customize the `out_clp_s3_v2` output in your
ConfigMap's `fluent-bit.yaml` to control compression, upload timing, and log organization.

> **Full reference:** [Plugin README](../../README.md) for all options

### Common Customizations

```yaml
outputs:
  - name: "out_clp_s3_v2"
    match: "*"

    # Required: S3 bucket for logs
    log_bucket: "my-logs-bucket"

    # Log level detection (for level-based flush timing)
    # Set to the JSON field containing log level in your logs
    log_level_key: "level"        # Default. Looks for {"level": "ERROR", ...}
    # log_level_key: "severity"   # Use if your logs have {"severity": "error", ...}

    # Flush timing: Control how quickly logs are uploaded
    # Hard delta: Maximum time before forced upload (even if still receiving logs)
    # Soft delta: Time after last log before upload (idle timeout)
    #
    # Fast upload for errors (good for alerting):
    flush_hard_delta_error: "5s"
    flush_soft_delta_error: "2s"
    #
    # Batch debug logs longer (reduces S3 costs):
    flush_hard_delta_debug: "5m"
    flush_soft_delta_debug: "1m"
```

### Log Level Values

The plugin recognizes these values:

| Level | Recognized Values |
|-------|-------------------|
| TRACE | `trace`, `TRACE` |
| DEBUG | `debug`, `DEBUG`, `D` |
| INFO | `info`, `INFO`, `I` |
| WARN | `warn`, `warning`, `WARN`, `WARNING`, `W` |
| ERROR | `error`, `critical`, `ERROR`, `CRITICAL`, `E` |
| FATAL | `fatal`, `wtf`, `FATAL` |

Unrecognized or missing levels default to INFO.

### Flush Timing Presets

| Use Case | Hard Delta | Soft Delta | Description |
|----------|------------|------------|-------------|
| **Real-time** | 3s | 1s | Fast uploads, more S3 requests |
| **Balanced** | 30s | 10s | Good for most production use |
| **Cost-optimized** | 5m | 1m | Fewer uploads, higher latency |

## AWS S3 (Production)

Remove the `AWS_ENDPOINT_URL` environment variable to use real AWS S3:

```yaml
env: []
  # Remove AWS_ENDPOINT_URL for AWS S3
```

Configure AWS credentials via:
- IAM roles for service accounts (IRSA) on EKS
- Workload Identity on GKE
- Kubernetes secrets (as shown in examples)

## Directory Structure

```
kubernetes/
├── README.md                    # This file
├── quickstart/                  # Pre-built image examples
│   ├── config.yaml
│   ├── daemonset.yaml
│   └── sidecar.yaml
├── init-container/              # Download plugin at startup
│   ├── config.yaml
│   ├── daemonset.yaml
│   └── sidecar.yaml
├── production/                  # Production-ready examples
│   ├── README.md
│   ├── fluent-bit-config.yaml
│   ├── fluent-bit-daemonset.yaml
│   ├── fluent-bit-sidecar.yaml
│   └── *-init.yaml              # Init container variants
├── minio.yaml                   # MinIO pod + service (local testing)
├── logs-bucket-creation.yaml    # Job to create logs bucket
├── service-account.yaml         # ServiceAccount for Fluent Bit
└── yscope-log-viewer-deployment.yaml  # Log viewer web UI (optional)
```

## Cleanup

```shell
k3d cluster delete clp-test
```

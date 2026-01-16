# Fluent Bit CLP Plugins

[Fluent Bit][fluent-bit] output plugins that compress logs using [CLP][clp-blog] for efficient
storage and search on AWS S3.

## Table of Contents

- [Overview](#overview)
- [Which Plugin Should I Use?](#which-plugin-should-i-use)
- [Quick Start](#quick-start)
- [Docker Images](#docker-images)
- [Pre-built Binaries](#pre-built-binaries)
- [Documentation](#documentation)
- [Development](#development)

## Overview

```mermaid
flowchart LR
    A[Fluent Bit] --> B[CLP Plugin]
    B --> C[CLP IR + Zstd]
    C --> D[(S3)]
    D --> E[YScope Log Viewer]
```

**[CLP][clp-blog]** (Compressed Log Processor) is a log compression tool that achieves 10-100x
better compression than gzip while enabling fast search. These plugins compress logs into CLP's
Intermediate Representation (IR) format with [Zstd][zstd] compression, then upload to S3.

Use [YScope Log Viewer][log-viewer] to view compressed logs directly in the browser, or ingest
into CLP for search at scale.

## Which Plugin Should I Use?

| Plugin | Upload Trigger | Best For |
|--------|---------------|----------|
| **[out_clp_s3_v2](plugins/out_clp_s3_v2/README.md)** | Time-based (per log level) | Kubernetes, latency-sensitive workloads |
| **[out_clp_s3](plugins/out_clp_s3/README.md)** | Size-based (MB threshold) | Batch processing, size optimization |

**Recommended: Start with `out_clp_s3_v2`** - it offers more control over upload latency and
works well with Kubernetes deployments.

### out_clp_s3_v2 (Recommended)

Time-based flushing with per-log-level control:
- ERROR logs uploaded in seconds, DEBUG logs batched for minutes
- Dual-timer strategy (hard + soft) prevents upload storms
- Designed for Kubernetes (sidecar and DaemonSet patterns)
- MinIO compatible for local development

### out_clp_s3

Size-based uploads with crash recovery:
- Upload when buffer reaches size threshold (default 16 MB)
- Disk buffering with crash recovery
- IAM role assumption for cross-account access
- Single key extraction from log records

## Quick Start

### Docker Compose (Easiest)

The fastest way to try the plugins locally:

```shell
# out_clp_s3_v2 (recommended)
cd plugins/out_clp_s3_v2/examples/docker-compose
docker compose up

# out_clp_s3
cd plugins/out_clp_s3/examples/docker-compose
docker compose up
```

This starts MinIO, Fluent Bit with the plugin, and a log generator. View logs at
http://localhost:9001 (minioadmin/minioadmin).

### Kubernetes (k3d)

```shell
# Download plugins from GitHub Actions (see Pre-built Binaries section)
# Extract to a local directory, e.g., ./plugins/

# Create k3d cluster with plugin mounted
k3d cluster create yscope --servers 1 --agents 1 \
  -v $(pwd)/plugins:/fluent-bit/plugins \
  -p 9000:30000@agent:0 -p 9001:30001@agent:0

# Deploy MinIO and Fluent Bit
cd plugins/out_clp_s3_v2/examples/kubernetes
kubectl apply -f minio.yaml -f aws-credentials.yaml
kubectl apply -f logs-bucket-creation.yaml
kubectl apply -f sidecar/fluent-bit-sidecar.yaml -f sidecar/fluent-bit-sidecar-config.yaml
```

See [Kubernetes Examples](plugins/out_clp_s3_v2/examples/kubernetes/README.md) for sidecar and
DaemonSet deployment patterns.

### Docker (Build from Source)

```shell
# out_clp_s3_v2
cd plugins/out_clp_s3_v2
docker build -t fluent-bit-clp-v2 -f Dockerfile ../../
docker run -v ~/.aws/credentials:/root/.aws/credentials fluent-bit-clp-v2

# out_clp_s3
cd plugins/out_clp_s3
docker build -t fluent-bit-clp -f Dockerfile ../../
docker run -v ~/.aws/credentials:/root/.aws/credentials fluent-bit-clp
```

## Docker Images

Pre-built Fluent Bit images with CLP plugins are published to GitHub Container Registry:

```shell
# Time-based plugin (recommended)
docker pull ghcr.io/y-scope/fluent-bit-clp-s3-v2:latest

# Size-based plugin
docker pull ghcr.io/y-scope/fluent-bit-clp-s3:latest
```

Images are tagged with `latest` (main branch), branch names, and commit SHAs.

## Pre-built Binaries

Standalone plugin binaries (`.so` files) are also available as GitHub Actions artifacts.

**Download:** Go to [Actions → build](../../actions/workflows/build.yaml), select a workflow run,
and download the `fluent-bit-clp-plugins-linux-amd64` artifact.

| File | Plugin |
|------|--------|
| `out_clp_s3_v2_linux_amd64.so` | Time-based plugin (recommended) |
| `out_clp_s3_linux_amd64.so` | Size-based plugin |

## Documentation

| Document | Description |
|----------|-------------|
| [out_clp_s3_v2 README](plugins/out_clp_s3_v2/README.md) | Configuration, flush strategy, Kubernetes deployment |
| [out_clp_s3 README](plugins/out_clp_s3/README.md) | Configuration, disk buffering, crash recovery |
| [Kubernetes Examples](plugins/out_clp_s3_v2/examples/kubernetes/README.md) | Sidecar and DaemonSet deployments |

## Development

### Prerequisites

- Go 1.24+
- [Task](https://taskfile.dev/installation/)

### Cloning

This project uses git submodules for the [clp-ffi-go](https://github.com/y-scope/clp-ffi-go)
dependency:

```shell
# New clone (recommended)
git clone --recursive https://github.com/y-scope/fluent-bit-clp.git

# Existing clone
git submodule update --init --recursive
```

### Building

```shell
# Download clp-ffi-go native libraries
bash third-party/clp-ffi-go/scripts/download-libs.sh

# Build plugins
task build
```

### Linting

```shell
# Install prerequisites
# - Task: https://taskfile.dev/installation/
# - uv: https://docs.astral.sh/uv/getting-started/installation/

# Run checks
task lint:check

# Auto-fix
task lint:fix
```

> **Note:** Linting for the v1 plugin (`out_clp_s3`) is currently disabled and will be addressed in
> a future update.

[clp-blog]: https://www.uber.com/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp
[fluent-bit]: https://fluentbit.io/
[log-viewer]: https://github.com/y-scope/yscope-log-viewer
[zstd]: https://facebook.github.io/zstd/

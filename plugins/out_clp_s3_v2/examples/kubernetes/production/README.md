# Production Deployment

Production-ready Kubernetes manifests that work on any cluster (EKS, GKE, AKS, etc.) without
requiring pre-mounted plugin volumes.

> **See also:** [Kubernetes Examples](../README.md) for local k3d setup |
> [Plugin README](../../../README.md) for configuration options

## Recommended: Pre-built Docker Image

The simplest and most reliable approach is to use the pre-built Docker image:

```yaml
containers:
  - name: "fluent-bit"
    image: "ghcr.io/y-scope/fluent-bit-clp-s3-v2:<branch-or-tag>"
```

This image includes the plugin with all required libraries, avoiding compatibility issues.

## Alternative: Init Container

> **Note:** The init container approach may have GLIBC compatibility issues depending on the
> Fluent Bit base image version. If you encounter errors like `GLIBCXX_X.X.XX not found`,
> use the pre-built Docker image instead.

## How It Works

These examples use an **init container** to download the CLP plugin from GitHub Releases at pod
startup. This eliminates the need for hostPath volumes or custom node configuration.

```
┌─────────────────────────────────────────────────────────┐
│ Pod Startup                                             │
│                                                         │
│  1. Init Container                                      │
│     ┌─────────────────────────────────────────────┐     │
│     │  wget plugin from GitHub Releases           │     │
│     │  → /plugins/out_clp_s3_v2.so                │     │
│     └─────────────────────────────────────────────┘     │
│                         │                               │
│                         ▼ (emptyDir volume)             │
│  2. Fluent Bit Container                                │
│     ┌─────────────────────────────────────────────┐     │
│     │  Loads plugin from /fluent-bit/plugins/     │     │
│     └─────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────┘
```

## Quick Start

### Sidecar Deployment

```shell
# Create secrets
kubectl create secret generic aws-credentials \
  --from-file=credentials=$HOME/.aws/credentials

# Deploy ConfigMap and sidecar pod
kubectl apply -f fluent-bit-config.yaml
kubectl apply -f fluent-bit-sidecar.yaml
```

### DaemonSet Deployment

```shell
# Create secrets
kubectl create secret generic aws-credentials \
  --from-file=credentials=$HOME/.aws/credentials

# Deploy ConfigMap and DaemonSet
kubectl apply -f fluent-bit-config.yaml
kubectl apply -f fluent-bit-daemonset.yaml
```

## Configuration

### Plugin Version

Edit the `PLUGIN_VERSION` environment variable in the init container:

```yaml
env:
  - name: "PLUGIN_VERSION"
    value: "latest"  # Or use a specific commit hash, e.g., "e0efdea"
```

Available versions can be found at: https://github.com/y-scope/fluent-bit-clp/releases

### Architecture

Architecture is **automatically detected** at runtime using `uname -m`. The init container
maps `x86_64` → `amd64` and `aarch64` → `arm64`. No configuration needed for mixed-architecture
clusters.

### AWS S3 (Production)

Remove the `AWS_ENDPOINT_URL` environment variable to use real AWS S3:

```yaml
env: []
  # Remove AWS_ENDPOINT_URL for AWS S3
```

Configure AWS credentials via:
- IAM roles for service accounts (IRSA) on EKS
- Workload Identity on GKE
- Kubernetes secrets (as shown in examples)

## Alternative: Pre-built Docker Image

For simpler deployments, use the pre-built Fluent Bit image that includes the plugin:

```yaml
containers:
  - name: "fluent-bit"
    image: "ghcr.io/y-scope/fluent-bit-clp-s3-v2:latest"
```

This eliminates the need for init containers but offers less flexibility in plugin versioning.

## Files

| File | Description |
|------|-------------|
| `fluent-bit-config.yaml` | ConfigMap with Fluent Bit configuration |
| `fluent-bit-sidecar.yaml` | Sidecar pod with init container |
| `fluent-bit-daemonset.yaml` | DaemonSet with init container and ServiceAccount |

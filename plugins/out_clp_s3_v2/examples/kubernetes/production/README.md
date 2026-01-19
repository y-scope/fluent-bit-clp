# Production Deployment

Production-ready Kubernetes manifests that work on any cluster (EKS, GKE, AKS, etc.).

> **See also:** [Kubernetes Examples](../README.md) for local k3d setup |
> [Plugin README](../../../README.md) for configuration options

## Deployment Options

### Option Comparison

| | Pre-built Image (bundled .so) | Init Container (.so download) |
|-|-------------------------------|-------------------------------|
| **Pros** | Simple setup, no compatibility issues | Flexible plugin versioning, combine with other plugins |
| **Cons** | Fixed plugin version per image tag | Potential GLIBC compatibility issues with some base images |
| **Best for** | Most users, quick deployment | Custom setups, multi-plugin configurations |

### Option 1: Pre-built Docker Image

The image includes the plugin with all required libraries, avoiding compatibility issues.

```shell
# Create secrets
kubectl create secret generic aws-credentials \
  --from-file=credentials=$HOME/.aws/credentials

# Deploy
kubectl apply -f service-account.yaml
kubectl apply -f fluent-bit-config.yaml
kubectl apply -f fluent-bit-daemonset.yaml  # or fluent-bit-sidecar.yaml
```

Build the image locally if not using the published image:
```shell
./scripts/build-docker.sh --amd64  # or --arm64
```

### Option 2: Init Container (Download Plugin)

Downloads the plugin binary from GitHub Releases at pod startup. Use this when you need
to pin a specific plugin version or combine with other Fluent Bit plugins in a custom image.

> **Note:** This approach may have GLIBC compatibility issues depending on the Fluent Bit
> base image version. If you encounter errors like `GLIBCXX_X.X.XX not found`, consider
> using the pre-built Docker image or building a custom image with matching libraries.

```shell
# Create secrets
kubectl create secret generic aws-credentials \
  --from-file=credentials=$HOME/.aws/credentials

# Deploy (uses *-init.yaml variants)
kubectl apply -f service-account.yaml
kubectl apply -f fluent-bit-config-init.yaml
kubectl apply -f fluent-bit-daemonset-init.yaml  # or fluent-bit-sidecar-init.yaml
```

#### How Init Container Works

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

## Configuration

### Plugin Version (Init Container only)

Edit the `PLUGIN_VERSION` environment variable in the init container:

```yaml
env:
  - name: "PLUGIN_VERSION"
    value: "latest"  # Or use a specific tag, e.g., "v0.1.0"
```

Available versions: https://github.com/y-scope/fluent-bit-clp/releases

### Architecture

Architecture is **automatically detected** at runtime:
- Pre-built image: Multi-arch image works on both amd64 and arm64
- Init container: Uses `uname -m` to download the correct binary

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

## Files

| File | Description |
|------|-------------|
| **Pre-built Image** | |
| `fluent-bit-config.yaml` | ConfigMap for pre-built image |
| `fluent-bit-daemonset.yaml` | DaemonSet using pre-built image |
| `fluent-bit-sidecar.yaml` | Sidecar pod using pre-built image |
| **Init Container (.so download)** | |
| `fluent-bit-config-init.yaml` | ConfigMap for init container deployment |
| `fluent-bit-daemonset-init.yaml` | DaemonSet with init container |
| `fluent-bit-sidecar-init.yaml` | Sidecar pod with init container |
| **Common** | |
| `service-account.yaml` | ServiceAccount for Fluent Bit |

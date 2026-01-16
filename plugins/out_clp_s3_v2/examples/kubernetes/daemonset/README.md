# DaemonSet Deployment

Deploy Fluent Bit as a DaemonSet to collect logs from all nodes in the cluster. Reads from
`/var/log` on each node.

> **See also:** [Kubernetes Examples](../README.md) for infrastructure setup |
> [Plugin README](../../../README.md) for configuration options

## When to Use

- Collect logs from all pods on a node
- Centralized log collection strategy
- Logs written to node filesystem (hostPath volumes)

## Quick Start

```shell
# Prerequisites: deploy infrastructure from parent directory first
cd ..
kubectl apply -f minio.yaml -f aws-credentials.yaml
kubectl wait --for=condition=ready pod/minio --timeout=60s
kubectl apply -f logs-bucket-creation.yaml
cd daemonset

# Deploy DaemonSet
kubectl create serviceaccount fluent-bit
kubectl apply -f fluent-bit-daemonset.yaml -f fluent-bit-daemonset-config.yaml -f ../aws-credentials.yaml

# Deploy test pod
kubectl apply -f ubuntu.yaml
```

## Test

```shell
# Write a test log from ubuntu pod
kubectl exec ubuntu -- sh -c \
  'mkdir -p /var/log/app && echo "{\"message\":\"test\",\"level\":\"info\"}" > /var/log/app/test-001.jsonl'

# Check DaemonSet logs
kubectl logs daemonset/fluent-bit

# Verify upload to MinIO (wait a few seconds for flush)
kubectl exec minio -- mc ls local/logs/ --recursive
```

## View Logs

Open the local [YScope Log Viewer](http://localhost:9000/log-viewer/index.html) to view compressed logs.

**Generate a direct link:**
```
http://localhost:9000/log-viewer/index.html?filePath=http://localhost:9000/logs/<filename>.clp.zst
```

**Example:**
```
http://localhost:9000/log-viewer/index.html?filePath=http://localhost:9000/logs/app/test-001.jsonl.clp.zst
```

The viewer decompresses and displays logs directly in the browser.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Node                                                    │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                  │
│  │  Pod A  │  │  Pod B  │  │  Pod C  │                  │
│  └────┬────┘  └────┬────┘  └────┬────┘                  │
│       │            │            │                       │
│       └────────────┼────────────┘                       │
│                    ▼                                    │
│            /var/log (hostPath)                          │
│                    │                                    │
│                    ▼                                    │
│  ┌─────────────────────────────────────┐                │
│  │  Fluent Bit DaemonSet Pod           │                │
│  │  (one per node)                     │────▶ S3        │
│  └─────────────────────────────────────┘                │
└─────────────────────────────────────────────────────────┘
```

## Cleanup

```shell
kubectl delete daemonset fluent-bit
kubectl delete pod ubuntu
kubectl delete serviceaccount fluent-bit
```

## Files

| File | Description |
|------|-------------|
| `fluent-bit-daemonset.yaml` | DaemonSet definition |
| `fluent-bit-daemonset-config.yaml` | ConfigMap with Fluent Bit config |
| `ubuntu.yaml` | Test pod for generating logs |

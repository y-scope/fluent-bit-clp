# Sidecar Deployment

Deploy Fluent Bit as a sidecar container alongside your application pod. Logs are shared via an
emptyDir volume.

> **See also:** [Kubernetes Examples](../README.md) for infrastructure setup |
> [Plugin README](../../../README.md) for configuration options

## When to Use

- Application writes logs to files (not stdout)
- Need per-pod log isolation
- Fine-grained control over which pods get log collection

## Quick Start

```shell
# Prerequisites: deploy infrastructure from parent directory first
cd ..
kubectl apply -f minio.yaml -f aws-credentials.yaml
kubectl wait --for=condition=ready pod/minio --timeout=60s
kubectl apply -f logs-bucket-creation.yaml
cd sidecar

# Deploy sidecar pod
kubectl apply -f fluent-bit-sidecar.yaml -f fluent-bit-sidecar-config.yaml -f ../aws-credentials.yaml
```

## Test

```shell
# Write a test log
kubectl exec fluent-bit-sidecar -c ubuntu -- sh -c \
  'mkdir -p /logs/app && echo "{\"message\":\"test\",\"level\":\"info\"}" > /logs/app/test-001.jsonl'

# Check Fluent Bit logs
kubectl logs fluent-bit-sidecar -c fluent-bit-sidecar

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
┌─────────────────────────────────────────┐
│ Pod                                     │
│  ┌─────────────┐    ┌────────────────┐  │
│  │ Application │───▶│   Fluent Bit   │  │
│  │  container  │    │   (sidecar)    │  │
│  └─────────────┘    └────────────────┘  │
│         │                   │           │
│         └───── /logs ───────┘           │
│            (emptyDir)                   │
└─────────────────────────────────────────┘
```

## Cleanup

```shell
kubectl delete pod fluent-bit-sidecar
```

## Files

| File | Description |
|------|-------------|
| `fluent-bit-sidecar.yaml` | Pod with ubuntu + fluent-bit sidecar |
| `fluent-bit-sidecar-config.yaml` | ConfigMap with Fluent Bit config |
| `*-full.yaml` | Extended config with 30s flush intervals |

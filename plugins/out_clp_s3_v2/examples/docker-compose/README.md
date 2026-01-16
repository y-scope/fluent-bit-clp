# Docker Compose Example

Local development setup with MinIO (S3-compatible storage) for testing the out_clp_s3_v2 plugin.

> **See also:** [Plugin README](../../README.md) for configuration options |
> [Main README](../../../../README.md) for project overview

## Quick Start

```shell
docker compose up
```

This starts:
- **MinIO** - S3-compatible storage
- **Fluent Bit** - with the CLP plugin configured
- **Log generator** - produces sample JSON logs

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Log Viewer** | http://localhost:9000/log-viewer/index.html | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO API | http://localhost:9000 | - |

## Verify Logs

Logs are uploaded based on flush timers (default: 10s for development settings).

```shell
# Check uploaded files
docker compose exec minio mc ls local/logs/ --recursive
```

## View Logs

Open the local [YScope Log Viewer](http://localhost:9000/log-viewer/index.html) to view compressed logs.

**Generate a direct link:**
```
http://localhost:9000/log-viewer/index.html?filePath=http://localhost:9000/logs/<filename>.clp.zst
```

**Example:**
```
http://localhost:9000/log-viewer/index.html?filePath=http://localhost:9000/logs/app.logs.app.test-20260115.jsonl.clp.zst
```

The viewer decompresses and displays logs directly in the browser.

## Configuration

Edit `fluent-bit.yaml` to customize:
- Input paths and parsers
- Flush intervals per log level (`flush_hard_delta_*`, `flush_soft_delta_*`)
- Output bucket and prefix

See [plugin documentation](../../README.md) for all options and timing presets.

## Cross-Platform Builds

Build for a different architecture using the `PLATFORM` environment variable:

```shell
# Build for amd64 (default)
docker compose up

# Build for arm64 (uses QEMU emulation)
PLATFORM=linux/arm64 docker compose up

# Rebuild for a specific platform
PLATFORM=linux/arm64 docker compose build fluent-bit
```

## Cleanup

```shell
docker compose down -v
```

## Files

| File | Description |
|------|-------------|
| `docker-compose.yaml` | Service definitions |
| `fluent-bit.yaml` | Fluent Bit configuration |

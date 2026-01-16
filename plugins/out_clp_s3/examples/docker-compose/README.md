# Docker Compose Example

Local development setup with MinIO (S3-compatible storage) for testing the out_clp_s3 plugin.

> **See also:** [Plugin README](../../README.md) for configuration options |
> [Main README](../../../../README.md) for project overview

## Quick Start

```shell
docker compose up
```

This starts:
- **MinIO** - S3-compatible storage
- **Fluent Bit** - with the CLP plugin configured
- **Log generator** - produces sample log lines

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO API | http://localhost:9000 | - |

## Verify Logs

Logs are uploaded when the buffer reaches `upload_size_mb` (default: 16 MB). To see uploads sooner,
generate more logs or lower the threshold in `fluent-bit.conf`.

```shell
# Check uploaded files
docker compose exec minio mc ls local/logs/ --recursive

# Or open MinIO Console
open http://localhost:9001
```

## Configuration

Edit `fluent-bit.conf` to customize:
- Input paths and parsers
- Upload size threshold (`upload_size_mb`)
- Output bucket and prefix

See [plugin documentation](../../README.md) for all options.

## Cleanup

```shell
docker compose down -v
```

## Files

| File | Description |
|------|-------------|
| `docker-compose.yaml` | Service definitions |
| `fluent-bit.conf` | Fluent Bit configuration |

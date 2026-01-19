# Contributing to Fluent Bit CLP Plugins

Guide for developers who want to contribute to or extend the CLP plugins.

## Table of Contents

- [Development Setup](#development-setup)
- [Building](#building)
- [Testing](#testing)
- [Code Structure](#code-structure)
- [Adding Features](#adding-features)
- [Submitting Changes](#submitting-changes)

---

## Development Setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| [Go](https://go.dev/dl/) | 1.24+ | Plugin compilation |
| [Task](https://taskfile.dev/installation/) | 3.x | Build automation |
| [Docker](https://docs.docker.com/get-docker/) | 20+ | Container builds and testing |

### Clone and Initialize

```shell
# Clone with submodules (required for clp-ffi-go)
git clone --recursive https://github.com/y-scope/fluent-bit-clp.git
cd fluent-bit-clp

# If you already cloned without --recursive:
git submodule update --init --recursive

# Download CLP native libraries
bash third-party/clp-ffi-go/scripts/download-libs.sh
```

### Verify Setup

```shell
# Build both plugins
task build

# Run tests
go test ./...

# Check linting (requires uv for Python tools)
task lint:check
```

---

## Building

### Build Commands

```shell
# Build all plugins
task build

# Build specific plugin
task build:s3v2    # out_clp_s3_v2 (continuous sync)
task build:s3      # out_clp_s3 (batch upload)
```

Output binaries are placed in `pre-built/`:
```
pre-built/
├── out_clp_s3_v2_linux_amd64.so
├── out_clp_s3_v2_linux_arm64.so
├── out_clp_s3_linux_amd64.so
└── out_clp_s3_linux_arm64.so
```

### Docker Builds

```shell
# Build Docker image for out_clp_s3_v2
cd plugins/out_clp_s3_v2
./scripts/build-docker.sh --amd64    # Single architecture
./scripts/build-docker.sh            # Both amd64 and arm64

# Build and push to registry
IMAGE_NAME=myregistry/fluent-bit-clp-s3-v2 ./scripts/build-docker.sh --push
```

### Cross-Compilation

The plugins use CGO with the CLP FFI library. Cross-compilation requires the
appropriate native libraries:

```shell
# Libraries are downloaded per-architecture
bash third-party/clp-ffi-go/scripts/download-libs.sh

# Build for specific architecture
GOARCH=arm64 task build:s3v2
```

---

## Testing

### Unit Tests

```shell
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run specific package tests
go test -v ./plugins/out_clp_s3_v2/internal/...

# Run with coverage
go test -cover ./...
```

### Integration Testing with Docker Compose

The fastest way to test changes end-to-end:

```shell
cd plugins/out_clp_s3_v2/examples/docker-compose

# Start services (builds plugin from source)
docker compose up --build

# In another terminal, check logs are being uploaded
docker exec docker-compose-minio-1 mc ls local/logs/

# View MinIO Console at http://localhost:9001 (minioadmin/minioadmin)

# Cleanup
docker compose down -v
```

### Manual Testing with Fluent Bit

```shell
# Build the plugin
task build:s3v2

# Run Fluent Bit with your config
fluent-bit -c /path/to/fluent-bit.yaml

# Or use the example config
cd plugins/out_clp_s3_v2
fluent-bit -c fluent-bit.yaml
```

---

## Code Structure

```
fluent-bit-clp/
├── internal/                    # Shared code between plugins
│   ├── decoder/                 # Fluent Bit record decoding
│   ├── irzstd/                  # CLP IR + Zstd compression writers
│   └── outctx/                  # Output context management
│
├── plugins/
│   ├── out_clp_s3_v2/           # Continuous sync plugin
│   │   ├── out_clp_s3_v2.go     # Plugin entry points (register, flush, exit)
│   │   ├── internal/
│   │   │   ├── context.go       # Plugin instance state
│   │   │   ├── flush_manager.go # Dual-timer flush logic
│   │   │   ├── ingestion.go     # Log record processing
│   │   │   └── s3.go            # S3 upload operations
│   │   ├── examples/            # Docker/Kubernetes examples
│   │   └── Dockerfile
│   │
│   └── out_clp_s3/              # Batch upload plugin
│       ├── out_clp_s3.go
│       ├── internal/
│       │   ├── flush/           # Size-based flush logic
│       │   └── recovery/        # Crash recovery
│       └── examples/
│
├── third-party/
│   └── clp-ffi-go/              # CLP FFI Go bindings (submodule)
│
└── taskfiles/                   # Task build definitions
    ├── build.yaml
    └── lint.yaml
```

### Key Components

#### Fluent Bit Plugin Interface

Plugins implement the [Fluent Bit Go interface](https://docs.fluentbit.io/manual/development/golang-output-plugins):

```go
// out_clp_s3_v2.go

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
    // Called once at startup to register the plugin
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
    // Called once per output instance to initialize
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
    // Called when Fluent Bit has records to flush
}

//export FLBPluginExit
func FLBPluginExit() int {
    // Called at shutdown for cleanup
}
```

#### CLP IR Compression

The `internal/irzstd` package handles CLP compression:

```go
// Create a writer that outputs CLP IR compressed with Zstd
writer := irzstd.NewDiskWriter("/tmp/output.clp.zst")
defer writer.Close()

// Write log records
writer.Write(timestamp, message)
```

#### Dual-Timer Flush Strategy (v2)

The `flush_manager.go` implements the dual-timer strategy:

- **Soft timer**: Resets on every new log, triggers upload during quiet periods
- **Hard timer**: Never resets (only moves earlier), guarantees max latency

```go
// When a log arrives, update timers based on its level
manager.UpdateTimers(logLevel)

// Check if flush is needed
if manager.ShouldFlush() {
    // Upload to S3
}
```

---

## Adding Features

### Adding a New Configuration Option

1. **Define the option** in the plugin's config parsing:

```go
// out_clp_s3_v2.go - in FLBPluginInit
myOption := output.FLBPluginConfigKey(plugin, "my_option")
```

2. **Store in context** (`internal/context.go`):

```go
type Context struct {
    // ...
    MyOption string
}
```

3. **Document** in the plugin's README.md

4. **Add to examples** in `examples/` directories

### Adding a New Log Level

Edit `logLevelMap` in `out_clp_s3_v2.go`:

```go
var logLevelMap = map[string]int{
    // Add new mappings
    "verbose": LogLevelDebug,
    "VERBOSE": LogLevelDebug,
}
```

### Adding Tests

Create `*_test.go` files alongside the code:

```go
// internal/flush_manager_test.go
func TestFlushManager_UpdateTimers(t *testing.T) {
    manager := NewFlushManager(config)
    manager.UpdateTimers(LogLevelError)

    if !manager.ShouldFlush() {
        t.Error("Expected flush after error log")
    }
}
```

---

## Linting

```shell
# Check for issues
task lint:check

# Auto-fix issues (Go formatting)
task lint:fix
```

The project uses:
- [golangci-lint](https://golangci-lint.run/) for Go (config: `.golangci.yaml`)
- [yamllint](https://yamllint.readthedocs.io/) for YAML (config: `.yamllint.yml`)

---

## Submitting Changes

### Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(v2): add support for custom S3 prefix
fix(v2): handle empty log records gracefully
docs: update Kubernetes examples
test(v2): add flush manager unit tests
refactor: extract common S3 upload logic
```

### Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Make changes and add tests
4. Run linting: `task lint:check`
5. Run tests: `go test ./...`
6. Commit with conventional commit message
7. Push and open a PR against `main`

### CI Checks

PRs must pass:
- Go build (amd64 and arm64)
- Unit tests
- Linting (golangci-lint, yamllint)

---

## Resources

- [Fluent Bit Go Plugin Development](https://docs.fluentbit.io/manual/development/golang-output-plugins)
- [CLP Documentation](https://docs.yscope.com/clp/main/)
- [CLP FFI Go Bindings](https://github.com/y-scope/clp-ffi-go)
- [Task Documentation](https://taskfile.dev/)

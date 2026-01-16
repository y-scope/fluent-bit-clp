package internal

import (
	"log"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/klauspost/compress/zstd"
	"github.com/y-scope/clp-ffi-go/ir"
)

// Default configuration values.
const (
	// defaultLogLevelKey is the JSON key used to extract log severity from records.
	defaultLogLevelKey = "level"
	// defaultFlushDelta is the default time between flushes for all log levels.
	defaultFlushDelta = 3 * time.Second
)

// FlushConfigContext stores configuration for the dual-timer flush strategy.
//
// The flush strategy uses two timers per log stream:
//   - Hard timer: Guarantees logs are uploaded within a maximum time window
//   - Soft timer: Uploads after a period of inactivity (no new logs)
//
// Each log level can have different timer values, allowing critical logs
// (ERROR, FATAL) to trigger faster uploads than debug logs.
type FlushConfigContext struct {
	// LogLevelKey is the JSON key used to extract log severity from records.
	// Common values: "level", "severity", "log_level"
	LogLevelKey string

	// defaultLogLevel is the log level index to use when level cannot be determined.
	// Maps to the index in hardDeltas/softDeltas arrays.
	defaultLogLevel int

	// hardDeltas contains the hard flush deadline for each log level.
	// Index corresponds to log level (0=debug, 1=info, 2=warn, 3=error, 4=fatal).
	hardDeltas []time.Duration

	// softDeltas contains the soft flush delay for each log level.
	// Soft timer resets on each log event, triggering upload after inactivity.
	softDeltas []time.Duration
}

// flushContext manages the dual-timer flush strategy for a single log stream.
//
// Thread-safety: All public methods acquire the Mutex before modifying state.
// The timers run in separate goroutines but callbacks also acquire the Mutex.
type flushContext struct {
	// HardTimer fires at the hard deadline, guaranteeing upload within a time window.
	HardTimer *time.Timer
	// hardTimeout tracks when the hard timer will fire (for comparison with new events).
	hardTimeout time.Time

	// SoftTimer fires after a period of inactivity (no new log events).
	SoftTimer *time.Timer
	// softDelta tracks the current soft timer duration (minimum seen for this batch).
	softDelta time.Duration

	// userCallback is invoked when either timer fires, triggering S3 upload.
	userCallback func()

	// Mutex protects all fields from concurrent access.
	Mutex sync.Mutex
}

// compressionContext encapsulates the compression pipeline resources.
//
// The pipeline processes log events as:
//
//	Log Event → IR Writer → Zstd Writer → File
type compressionContext struct {
	// File is the temporary file storing compressed logs before S3 upload.
	File *os.File
	// ZstdWriter compresses data using Zstandard algorithm.
	ZstdWriter *zstd.Encoder
	// IRWriter encodes log events into CLP's Intermediate Representation format.
	IRWriter *ir.Writer
}

// IngestionContext contains all resources for processing a single log stream.
//
// Each unique log path (derived from Fluent Bit tag) has its own IngestionContext,
// allowing independent compression and flush timing per stream.
type IngestionContext struct {
	// Compression holds the file and encoder resources for this stream.
	Compression *compressionContext
	// Flush manages the upload timing for this stream.
	Flush *flushContext
}

// s3Context holds the S3 client and bucket configuration.
type s3Context struct {
	// Client is the configured AWS S3 client.
	Client *s3.Client
	// Bucket is the target S3 bucket for log uploads.
	Bucket string
}

// PluginContext is the top-level context for the out_clp_s3_v2 plugin.
//
// A single PluginContext is created during FLBPluginInit and shared across
// all flush callbacks. It contains:
//   - S3 configuration and client
//   - Map of ingestion contexts (one per log stream/tag)
//   - Flush timing configuration
type PluginContext struct {
	// S3 holds the S3 client and bucket configuration.
	S3 *s3Context
	// Ingestion maps log paths to their ingestion contexts.
	// Key is typically the Fluent Bit tag or file_path from log records.
	Ingestion map[string]*IngestionContext
	// FlushConfig contains the dual-timer flush strategy configuration.
	FlushConfig *FlushConfigContext
}

// NewPluginContext creates and initializes a new PluginContext from Fluent Bit configuration.
//
// This function:
//  1. Creates an S3 client using AWS credentials from the environment
//  2. Validates the target S3 bucket exists and is accessible
//  3. Loads flush timing configuration from plugin settings
//
// Configuration keys read from Fluent Bit:
//   - log_bucket: Target S3 bucket name (required)
//   - log_level_key: JSON key for log severity (default: "level")
//   - flush_hard_delta_*: Hard timer durations per log level
//   - flush_soft_delta_*: Soft timer durations per log level
//
// Returns an error if S3 client creation or bucket validation fails.
func NewPluginContext(plugin unsafe.Pointer) (*PluginContext, error) {
	// Create and validate S3 client
	client, err := S3CreateClient()
	if err != nil {
		log.Printf("[error] Failed to create S3 client: %v", err)
		return nil, err
	}

	bucket := output.FLBPluginConfigKey(plugin, "log_bucket")
	if err := S3ValidateLogBucket(client, bucket); err != nil {
		log.Printf("[error] Failed to validate log bucket %q: %v", bucket, err)
		return nil, err
	}
	log.Printf("[info] Logs are configured to be uploaded to s3://%s", bucket)

	// Load log level key configuration
	logLevelKey := getConfigWithDefault(plugin, "log_level_key", defaultLogLevelKey)
	log.Printf("[info] Log level key is configured to: %q", logLevelKey)

	// Load flush timing configuration for each log level
	// Index order: 0=debug, 1=info, 2=warn, 3=error, 4=fatal
	hardDeltas := []time.Duration{
		getConfigDuration(plugin, "flush_hard_delta_debug", defaultFlushDelta),
		getConfigDuration(plugin, "flush_hard_delta_info", defaultFlushDelta),
		getConfigDuration(plugin, "flush_hard_delta_warn", defaultFlushDelta),
		getConfigDuration(plugin, "flush_hard_delta_error", defaultFlushDelta),
		getConfigDuration(plugin, "flush_hard_delta_fatal", defaultFlushDelta),
	}
	softDeltas := []time.Duration{
		getConfigDuration(plugin, "flush_soft_delta_debug", defaultFlushDelta),
		getConfigDuration(plugin, "flush_soft_delta_info", defaultFlushDelta),
		getConfigDuration(plugin, "flush_soft_delta_warn", defaultFlushDelta),
		getConfigDuration(plugin, "flush_soft_delta_error", defaultFlushDelta),
		getConfigDuration(plugin, "flush_soft_delta_fatal", defaultFlushDelta),
	}

	return &PluginContext{
		S3: &s3Context{
			Client: client,
			Bucket: bucket,
		},
		Ingestion: make(map[string]*IngestionContext),
		FlushConfig: &FlushConfigContext{
			LogLevelKey:     logLevelKey,
			defaultLogLevel: 0, // Default to debug level
			hardDeltas:      hardDeltas,
			softDeltas:      softDeltas,
		},
	}, nil
}

// getConfigDuration reads a duration configuration value with a default fallback.
func getConfigDuration(plugin unsafe.Pointer, key string, defaultVal time.Duration) time.Duration {
	rawValue := output.FLBPluginConfigKey(plugin, key)
	if rawValue == "" {
		return defaultVal
	}

	duration, err := time.ParseDuration(rawValue)
	if err != nil {
		log.Printf("[warn] Failed to parse duration for %q (%q): %v; using default %v",
			key, rawValue, err, defaultVal)
		return defaultVal
	}
	return duration
}

// getConfigWithDefault reads a string configuration value with a default fallback.
func getConfigWithDefault(plugin unsafe.Pointer, key, defaultVal string) string {
	val := output.FLBPluginConfigKey(plugin, key)
	if val == "" {
		return defaultVal
	}
	return val
}

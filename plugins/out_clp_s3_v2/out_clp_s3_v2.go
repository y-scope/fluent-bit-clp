// Package main implements a Fluent Bit output plugin that compresses logs using CLP
// (Compressed Log Processor) and continuously syncs them to S3.
//
// This plugin uses CLP's IR (Intermediate Representation) format with Zstd compression,
// achieving 10-100x better compression than gzip while enabling fast search.
//
// # Fluent Bit Plugin Architecture
//
// Fluent Bit plugins are compiled as shared libraries (.so) and loaded at runtime.
// The plugin must export C-compatible functions with specific names:
//
//   - FLBPluginRegister: Called once when plugin is loaded
//   - FLBPluginInit: Called once per plugin instance during startup
//   - FLBPluginFlushCtx: Called repeatedly to process log batches
//   - FLBPluginExitCtx: Called during graceful shutdown
//
// # Data Flow
//
//	Fluent Bit Input → Msgpack → This Plugin → CLP IR → Zstd → S3
//
// Each log record arrives as a Msgpack-encoded [timestamp, data] tuple.
// The plugin decodes it, extracts metadata, compresses with CLP IR + Zstd,
// and uploads to S3 based on the dual-timer flush strategy.
package main

import (
	"C"
)

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/y-scope/clp-ffi-go/ffi"

	"github.com/y-scope/fluent-bit-clp/internal/decoder"
	"github.com/y-scope/fluent-bit-clp/plugins/out_clp_s3_v2/internal"
)

// Log level constants matching common logging frameworks.
// These map to indices in the flush configuration arrays.
const (
	LogLevelTrace = iota // 0 - Most verbose, typically disabled in production
	LogLevelDebug        // 1 - Debugging information
	LogLevelInfo         // 2 - Normal operational messages
	LogLevelWarn         // 3 - Warning conditions
	LogLevelError        // 4 - Error conditions
	LogLevelFatal        // 5 - Fatal errors, application may terminate
)

// logLevelMap provides case-insensitive mapping from log level strings to integers.
// Supports common formats from various logging frameworks:
//   - Standard: debug, info, warn, error, fatal
//   - Uppercase: DEBUG, INFO, WARN, ERROR, FATAL
//   - Single-letter: D, I, W, E (common in Android logging)
//   - Variants: warning, critical (mapped to warn/error)
var logLevelMap = map[string]int{
	// Trace level
	"trace": LogLevelTrace,
	"TRACE": LogLevelTrace,

	// Debug level
	"debug": LogLevelDebug,
	"DEBUG": LogLevelDebug,
	"D":     LogLevelDebug,

	// Info level
	"info": LogLevelInfo,
	"INFO": LogLevelInfo,
	"I":    LogLevelInfo,

	// Warn level
	"warn":    LogLevelWarn,
	"warning": LogLevelWarn,
	"WARN":    LogLevelWarn,
	"WARNING": LogLevelWarn,
	"W":       LogLevelWarn,

	// Error level (includes "critical" as alias)
	"critical": LogLevelError,
	"error":    LogLevelError,
	"CRITICAL": LogLevelError,
	"ERROR":    LogLevelError,
	"E":        LogLevelError,

	// Fatal level
	"wtf":   LogLevelFatal, // Android's "What a Terrible Failure"
	"fatal": LogLevelFatal,
	"FATAL": LogLevelFatal,
}

// Plugin constants
const (
	// PluginName is the identifier used to register with Fluent Bit.
	PluginName = "out_clp_s3_v2"

	// filePathKey is the JSON key used to extract file path from log records.
	// This is used as the S3 object key prefix.
	filePathKey = "file_path"
)

// FLBPluginRegister is called once when the plugin shared library is loaded.
//
// This function registers the plugin with Fluent Bit's plugin system.
// It provides the plugin name and description that appear in Fluent Bit's logs.
//
//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, PluginName, "CLP IR S3 output plugin with continuous sync")
}

// FLBPluginInit is called once for each plugin instance during Fluent Bit startup.
//
// This function:
//  1. Creates the plugin context (S3 client, configuration)
//  2. Validates S3 bucket access
//  3. Associates the context with this plugin instance
//
// Configuration is read from the Fluent Bit configuration file.
// Returns FLB_ERROR if initialization fails.
//
//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	outCtx, err := internal.NewPluginContext(plugin)
	if err != nil {
		log.Printf("[error] Failed to initialize plugin: %s.", err)
		return output.FLB_ERROR
	}

	// Store context for retrieval during flush callbacks
	output.FLBPluginSetContext(plugin, outCtx)

	return output.FLB_OK
}

// FLBPluginFlushCtx is called repeatedly to process batches of log records.
//
// Fluent Bit buffers log records and periodically flushes them to output plugins.
// Each call receives a batch of Msgpack-encoded records that this function:
//  1. Decodes from Msgpack format
//  2. Extracts timestamp, log level, and file path
//  3. Writes to the CLP IR compression pipeline
//  4. Updates flush timers based on log severity
//
// Parameters (provided by Fluent Bit):
//   - ctx: Plugin context pointer (contains our PluginContext)
//   - data: Pointer to Msgpack-encoded log records
//   - length: Size of the data buffer in bytes
//   - tag: Fluent Bit tag string for this batch
//
// Returns FLB_OK on success, FLB_ERROR on failure.
//
//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	pluginCtx, ok := getPluginContext(ctx)
	if !ok {
		return output.FLB_ERROR
	}

	flushConfig := pluginCtx.FlushConfig
	tagStr := C.GoString(tag)

	// Create Msgpack decoder for this batch
	dec := decoder.New(data, int(length))

	// Process each record in the batch
	for {
		flbTimestamp, jsonRecord, err := decoder.GetRecord(dec)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("[error] decoder.GetRecord error: %v", err)
			}
			break // End of batch or error
		}

		processRecord(pluginCtx, tagStr, flushConfig, flbTimestamp, jsonRecord)
	}

	return output.FLB_OK
}

// FLBPluginExitCtx is called during graceful shutdown.
//
// This function ensures all buffered logs are uploaded before the plugin exits:
//  1. Stops all flush timers to prevent concurrent operations
//  2. Triggers a final flush for each ingestion context
//
// Note: This is only called for graceful shutdown. Crash scenarios may lose
// buffered data (logs are in temp files, not yet uploaded).
//
//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	pluginCtx, ok := getPluginContext(ctx)
	if !ok {
		return output.FLB_ERROR
	}

	// Flush all ingestion contexts
	for path, ingestionCtx := range pluginCtx.Ingestion {
		flushCtx := ingestionCtx.Flush

		// Stop timers to prevent concurrent flush during shutdown
		flushCtx.Mutex.Lock()
		if flushCtx.HardTimer != nil {
			flushCtx.HardTimer.Stop()
			flushCtx.HardTimer = nil
		}
		if flushCtx.SoftTimer != nil {
			flushCtx.SoftTimer.Stop()
			flushCtx.SoftTimer = nil
		}
		flushCtx.Mutex.Unlock()

		// Trigger final upload
		log.Printf("[info] Graceful shutdown: flushing logs for %q", path)
		flushCtx.Callback()
	}

	log.Println("[info] Plugin shutdown complete.")
	return output.FLB_OK
}

// getPluginContext retrieves and type-asserts the plugin context from Fluent Bit.
func getPluginContext(ctx unsafe.Pointer) (*internal.PluginContext, bool) {
	p := output.FLBPluginGetContext(ctx)
	pluginCtx, ok := p.(*internal.PluginContext)
	if !ok {
		log.Println("[error] Could not read context.")
	}
	return pluginCtx, ok
}

// processRecord handles a single decoded log record.
//
// Processing steps:
//  1. Parse timestamp from Fluent Bit format
//  2. Unmarshal JSON record to extract fields
//  3. Get or create ingestion context for this stream
//  4. Build CLP log event with auto/user KV separation
//  5. Write to IR compression pipeline
//  6. Update flush timers based on log level
func processRecord(
	pluginCtx *internal.PluginContext,
	tagStr string,
	flushConfig *internal.FlushConfigContext,
	flbTimestamp any,
	jsonRecord []byte,
) {
	timestamp := parseTimestamp(flbTimestamp)

	userKvPairs, ok := unmarshalRecord(jsonRecord)
	if !ok {
		return
	}

	ingestionCtx, err := internal.GetOrCreateIngestionContext(pluginCtx, tagStr)
	if err != nil || ingestionCtx == nil {
		log.Printf("[error] Failed to get or create ingestion context for tag %s: %v", tagStr, err)
		return
	}

	event := buildLogEvent(timestamp, userKvPairs)

	if !writeLogEvent(ingestionCtx, event) {
		return
	}

	// Update flush timers based on log severity
	level := extractLogLevel(userKvPairs, flushConfig)
	ingestionCtx.Flush.Update(level, timestamp, flushConfig)
}

// parseTimestamp converts Fluent Bit's timestamp format to Go's time.Time.
//
// Fluent Bit can provide timestamps in multiple formats:
//   - FlbTime: Custom extension type with nanosecond precision
//   - uint64: Unix timestamp in milliseconds
//
// Falls back to current time if format is unrecognized.
func parseTimestamp(flbTimestamp any) time.Time {
	switch t := flbTimestamp.(type) {
	case decoder.FlbTime:
		return t.Time
	case uint64:
		return time.UnixMilli(int64(t))
	default:
		log.Printf("[warn] Invalid time type (%T), defaulting to now.", t)
		return time.Now()
	}
}

// unmarshalRecord decodes JSON record bytes into a map.
func unmarshalRecord(jsonRecord []byte) (map[string]any, bool) {
	var userKvPairs map[string]any
	if err := json.Unmarshal(jsonRecord, &userKvPairs); err != nil {
		log.Printf("[error] Failed to unmarshal JSON record %q: %v", string(jsonRecord), err)
		return nil, false
	}
	return userKvPairs, true
}

// buildLogEvent creates a CLP log event from the parsed record.
//
// CLP IR format distinguishes between:
//   - Auto KV pairs: System-generated metadata (timestamp, file_path)
//   - User KV pairs: Application-provided log fields
//
// The file_path field is extracted from user data and moved to auto KV
// since it's typically used for log routing/organization.
func buildLogEvent(timestamp time.Time, userKvPairs map[string]any) *ffi.LogEvent {
	event := ffi.NewLogEvent()

	// Add timestamp as auto-generated metadata
	event.AutoKvPairs["timestamp"] = timestamp.UnixMilli()

	// Extract and move file_path to auto KV pairs
	filePath, exists := userKvPairs[filePathKey]
	if exists {
		delete(userKvPairs, filePathKey)
	} else {
		filePath = ""
	}
	event.AutoKvPairs[filePathKey] = filePath

	// Remaining fields are user KV pairs
	event.UserKvPairs = userKvPairs
	return event
}

// writeLogEvent writes a log event to the compression pipeline.
func writeLogEvent(ingestionCtx *internal.IngestionContext, event *ffi.LogEvent) bool {
	if _, err := ingestionCtx.Compression.IRWriter.WriteLogEvent(*event); err != nil {
		log.Printf("[error] Failed to write log event: %v", err)
		return false
	}
	return true
}

// extractLogLevel extracts the log severity level from record fields.
//
// Looks for the configured log level key (default: "level") and maps
// the value to an integer level using logLevelMap.
//
// Returns LogLevelInfo (2) if:
//   - Log level key is not found
//   - Log level value is not a string
//   - Log level string is not in logLevelMap
func extractLogLevel(userKvPairs map[string]any, flushConfig *internal.FlushConfigContext) int {
	level := LogLevelInfo // Default to info

	lvl, found := userKvPairs[flushConfig.LogLevelKey]
	if !found {
		// Log level key not present in record - common for simple log formats
		return level
	}

	lvlStr, ok := lvl.(string)
	if !ok {
		log.Printf("[warn] LogLevel is not a string (got %T), defaulting to info.", lvl)
		return level
	}

	mapped, ok := logLevelMap[lvlStr]
	if !ok {
		log.Printf("[warn] Unknown log level %q, defaulting to info.", lvlStr)
		return level
	}

	return mapped
}

// main is required for the Go compiler but never called.
// Fluent Bit loads this as a shared library, not an executable.
func main() {
}

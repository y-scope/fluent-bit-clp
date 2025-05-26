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

const (
	LogLevelTrace = iota
	LogLevelDebug
	LogLevelInfo
	LogLevelWarn
	LogLevelError
	LogLevelFatal
)

var logLevelMap = map[string]int{
	"trace":    LogLevelTrace,
	"TRACE":    LogLevelTrace,
	"debug":    LogLevelDebug,
	"DEBUG":    LogLevelDebug,
	"D":        LogLevelDebug,
	"info":     LogLevelInfo,
	"INFO":     LogLevelInfo,
	"I":        LogLevelInfo,
	"warn":     LogLevelWarn,
	"warning":  LogLevelWarn,
	"WARN":     LogLevelWarn,
	"WARNING":  LogLevelWarn,
	"W":        LogLevelWarn,
	"critical": LogLevelError,
	"error":    LogLevelError,
	"CRITICAL": LogLevelError,
	"ERROR":    LogLevelError,
	"E":        LogLevelError,
	"wtf":      LogLevelFatal,
	"fatal":    LogLevelFatal,
	"FATAL":    LogLevelFatal,
}

const (
	PluginName  = "out_clp_s3_v2"
	filePathKey = "file_path"
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	// Gets called only once when the plugin.so is loaded
	return output.FLBPluginRegister(def, PluginName, "CLP IR file output plugin")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	// Gets called only once for each instance you have configured.
	outCtx, err := internal.NewPluginContext(plugin)
	if err != nil {
		log.Printf("[error] Failed to initialize plugin: %s.", err)
		return output.FLB_ERROR
	}

	// Set the context for this instance so that params can be retrieved during flush.
	output.FLBPluginSetContext(plugin, outCtx)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	pluginCtx, ok := getPluginContext(ctx)
	if !ok {
		return output.FLB_ERROR
	}

	flushConfig := pluginCtx.FlushConfig
	tagStr := C.GoString(tag)
	dec := decoder.New(data, int(length))

	for {
		flbTimestamp, jsonRecord, err := decoder.GetRecord(dec)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("[error] decoder.GetRecord error: %v", err)
			}
			break
		}

		processDecodedRecord(pluginCtx, tagStr, flushConfig, flbTimestamp, jsonRecord)
	}

	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	pluginCtx, ok := getPluginContext(ctx)
	if !ok {
		return output.FLB_ERROR
	}

	// Iterate and gracefully shutdown all ingestion contexts
	for path, ingestionCtx := range pluginCtx.Ingestion {
		flushCtx := ingestionCtx.Flush
		flushCtx.Mutex.Lock()

		// Stop timers to prevent further flushes
		if flushCtx.HardTimer != nil {
			flushCtx.HardTimer.Stop()
			flushCtx.HardTimer = nil
		}
		if flushCtx.SoftTimer != nil {
			flushCtx.SoftTimer.Stop()
			flushCtx.SoftTimer = nil
		}

		// Trigger final flush (calls userCallback)
		log.Printf("[info] Graceful shutdown: flushing logs for %q", path)
		flushCtx.Mutex.Unlock() // Unlock before callback to avoid deadlock
		flushCtx.Callback()
	}

	log.Println("[info] Plugin shutdown complete.")
	return output.FLB_OK
}

func getPluginContext(ctx unsafe.Pointer) (*internal.PluginContext, bool) {
	p := output.FLBPluginGetContext(ctx)
	pluginCtx, ok := p.(*internal.PluginContext)
	if !ok {
		log.Println("[error] Could not read context.")
	}
	return pluginCtx, ok
}

func processDecodedRecord(
	pluginCtx *internal.PluginContext,
	tagStr string,
	flushConfig *internal.FlushConfigContext,
	flbTimestamp any,
	jsonRecord []byte,
) {
	timestamp := parseTimestamp(flbTimestamp)

	userKvPairs, ok := unmarshalUserKvPairs(jsonRecord)
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

	level := extractLogLevel(userKvPairs, flushConfig)
	ingestionCtx.Flush.Update(level, timestamp, flushConfig)
}

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

func unmarshalUserKvPairs(jsonRecord []byte) (map[string]any, bool) {
	var userKvPairs map[string]any
	if err := json.Unmarshal(jsonRecord, &userKvPairs); err != nil {
		log.Printf("[error] Failed to unmarshal JSON record %q: %v", string(jsonRecord), err)
		return nil, false
	}
	return userKvPairs, true
}

func buildLogEvent(timestamp time.Time, userKvPairs map[string]any) *ffi.LogEvent {
	// CLP IrV2 makes a differentiation between auto-generated KV and user KV. For now,
	// we mark the timestamp and file_path as auto-generated KV, leaving the rest as user KV.
	event := ffi.NewLogEvent()
	event.AutoKvPairs["timestamp"] = timestamp.UnixMilli()

	filePath, exists := userKvPairs[filePathKey]
	if exists {
		delete(userKvPairs, filePathKey)
	} else {
		filePath = ""
	}
	event.AutoKvPairs[filePathKey] = filePath
	event.UserKvPairs = userKvPairs
	return event
}

func writeLogEvent(ingestionCtx *internal.IngestionContext, event *ffi.LogEvent) bool {
	if _, err := ingestionCtx.Compression.IRWriter.WriteLogEvent(*event); err != nil {
		log.Printf("[error] Failed to write log event: %v", err)
		return false
	}
	return true
}

func extractLogLevel(userKvPairs map[string]any, flushConfig *internal.FlushConfigContext) int {
	level := LogLevelInfo
	if lvl, found := userKvPairs[flushConfig.LogLevelKey]; found {
		if lvlStr, ok := lvl.(string); ok {
			if mapped, ok := logLevelMap[lvlStr]; ok {
				level = mapped
			} else {
				log.Printf("[warn] Unknown log level %q, defaulting to %d (info).",
					lvlStr, LogLevelInfo)
			}
		} else {
			log.Printf("[warn] LogLevel is not a string, defaulting to %d (info).", LogLevelInfo)
		}
	} else {
		log.Printf("[warn] LogLevel not found, defaulting to %d (info).", LogLevelInfo)
	}
	return level
}

func main() {
}

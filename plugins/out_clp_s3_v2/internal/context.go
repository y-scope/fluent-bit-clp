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

// FlushConfigContext stores the flush control configurations
type FlushConfigContext struct {
	LogLevelKey     string
	defaultLogLevel int
	hardDeltas      []time.Duration
	softDeltas      []time.Duration
}

// flushContext manages timing and callback logic for log flushing.
type flushContext struct {
	HardTimer    *time.Timer
	hardTimeout  time.Time
	softDelta    time.Duration
	SoftTimer    *time.Timer
	userCallback func()
	Mutex        sync.Mutex
}

// compressionContext encapsulates file and compression writers.
type compressionContext struct {
	File       *os.File
	ZstdWriter *zstd.Encoder
	IRWriter   *ir.Writer
}

// IngestionContext contains compression and flush contexts for a particular log path.
type IngestionContext struct {
	Compression *compressionContext
	Flush       *flushContext
}

// s3Context holds AWS S3 configuration and client.
type s3Context struct {
	Client *s3.Client
	Bucket string
}

// PluginContext is the top-level context for the plugin.
type PluginContext struct {
	S3          *s3Context
	Ingestion   map[string]*IngestionContext
	FlushConfig *FlushConfigContext
}

// NewPluginContext initializes a new PluginContext
func NewPluginContext(plugin unsafe.Pointer) (*PluginContext, error) {
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

	logLevelKey := getConfigWithDefaultString(plugin, "log_level_key", "level")
	log.Printf("[info] Log level key is configured to: %q", logLevelKey)

	// Flush behavior control - use very aggressive defaults for now
	hardDeltas := []time.Duration{
		getConfigWithDefaultTimeDuration(plugin, "flush_hard_delta_debug", 3*time.Second),
		getConfigWithDefaultTimeDuration(plugin, "flush_hard_delta_info", 3*time.Second),
		getConfigWithDefaultTimeDuration(plugin, "flush_hard_delta_warn", 3*time.Second),
		getConfigWithDefaultTimeDuration(plugin, "flush_hard_delta_error", 3*time.Second),
		getConfigWithDefaultTimeDuration(plugin, "flush_hard_delta_fatal", 3*time.Second),
	}
	softDeltas := []time.Duration{
		getConfigWithDefaultTimeDuration(plugin, "flush_soft_delta_debug", 3*time.Second),
		getConfigWithDefaultTimeDuration(plugin, "flush_soft_delta_info", 3*time.Second),
		getConfigWithDefaultTimeDuration(plugin, "flush_soft_delta_warn", 3*time.Second),
		getConfigWithDefaultTimeDuration(plugin, "flush_soft_delta_error", 3*time.Second),
		getConfigWithDefaultTimeDuration(plugin, "flush_soft_delta_fatal", 3*time.Second),
	}

	pluginCtx := &PluginContext{
		S3: &s3Context{
			Client: client,
			Bucket: bucket,
		},
		Ingestion: make(map[string]*IngestionContext),
		FlushConfig: &FlushConfigContext{
			LogLevelKey:     logLevelKey,
			defaultLogLevel: 0,
			hardDeltas:      hardDeltas,
			softDeltas:      softDeltas,
		},
	}

	return pluginCtx, nil
}

func getConfigWithDefaultTimeDuration(
	plugin unsafe.Pointer,
	key string,
	defaultVal time.Duration,
) time.Duration {
	duration, err := time.ParseDuration(output.FLBPluginConfigKey(plugin, key))
	if err != nil {
		log.Printf("[error] Failed to parse duration %q: %v", key, err)
		return defaultVal
	}
	return duration
}

func getConfigWithDefaultString(
	plugin unsafe.Pointer,
	key,
	defaultVal string,
) string {
	val := output.FLBPluginConfigKey(plugin, key)
	if val == "" {
		return defaultVal
	}
	return val
}

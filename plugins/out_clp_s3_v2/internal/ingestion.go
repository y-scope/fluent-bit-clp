package internal

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/y-scope/clp-ffi-go/ir"
)

// tempFilePattern is the pattern for creating temporary CLP IR files.
// Files are created in the system temp directory with this pattern.
const tempFilePattern = "clp-irv2-*.clp.zst"

// GetOrCreateIngestionContext returns an existing IngestionContext for the given path,
// or creates and registers a new one.
//
// Each unique log path (typically derived from the Fluent Bit tag) gets its own
// IngestionContext, which includes:
//   - A temporary file for buffering compressed logs
//   - A Zstd compression writer
//   - An IR (Intermediate Representation) writer for CLP encoding
//   - A flush context managing upload timing
//
// The created temp file is continuously synced to S3 based on the flush strategy.
// Multiple calls with the same path return the existing context.
func GetOrCreateIngestionContext(pluginCtx *PluginContext, path string) (*IngestionContext, error) {
	// Return existing ingestion context if available
	if ingestionContext, exists := pluginCtx.Ingestion[path]; exists {
		return ingestionContext, nil
	}

	// Create new ingestion context for this path
	ingestionCtx, err := createIngestionContext(pluginCtx, path)
	if err != nil {
		return nil, err
	}

	pluginCtx.Ingestion[path] = ingestionCtx
	return ingestionCtx, nil
}

// createIngestionContext creates a new IngestionContext with all required resources.
//
// This function sets up the compression pipeline:
//
//	Log Events → IR Writer → Zstd Writer → Temp File → S3
//
// Resources are cleaned up on error to prevent leaks.
func createIngestionContext(pluginCtx *PluginContext, path string) (*IngestionContext, error) {
	// Create temp file for buffering compressed logs
	tempFile, err := os.CreateTemp(os.TempDir(), tempFilePattern)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	// Create Zstd compression writer
	zstdWriter, err := zstd.NewWriter(tempFile)
	if err != nil {
		cleanupOnError(tempFile, nil)
		return nil, fmt.Errorf("failed to create zstd writer: %w", err)
	}

	// Create CLP IR writer (FourByteEncoding is the standard encoding)
	irWriter, err := ir.NewWriter[ir.FourByteEncoding](zstdWriter)
	if err != nil {
		cleanupOnError(tempFile, zstdWriter)
		return nil, fmt.Errorf("failed to create IR writer: %w", err)
	}

	// Create flush context with the upload callback
	flushCtx := newFlushContext(pluginCtx, path, tempFile, zstdWriter)

	return &IngestionContext{
		Compression: &compressionContext{
			File:       tempFile,
			ZstdWriter: zstdWriter,
			IRWriter:   irWriter,
		},
		Flush: flushCtx,
	}, nil
}

// cleanupOnError closes and removes resources when ingestion context creation fails.
func cleanupOnError(tempFile *os.File, zstdWriter *zstd.Encoder) {
	if zstdWriter != nil {
		_ = zstdWriter.Close()
	}
	if tempFile != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
	}
}

// newFlushContext creates a flush context with the S3 upload callback.
//
// The callback is invoked by the flush manager when either timer fires.
// It flushes the Zstd buffer and uploads the temp file to S3.
func newFlushContext(
	pluginCtx *PluginContext,
	path string,
	tempFile *os.File,
	zstdWriter *zstd.Encoder,
) *flushContext {
	return &flushContext{
		// Initialize timers - they will be properly scheduled on first Update() call
		HardTimer: time.NewTimer(0),
		SoftTimer: time.NewTimer(0),
		userCallback: func() {
			// Flush any buffered data in the Zstd encoder
			if err := zstdWriter.Flush(); err != nil {
				log.Printf("[error] zstdWriter.Flush failed: %v", err)
				return
			}
			// Upload the temp file to S3
			remotePath := fmt.Sprintf("%s.clp.zst", path)
			if err := S3Upload(pluginCtx.S3.Client, pluginCtx.S3.Bucket, tempFile.Name(), remotePath); err != nil {
				log.Printf("[error] Failed to upload to S3: %v", err)
			}
		},
	}
}

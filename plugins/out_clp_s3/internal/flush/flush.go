// Package implements methods to send data to output. All data provided by Fluent Bit is encoded
// with Msgpack.

package flush

import (
	"C"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/ugorji/go/codec"
	"github.com/y-scope/clp-ffi-go/ffi"

	"github.com/y-scope/fluent-bit-clp/internal/decoder"
	"github.com/y-scope/fluent-bit-clp/internal/outctx"
)

// Ingests Fluent Bit chunk, then sends to s3 in IR format. Data may be buffered on disk or in
// memory depending on plugin configuration.
//
// Parameters:
//   - data: Msgpack data
//   - size: Byte length
//   - tag: Fluent Bit tag
//   - ctx: Plugin context
//
// Returns:
//   - code: Fluent Bit success code (OK, RETRY, ERROR)
//   - err: Error if flush fails
func Ingest(data unsafe.Pointer, size int, tag string, ctx *outctx.S3Context) (int, error) {
	dec := decoder.New(data, size)
	logEvents, err := decodeMsgpack(dec, ctx.Config)
	if err != io.EOF {
		return output.FLB_ERROR, err
	}

	eventManager, err := ctx.GetEventManager(tag)
	if err != nil {
		return output.FLB_RETRY, fmt.Errorf("error getting event manager: %w", err)
	}

	numEvents, err := eventManager.Writer.WriteIrZstd(logEvents)
	if err != nil {
		log.Printf(
			"Wrote %d out of %d total log events for tag %s",
			numEvents,
			len(logEvents),
			eventManager.Tag,
		)
		return output.FLB_ERROR, err
	}

	uploadCriteriaMet, err := checkUploadCriteriaMet(
		eventManager,
		ctx.Config.UploadSizeMb,
	)
	if err != nil {
		return output.FLB_ERROR, fmt.Errorf("error checking upload criteria: %w", err)
	}

	if !uploadCriteriaMet {
		return output.FLB_OK, nil
	}

	err = eventManager.ToS3(ctx.Config, ctx.Uploader)
	if err != nil {
		return output.FLB_ERROR, fmt.Errorf("error flushing Zstd buffer to s3: %w", err)
	}

	return output.FLB_OK, nil
}

// Decodes Msgpack Fluent Bit chunk into slice of log events. Decode of Msgpack based on
// [Fluent Bit reference].
//
// Parameters:
//   - decoder: Msgpack decoder
//   - config: Plugin configuration
//
// Returns:
//   - logEvents: Slice of log events
//   - err: Error decoding Msgpack, error retrieving log message from decoded object
//
// [Fluent Bit reference]:
// https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
func decodeMsgpack(dec *codec.Decoder, config outctx.S3Config) ([]ffi.LogEvent, error) {
	var logEvents []ffi.LogEvent
	for {
		// TODO: Add an option to include the Fluent Bit timestamp as an auto-generated key.
		_, jsonRecord, err := decoder.GetRecord(dec)
		if err != nil {
			return logEvents, err
		}

		var autoKvPairs map[string]any = make(map[string]any)
		var userKvPairs map[string]any
		err = json.Unmarshal(jsonRecord, &userKvPairs)
		if err != nil {
			err = fmt.Errorf("failed to get message from record: %w", err)
			return nil, err
		}

		event := ffi.LogEvent{
			AutoKvPairs: autoKvPairs,
			UserKvPairs: userKvPairs,
		}
		logEvents = append(logEvents, event)
	}
}

// Checks if criteria are met to upload to s3. If useDiskBuffer is false, then the chunk is always
// uploaded so always returns true. If useDiskBuffer is true, check if Zstd buffer size is greater
// than upload size.
//
// Parameters:
//   - eventManager: Manager for Fluent Bit events with the same tag
//   - uploadSizeMb: S3 upload size in MB
//
// Returns:
//   - readyToUpload: Boolean if upload criteria met or not
//   - err: Error getting Zstd buffer size
func checkUploadCriteriaMet(eventManager *outctx.EventManager, uploadSizeMb int) (bool, error) {
	if !eventManager.Writer.GetUseDiskBuffer() {
		return true, nil
	}

	bufferSize, err := eventManager.Writer.GetZstdOutputSize()
	if err != nil {
		return false, fmt.Errorf("error could not get size of buffer: %w", err)
	}

	uploadSize := uploadSizeMb << 20

	if bufferSize >= uploadSize {
		log.Printf(
			"Zstd buffer size of %d for tag %s exceeded upload size %d",
			bufferSize,
			eventManager.Tag,
			uploadSize,
		)
		return true, nil
	}

	return false, nil
}

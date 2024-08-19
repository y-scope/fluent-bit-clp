// Package implements methods to send data to output. All data provided by Fluent Bit is encoded
// with Msgpack.

package flush

import (
	"C"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"
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

	eventManager, err := ctx.GetEventManager(tag, size)
	if err != nil {
		return output.FLB_RETRY, fmt.Errorf("error getting event manager: %w", err)
	}

	err = write(eventManager, logEvents, ctx.Config)
	if err != nil {
		return output.FLB_ERROR, err
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
		ts, record, err := decoder.GetRecord(dec)
		if err != nil {
			return logEvents, err
		}

		timestamp := decodeTs(ts)
		msg, err := getMessage(record, config)
		if err != nil {
			err = fmt.Errorf("failed to get message from record: %w", err)
			return nil, err
		}

		event := ffi.LogEvent{
			LogMessage: msg,
			Timestamp:  ffi.EpochTimeMs(timestamp.UnixMilli()),
		}
		logEvents = append(logEvents, event)
	}
}

// Decodes timestamp provided by Fluent Bit engine into time.Time. If timestamp cannot be
// decoded, returns system time.
//
// Parameters:
//   - ts: Timestamp provided by Fluent Bit
//
// Returns:
//   - timestamp: time.Time timestamp
func decodeTs(ts any) time.Time {
	var timestamp time.Time
	switch t := ts.(type) {
	case decoder.FlbTime:
		timestamp = t.Time
	case uint64:
		timestamp = time.Unix(int64(t), 0)
	default:
		log.Printf("time provided invalid, defaulting to now. Invalid type is %T", t)
		timestamp = time.Now()
	}
	return timestamp
}

// Retrieves message from a record object. The message can consist of the entire object or
// just a single key. For a single key, user should set use_single_key to true in fluent-bit.conf.
// In addition user, should set single_key to "log" which is default Fluent Bit key for unparsed
// messages; however, single_key can be set to another value. To prevent failure if the key is
// missing, user can specify allow_missing_key, and behaviour will fallback to the entire object.
//
// Parameters:
//   - record: JSON record from Fluent Bit with variable amount of keys
//   - config: Plugin configuration
//
// Returns:
//   - stringMsg: Retrieved message
//   - err: Key not found, json.Unmarshal error, string type assertion error
func getMessage(jsonRecord []byte, config outctx.S3Config) (string, error) {
	if !config.UseSingleKey {
		return string(jsonRecord), nil
	}

	var record map[string]any
	err := json.Unmarshal(jsonRecord, &record)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal json record %v: %w", jsonRecord, err)
	}

	singleKeyMsg, ok := record[config.SingleKey]
	if !ok {
		// If key not found in record, see if allow_missing_key=true. If missing key is
		// allowed, then return entire record.
		if config.AllowMissingKey {
			return string(jsonRecord), nil
		}
		return "", fmt.Errorf("key %s not found in record %v", config.SingleKey, record)
	}

	stringMsg, ok := singleKeyMsg.(string)
	if !ok {
		return "", fmt.Errorf("string type assertion for message failed %v", singleKeyMsg)
	}

	return stringMsg, nil
}

// Writes logEvents to event manager buffer. If upload criteria is met, sends upload signal to
// [EventManager.UploadRequests] channel. Method acquires lock to prevent upload while writing.
//
// Parameters:
//   - eventManager: Manager for Fluent Bit events with the same tag
//   - logEvents: Slice of log events
//   - config: Plugin configuration
//
// Returns:
//   - err: Error writing log events, error checking upload criteria
func write(
	eventManager *outctx.S3EventManager,
	logEvents []ffi.LogEvent,
	config outctx.S3Config,
) error {
	eventManager.Mutex.Lock()
	defer eventManager.Mutex.Unlock()

	numEvents, err := eventManager.Writer.WriteIrZstd(logEvents)
	if err != nil {
		log.Printf(
			"Wrote %d out of %d total log events for tag %s",
			numEvents,
			len(logEvents),
			eventManager.Tag,
		)
		return fmt.Errorf("error writing log events: %w", err)
	}

	uploadCriteriaMet, err := checkUploadCriteriaMet(
		eventManager,
		config.UploadSizeMb,
	)
	if err != nil {
		return fmt.Errorf("error checking upload criteria: %w", err)
	}

	if uploadCriteriaMet {
		log.Printf("Sending upload request to channel with tag %s", eventManager.Tag)
		eventManager.UploadRequests <- true
	}

	return nil
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
func checkUploadCriteriaMet(eventManager *outctx.S3EventManager, uploadSizeMb int) (bool, error) {
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

// Package implements methods to send data to output. All data provided by Fluent Bit is encoded
// with Msgpack.

package flush

import (
	"C"
	"encoding/json"
	"fmt"
	"io"
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
	logEvents, err := decodeMsgpack(dec)
	if err != io.EOF {
		return output.FLB_ERROR, err
	}

	eventManager, err := ctx.GetEventManager(tag)
	if err != nil {
		return output.FLB_RETRY, fmt.Errorf("error getting event manager: %w", err)
	}

	eventManager.LogEvents <- logEvents

	return output.FLB_OK, nil
}

// Decodes Msgpack Fluent Bit chunk into slice of log events. Decode of Msgpack based on
// [Fluent Bit reference].
//
// Parameters:
//   - decoder: Msgpack decoder
//
// Returns:
//   - logEvents: Slice of log events
//   - err: Error decoding Msgpack, error retrieving log message from decoded object
//
// [Fluent Bit reference]:
// https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
func decodeMsgpack(dec *codec.Decoder) ([]ffi.LogEvent, error) {
	var logEvents []ffi.LogEvent
	for {
		_, jsonRecord, err := decoder.GetRecord(dec)
		if err != nil {
			return logEvents, err
		}

		var autoKvPairs map[string]any = make(map[string]any)
		var userKvPairs map[string]any
		err = json.Unmarshal(jsonRecord, &userKvPairs)
		if err != nil {
			err = fmt.Errorf("failed to unmarshal record: %w", err)
			return nil, err
		}

		event := ffi.LogEvent{
			AutoKvPairs: autoKvPairs,
			UserKvPairs: userKvPairs,
		}
		logEvents = append(logEvents, event)
	}
}

// Package implements methods to send data to output. All data provided by fluent-bit is encoded
// with msgpack.

package flush

import (
	"C"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"

	"github.com/y-scope/fluent-bit-clp/context"
	"github.com/y-scope/fluent-bit-clp/decoder"
)

// Flushes data to a file in IR format. Decode of msgpack based on [fluent-bit reference]
// [fluent-bit reference]:
// https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
//
// Parameters:
//   - data: msgpack data
//   - length: Byte length
//   - tag: fluent-bit tag
//   - S3Context: plugin context
//
// Returns:
//   - err: Error if flush fails
//
// nolint:revive
func File(data unsafe.Pointer, length int, tag string, ctx *context.S3Context) (int, error) {
	// Buffer to store events from fluent-bit chunk.
	var logEvents []ffi.LogEvent

	dec := decoder.NewStringDecoder(data, length)

	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		timestamp := decodeTs(ts)
		msg, err := getMessage(record, ctx.Config)
		if err != nil {
			err = fmt.Errorf("failed to get message from record: %w", err)
			return output.FLB_ERROR, err
		}

		msgString, ok := msg.(string)
		if !ok {
			err = fmt.Errorf("string type assertion failed %v", msg)
			return output.FLB_ERROR, err
		}

		print(msgString)

		event := ffi.LogEvent{
			LogMessage: msgString,
			Timestamp:  ffi.EpochTimeMs(timestamp.UnixMilli()),
		}
		logEvents = append(logEvents, event)
	}

	f, err := createFile(ctx.Config.Path, ctx.Config.File)
	if err != nil {
		return output.FLB_RETRY, err
	}
	defer f.Close()

	// IR buffer using bytes.Buffer. So it will dynamically adjust if undersized.
	irWriter := OpenIrWriter(length, ctx.Config.IREncoding, ctx.Config.TimeZone)

	err = encodeIR(irWriter, logEvents)
	if err != nil {
		err = fmt.Errorf("error while encoding IR: %w", err)
		return output.FLB_ERROR, err
	}

	_, err = irWriter.CloseTo(f)
	if err != nil {
		err = fmt.Errorf("error writting IR to file: %w", err)
		return output.FLB_RETRY, err
	}

	return output.FLB_OK, nil
}

// Decodes timestamp provided by fluent-bit engine into time.Time type. If timestamp cannot be
// decoded, returns system time.
//
// Parameters:
//   - ts: timestamp provided by fluent-bit
//
// Returns:
//   - timestamp: time.Time timestamp
func decodeTs(ts interface{}) time.Time {
	var timestamp time.Time
	switch t := ts.(type) {
	case output.FLBTime:
		timestamp = ts.(output.FLBTime).Time
	case uint64:
		timestamp = time.Unix(int64(t), 0)
	default:
		fmt.Println("time provided invalid, defaulting to now.")
		timestamp = time.Now()
	}
	return timestamp
}

// Retrieves message as a string from record object. The message can consist of the entire object or
// just a single key. For a single key, user should set set_single_key to true in fluenbit.conf. To
// prevent failure if the key is missing, user can specify allow_missing_key, and behaviour will
// fallback to the entire object.
//
// Parameters:
//   - record: Structured record from fluent-bit. Record contains a variable amount of keys and
//
// values.
//   - config: Configuration based on fluent-bit.conf
//
// Returns:
//   - msg: retrieved message
//   - err: key not found, json.Marshal error
func getMessage(record map[interface{}]interface{}, config context.S3Config) (interface{}, error) {
	var msg interface{}
	var ok bool
	var err error

	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	// If use_single_key=true, then look for key in record, and set message to the key's value.
	if config.UseSingleKey {
		msg, ok = record[config.SingleKey]
		if !ok {
			// If key not found in record, see if allow_missing_key=false. If missing key is
			// allowed. then fallback to marshal entire object.
			if config.AllowMissingKey {
				msg, err = json.Marshal(record)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal record %v: %w", record, err)
				}
				// If key not found in record and allow_missing_key=false, then return an error.
			} else {
				return nil, fmt.Errorf("key %s not found in record %v", config.SingleKey, record)
			}
		}
	} else {
		msg, err = json.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal record %v: %w", record, err)
		}
	}

	return msg, nil
}

// Creates a new file to output IR. A new file is created for every fluent-bit chunk. File name is
// based on user configuration and the system timestamp is added as a suffix.
//
// Parameters:
//   - path: path from fluent-bit.conf
//   - file: file name from fluent-bit.conf
//
// Returns:
//   - f: os file
//   - err: could not create directory, could not create file
func createFile(path string, file string) (*os.File, error) {

	// Make directory if does not exist
	err := os.MkdirAll(path, 0o644)
	if err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", path, err)
		return nil, err
	}

	currentTime := time.Now()

	// Format the time as a string in RFC3339 format
	timeString := currentTime.Format(time.RFC3339)

	fileWithTs := fmt.Sprintf("%s_%s", file, timeString)

	fullFilePath := filepath.Join(path, fileWithTs)

	// If the file doesn't exist, create it.
	f, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		err = fmt.Errorf("failed to create file %s: %w", fullFilePath, err)
		return nil, err
	}
	return f, nil
}

// Encodes events into IR
//
// Parameters:
//   - irWriter: a writer to which the IR data will be written
//   - eventBuffer: a slice of log events to be encoded
//
// Returns:
//   - err: error if an event could not be written
func encodeIR(irWriter *ir.Writer, eventBuffer []ffi.LogEvent) error {
	for _, event := range eventBuffer {
		_, err := irWriter.Write(event)
		if err != nil {
			err = fmt.Errorf("failed to encode event %v into ir: %w", event, err)
			return err
		}
	}
	return nil
}

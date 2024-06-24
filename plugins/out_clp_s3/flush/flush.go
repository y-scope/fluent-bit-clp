// Package implements methods to send data to output. All data provided by Fluent Bit is encoded
// with Msgpack.

package flush

import (
	"C"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/zstd"
	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"

	"github.com/y-scope/fluent-bit-clp/config"
	"github.com/y-scope/fluent-bit-clp/decoder"
)

// Flushes data to a file in IR format. Decode of Msgpack based on [Fluent Bit reference].
//
// Parameters:
//   - data: Msgpack data
//   - length: Byte length
//   - tag: Fluent Bit tag
//   - S3Config: Plugin configuration
//
// Returns:
//   - code: Fluent Bit success code (OK, RETRY, ERROR)
//   - err: Error if flush fails
//
// [Fluent Bit reference]: https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
func File(data unsafe.Pointer, length int, tag string, config *config.S3Config) (int, error) {
	// Buffer to store events from Fluent Bit chunk.
	var logEvents []ffi.LogEvent

	dec := decoder.New(data, length)

	// Loop through all records in Fluent Bit chunk.
	for {
		ts, record, endOfStream, err := decoder.GetRecord(dec)

		if err != nil {
			err = fmt.Errorf("error decoding data from stream: %w", err)
			return output.FLB_ERROR, err
		}

		if endOfStream {
			break
		}

		timestamp := DecodeTs(ts)
		msg, err := GetMessage(record, config)
		if err != nil {
			err = fmt.Errorf("failed to get message from record: %w", err)
			return output.FLB_ERROR, err
		}

		msgString, ok := msg.(string)
		if !ok {
			err = fmt.Errorf("string type assertion for message failed %v", msg)
			return output.FLB_ERROR, err
		}

		event := ffi.LogEvent{
			LogMessage: msgString,
			Timestamp:  ffi.EpochTimeMs(timestamp.UnixMilli()),
		}
		logEvents = append(logEvents, event)
	}

	// Create file for IR output.
	f, err := CreateFile(config.Path, config.File)
	if err != nil {
		return output.FLB_RETRY, err
	}
	defer f.Close()

	zstdWriter, err := zstd.NewWriter(f)
	if err != nil {
		err = fmt.Errorf("error opening zstd writer: %w", err)
		return output.FLB_RETRY, err
	}
	defer zstdWriter.Close()

	// IR buffer using bytes.Buffer internally, so it will dynamically grow if undersized. Using
	// FourByteEncoding as default encoding.
	irWriter, err := ir.NewWriterSize[ir.FourByteEncoding](length, config.TimeZone)
	if err != nil {
		err = fmt.Errorf("error opening IR writer: %w", err)
		return output.FLB_RETRY, err
	}

	err = WriteIr(irWriter, logEvents)
	if err != nil {
		err = fmt.Errorf("error while encoding IR: %w", err)
		return output.FLB_ERROR, err
	}

	// Write zstd compressed IR to file.
	_, err = irWriter.CloseTo(zstdWriter)
	if err != nil {
		err = fmt.Errorf("error writting IR to file: %w", err)
		return output.FLB_RETRY, err
	}

	log.Printf("zstd compressed IR chunk written to %s", f.Name())
	return output.FLB_OK, nil
}

// Decodes timestamp provided by Fluent Bit engine into time.Time. If timestamp cannot be
// decoded, returns system time.
//
// Parameters:
//   - ts: Timestamp provided by Fluent Bit
//
// Returns:
//   - timestamp: time.Time timestamp
func DecodeTs(ts interface{}) time.Time {
	var timestamp time.Time
	switch t := ts.(type) {
	case decoder.FlbTime:
		timestamp = t.Time
	case uint64:
		timestamp = time.Unix(int64(t), 0)
	default:
		fmt.Printf("time provided invalid, defaulting to now. Invalid type is %T", t)
		timestamp = time.Now()
	}
	return timestamp
}

// Retrieves message from a record object. The message can consist of the entire object or
// just a single key. For a single key, user should set use_single_key to true in fluentbit.conf.
// In addition user, should set single_key to "log" which is default Fluent bit key for unparsed
// messages; however, single_key can be set to another value. To prevent failure if the key is
// missing, user can specify allow_missing_key, and behaviour will fallback to the entire object.
//
// Parameters:
//   - record: Json record from Fluent bit with variable amount of keys
//   - config: Configuration based on fluent-bit.conf
//
// Returns:
//   - msg: Retrieved message
//   - err: Key not found, json.Marshal error
func GetMessage(JsonRecord string, config *config.S3Config) (interface{}, error) {
	// If use_single_key=true, then look for key in record, and set message to the key's value.
	if config.UseSingleKey {
		var record map[interface{}]interface{}
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		err := json.UnmarshalFromString(JsonRecord, &record)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal json record %s: %w", JsonRecord, err)
		}

		singleKeyMsg, ok := record[config.SingleKey]
		if !ok {
			// If key not found in record, see if allow_missing_key=false. If missing key is
			// allowed, then return entire record.
			if config.AllowMissingKey {
				return JsonRecord, nil
				// If key not found in record and allow_missing_key=false, then return an error.
			} else {
				return nil, fmt.Errorf("key %s not found in record %v", config.SingleKey, record)
			}
		}
		return singleKeyMsg, nil
	}
	return JsonRecord, nil
}

// Creates a new file to output IR. A new file is created for every Fluent Bit chunk.
// The system timestamp is added as a suffix.
//
// Parameters:
//   - path: Directory path to create to write files inside
//   - file: File name prefix
//
// Returns:
//   - f: The created file
//   - err: Could not create directory, could not create file
func CreateFile(path string, file string) (*os.File, error) {
	// Make directory if does not exist.
	err := os.MkdirAll(path, 0o644)
	if err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", path, err)
		return nil, err
	}

	currentTime := time.Now()

	// Format the time as a string in RFC3339 format.
	timeString := currentTime.Format(time.RFC3339)

	fileWithTs := fmt.Sprintf("%s_%s.zst", file, timeString)

	fullFilePath := filepath.Join(path, fileWithTs)

	// If the file doesn't exist, create it.
	f, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		err = fmt.Errorf("failed to create file %s: %w", fullFilePath, err)
		return nil, err
	}
	return f, nil
}

// Writes log events to a IR Writer.
//
// Parameters:
//   - irWriter: CLP IR writer to write each log event with
//   - eventBuffer: A slice of log events to be encoded
//
// Returns:
//   - err: error if an event could not be written
func WriteIr(irWriter *ir.Writer, eventBuffer []ffi.LogEvent) error {
	for _, event := range eventBuffer {
		_, err := irWriter.Write(event)
		if err != nil {
			err = fmt.Errorf("failed to encode event %v into ir: %w", event, err)
			return err
		}
	}
	return nil
}

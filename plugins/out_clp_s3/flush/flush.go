// Package implements methods to send data to output. All data provided by fluent-bit is encoded
// with msgpack.

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

	"github.com/y-scope/fluent-bit-clp/config"
	"github.com/y-scope/fluent-bit-clp/decoder"
)

// Flushes data to a file in IR format. Decode of msgpack based on [fluent-bit reference].
// [fluent-bit reference]: https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
//
// Parameters:
//   - data: msgpack data
//   - length: Byte length
//   - tag: fluent-bit tag
//   - S3Config: Plugin configuration
//
// Returns:
//   - code: fluent-bit success code (OK, RETRY, ERROR)
//   - err: Error if flush fails
//
// nolint:revive
func File(data unsafe.Pointer, length int, tag string, config *config.S3Config) (int, error) {
	// Buffer to store events from fluent-bit chunk.
	var logEvents []ffi.LogEvent

	dec := decoder.NewStringDecoder(data, length)

	// Loop through all records in fluent-bit chunk.
	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
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

	zstdEncoder, err := zstd.NewWriter(f)
	if err != nil {
		err = fmt.Errorf("error opening zstd encoder: %w", err)
		return output.FLB_RETRY, err
	}
	defer zstdEncoder.Close()

	// IR buffer using bytes.Buffer. So it will dynamically adjust if undersized.
	irWriter, err := OpenIRWriter(length, config.IREncoding, config.TimeZone)
	if err != nil {
		err = fmt.Errorf("error opening IR writer: %w", err)
		return output.FLB_RETRY, err
	}

	err = EncodeIR(irWriter, logEvents)
	if err != nil {
		err = fmt.Errorf("error while encoding IR: %w", err)
		return output.FLB_ERROR, err
	}

	// Write zstd compressed IR to file.
	_, err = irWriter.CloseTo(zstdEncoder)
	if err != nil {
		err = fmt.Errorf("error writting IR to file: %w", err)
		return output.FLB_RETRY, err
	}

	log.Printf("zstd compressed IR chunk written to %s", f.Name())
	return output.FLB_OK, nil
}

// Decodes timestamp provided by fluent-bit engine into time.Time. If timestamp cannot be
// decoded, returns system time.
//
// Parameters:
//   - ts: timestamp provided by fluent-bit
//
// Returns:
//   - timestamp: time.Time timestamp
func DecodeTs(ts interface{}) time.Time {
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
// just a single key. For a single key, user should set set_single_key to true in fluentbit.conf.
// In addition user, should set single_key to "log" which is default fluent-bit key for unparsed
// messages; however, single_key can be set to another value. To prevent failure if the key is
// missing, user can specify allow_missing_key, and behaviour will fallback to the entire object.
//
// Parameters:
//   - record: Structured record from fluent-bit with variable amount of keys
//   - config: Configuration based on fluent-bit.conf
//
// Returns:
//   - msg: Retrieved message
//   - err: Key not found, json.Marshal error
func GetMessage(record map[interface{}]interface{}, config *config.S3Config) (interface{}, error) {
	var msg interface{}
	var ok bool
	var err error
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	// If use_single_key=true, then look for key in record, and set message to the key's value.
	if config.UseSingleKey {
		msg, ok = record[config.SingleKey]
		if !ok {
			// If key not found in record, see if allow_missing_key=false. If missing key is
			// allowed. then fallback to marshal entire object.
			if config.AllowMissingKey {
				msg, err = json.MarshalToString(record)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal record %v: %w", record, err)
				}
				// If key not found in record and allow_missing_key=false, then return an error.
			} else {
				return nil, fmt.Errorf("key %s not found in record %v", config.SingleKey, record)
			}
		}
	} else {
		msg, err = json.MarshalToString(record)
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

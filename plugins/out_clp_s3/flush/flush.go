// Package implements methods to send data to output. All data provided by fluent-bit is encoded
// with msgpack.

package flush

import (
	"C"
    "encoding/json"
	"fmt"
	"unsafe"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
	"github.com/fluent/fluent-bit-go/output"

	"github.com/y-scope/fluent-bit-clp/context"
	"github.com/y-scope/fluent-bit-clp/decoder"
)



// Flushes data to a file in IR format. Decode of msgpack based on [fluent-bit reference]
// [fluent-bit reference]: https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
//
// Parameters:
//   - data: msgpack data
//   - length: Byte length
//   - tag: fluent-bit tag
//   - S3Context: plugin context
//
// Returns:
//   - err: Error if flush fails
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
			err = fmt.Errorf("Failed to get message from record", err)
			return  output.FLB_ERROR, err
		}

		event := ffi.LogEvent{
			LogMessage: msg,
			Timestamp:  ffi.EpochTimeMs(timestamp.UnixMilli()),
		}
		logEvents = append(logEvents,event)
	}

	f, name, err :=  createFile(ctx.Config.Path, ctx.Config.File)
	if err != nil {
		err = fmt.Errorf("Could not create file %s", name)
		return output.FLB_RETRY, err
	}
	defer f.Close()

	//IR buffer using bytes.Buffer. Write so it will dynamically adjust if undersizes
	irWriter := OpenIrWriter(length, ctx.Config.IREncoding, ctx.Config.TimeZone)
	defer irWriter.Close()

	err = encodeIR(irWriter,logEvents)
	if err != nil {
		err = fmt.Errorf("Error while encoding IR")
		return output.FLB_ERROR, err
	}

	_, err = irWriter.WriteTo(f)
	if err != nil {
		err = fmt.Errorf("Error writing IR to file")
		return output.FLB_RETRY, err
	}

	return output.FLB_OK, nil
}

// Decodes timestamp provided by fluent-bit engine into time.Time type. If timestamp cannot be decoded, returns system time.
//
// Parameters:
//   - ts: timestamp provided by fluent-bit
//
// Returns:
//   - timestamp: time.Time timestamp
// nolint:revive
func decodeTs(ts interface{}) (time.Time) {
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


func getMessage(record map[interface{}]interface{}, config context.S3Config) (string,error) {
	var msg interface{}
	var ok bool
	var err error

	// if useSingleKey then look for key in record, and set msg to the key's value
	if config.UseSingleKey {
		msg, ok = record[config.SingleKey]

		// if key not found in record, see if logs with missing key are allowed
		// if allow missing key then marshall entire record to json. If not allowed then return error
		if !ok && config.Allow_Missing_Key {
			msg, err = json.Marshal(record)
		} else {
			err = fmt.Errorf("key %s not found in record %v", config.SingleKey, record)
		}
	// if not using single key, then marshall entire record to JSON
	} else {
		msg, err = json.Marshal(record)
	}

	msgString, ok := msg.(string)
	if !ok {
		err = fmt.Errorf("could not covert msg %v to string",msg)
	}
	return msgString, err
}

func createFile(path string, file string) (*os.File,string,error) {
	currentTime := time.Now()

	// Format the time as a string in RFC3339 format
	timeString := currentTime.Format(time.RFC3339)
	
	// Make directory if does not exist
	err := os.MkdirAll(path,0o644)
	if err != nil {
		return nil, "", err
	}

	fullFilePath := filepath.Join(path, file,"_",timeString)

	// If the file doesn't exist, create it. 
	f, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fullFilePath, err
	}
	return f, fullFilePath, err
}

func encodeIR(irWriter *ir.Writer, eventBuffer []ffi.LogEvent) (error) {
	for _, event := range eventBuffer {
		_, err := irWriter.Write(event)
		if nil != err {
			log.Printf("failed to write event: %v", event)
			return err
		}
	}
	return nil
}

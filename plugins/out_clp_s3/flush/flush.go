// Package implements methods to send data to output. All data provided by fluent-bit is encoded
// with msgpack.

package flush

import (
	"C"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
	"unsafe"
	"log"
	"encoding/json"

	//"bytes"
	"github.com/klauspost/compress/zstd"

	//"bytes"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
	"github.com/y-scope/fluent-bit-clp/decoder"
	"github.com/y-scope/fluent-bit-clp/internal/constant"

	"github.com/y-scope/fluent-bit-clp/context"
)

const (
	defaultTimestampPattern       string = "yyyy-MM-dd HH:mm:ss,SSS"
	defaultTimestampPatternSyntax string = "java::SimpleDateFormat"
	defaultTimeZoneId             string = "America/Toronto"
)

// Flushes data to file.
//
// Parameters:
//   - data: msgpack data
//   - length: Byte length
//   - tag: fluent-bit tag
//   - S3Config: Configuration based on fluent-bit.conf
//
// Returns:
//   - err: Error if flush fails
func File(data unsafe.Pointer, length int, tag string, S3Ctx *context.S3Context) error {
	config := (*S3Ctx).Config
	state := (*S3Ctx).State
	eventBuf := (*S3Ctx).EventBuffer
	var tempEventBuffer []ffi.LogEvent

	if (state.RawBytesInBuffer + length) >= constant.RawBytesFlushThreshold {
		//call flush
		ctx.State.Index = ctx.State.Index+1
		
	}

	fullFilePath := filepath.Join(config.Path, config.File)
	// If the file doesn't exist, create it, or append to the file. Will still cause error if there
	// is no directory
	f, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	defer f.Close()

	/* ================== Decode msgpack ================== */
	// Decode of msgpack based on [fluent-bit reference]
	// TODO: Update code so converts to IR and sends to s3.
	// [fluent-bit reference]: https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
	// nolint:revive
	dec := decoder.NewStringDecoder(data, length)
	// dec := output.NewDecoder(data, length)

	// compression := true

	// ioWriter, err := openIoWriter(f, compression)
	//irWriter, err := ir.NewWriterSize[ir.FourByteEncoding](length, defaultTimeZoneId)

	// Buffered writer improves performance and simplifies error handling. Checking for error when
	// flushing simplifies retry since nothing before error is written to file (written to buffer
	// instead). Buffer size set to value provided by fluent-bit to prevent overflow errors.
	// w := bufio.NewWriterSize(f, length)

	for {
		converteddec := (*output.FLBDecoder)(unsafe.Pointer(dec))
		ret, ts, record := output.GetRecord(converteddec)
		if ret != 0 {
			break
		}

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

		var msg interface{}
		var ok bool
		var err error

		if config.LogKey != "" {
			msg, ok = record[config.LogKey]
			if !ok && config.AllowJSON {
				msg, err = json.Marshal(record)
			}
		} else if config.AllowJSON{
			msg, err = json.Marshal(record)
		} else {
			log.Fatal("Did not find logkey. and AllowJSON is not set")
		}

		msg2, ok := msg.(string)

		event := ffi.LogEvent{
			LogMessage: msg2,
			Timestamp:  ffi.EpochTimeMs(timestamp.UnixMilli()),
		}

		tempEventBuffer = append(tempEventBuffer,event)
	}
	/* ================== End of Decode msgpack ================== */

	//if no error, then append all the events
	eventBuf = append(eventBuf,tempEventBuffer...)
	return err
}


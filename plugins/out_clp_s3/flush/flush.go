// Package implements methods to send data to output. All data provided by fluent-bit is encoded
// with msgpack.

package flush

import (
	"C"
	"fmt"
	"os"
	//"bytes"
	"path/filepath"
	"time"
	"unsafe"
	"io"
	"github.com/klauspost/compress/zstd"
	//"bytes"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/y-scope/fluent-bit-clp/output2"
	//"github.com/y-scope/fluent-bit-clp/output"
	"github.com/y-scope/clp-ffi-go/ir"
	"github.com/y-scope/clp-ffi-go/ffi"

	"github.com/y-scope/fluent-bit-clp/config"
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
func File(data unsafe.Pointer, length int, tag string, config *config.S3Config) error {
	fullFilePath := filepath.Join(config.Path, config.File)

	// If the file doesn't exist, create it, or append to the file. Will still cause error if there
	// is no directory
	f, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	//defer f.Close()

	/* ================== This code is mostly boilerplate [fluent-bit reference] ================== */
	// Temporary changes were made to boilerplate so that writes to file instead of stdout.
	// TODO: Update code so converts to IR and sends to s3.
	// [fluent-bit reference]: https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
	// nolint:revive
	dec := output2.NewStringDecoder(data, length)
	//dec := output.NewDecoder(data, length)

	//compression := true

	//ioWriter, err := openIoWriter(f, compression)
	irWriter, err := ir.NewWriterSize[ir.FourByteEncoding](length,defaultTimeZoneId)
	
	// Buffered writer improves performance and simplifies error handling. Checking for error when
	// flushing simplifies retry since nothing before error is written to file (written to buffer
	// instead). Buffer size set to value provided by fluent-bit to prevent overflow errors.
	//w := bufio.NewWriterSize(f, length)

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

		msg := record["log"].(string)

		event := ffi.LogEvent{
			LogMessage: msg,
			Timestamp:  ffi.EpochTimeMs(timestamp.UnixMilli()),
		}

		_, err := irWriter.Write(event)
		if nil != err {
			//t.Fatalf("ir.Writer.Write failed: %v", err)
		}

	}
	/* ================== End of boilerplate ================== */

	_, err = irWriter.CloseTo(f)
	if nil != err {
		//t.Fatalf("ir.Writer.CloseTo failed: %v", err)
	}
	//ioWriter.Close()
	f.Close()

	//t, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	t, err := os.OpenFile(fullFilePath,os.O_RDONLY,0o644)
	fmt.Print(err)
	

	irReader, err := ir.NewReader(t)
	fmt.Print("new reader")

	fmt.Print(err)
	log, err := irReader.Read()
	fmt.Print(log.LogMessageView)
	irReader.Close()
	t.Close()

	
	/*
	d, err := zstd.NewReader(f)


	var b bytes.Buffer
	d.WriteTo(&b)
	fmt.Print(err)
	fmt.Print(b)
	//irReader, err := ir.NewReader(d)
	//fmt.print(err)

	//log,err := irReader.Read()

	
	//_, err = io.Copy(&b, irReader)  

	//defer irReader.Close()
	defer d.Close()
	
	//fmt.Print(log)
	*/

	return err
}


func openIoWriter(writer io.WriteCloser, compression bool) (io.WriteCloser, error) {
	var out io.WriteCloser
	var err error

	if compression {
		out, err = zstd.NewWriter(writer)
	} else {
		out = writer
	}
	return out, err
}

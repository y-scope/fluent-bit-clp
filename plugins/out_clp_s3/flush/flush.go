// Package implements method to send data to output
package flush

import (
	"C"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/y-scope/fluent-bit-clp/config"
	"github.com/y-scope/fluent-bit-clp/internal/utils"
)

// flushes data to file
//
// Parameters:
//   - data: msgpack data
//   - length: byte length
//   - tag: fluent-bit tag
//   - S3Config: configuration based on fluent-bit.conf
//
// Returns:
//   - err: error flushing data
func File(data unsafe.Pointer, length int, tag string, config *config.S3Config) error {
	fullFilePath := filepath.Join(config.Path, config.File)

	// If the file doesn't exist, create it, or append to the file
	// will still cause error if there is no directory
	f, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	/* ====================== This code is mostly boilerplate [fluent-bit reference]====================== */
	// temporary changes were made so that writes to file instead of stdout
	// code will be deleted when switch to IR / send to S3
	// [fluent-bit reference]: https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
	dec := output.NewDecoder(data, length)
	count := 0
	for {
		ret, ts, record := output.GetRecord(dec)
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

		// temporary change so writes to file
		// code will be deleted
		_, err = f.WriteString(fmt.Sprintf("[%d] %s: [%s, {", count, tag, timestamp.String()))
		utils.CheckPrint(err)

		for k, v := range record {
			_, err = f.WriteString(fmt.Sprintf("\"%s\": %v, ", k, v))
			utils.CheckPrint(err)
		}

		_, err = f.WriteString("}\n")
		utils.CheckPrint(err)

		count++
	}
	/* ====================== End of boilerplate ====================== */
	return nil
}

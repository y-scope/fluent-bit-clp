package flush
import (
	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
	"os"
	"github.com/y-scope/fluent-bit-clp/context"
	"path/filepath"
	"log"
	"github.com/y-scope/fluent-bit-clp/internal/constant"
	"strconv"
)
	
func OpenIrWriter(size int, encoding string, timezone string) *ir.Writer {
	var irWriter *ir.Writer
	var err error
	switch encoding {
	case "FourByte":
		irWriter, err = ir.NewWriterSize[ir.EightByteEncoding](1024*1024, timezone)
	case "EightByte":
		irWriter, err = ir.NewWriterSize[ir.FourByteEncoding](1024*1024, timezone)
	default:
		log.Fatalf("unsupported encoding: %v", encoding)
	}
	if nil != err {
		log.Fatalf("NewWriterSize failed: %v", err)
	}
	return irWriter
}
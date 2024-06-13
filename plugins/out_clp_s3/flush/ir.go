package flush

import (
	"log"

	"github.com/y-scope/clp-ffi-go/ir"
)

func OpenIrWriter(size int, encoding string, timezone string) *ir.Writer {
	var irWriter *ir.Writer
	var err error
	switch encoding {
	case "EightByte":
		irWriter, err = ir.NewWriterSize[ir.EightByteEncoding](1024*1024, timezone)
	case "FourByte":
		irWriter, err = ir.NewWriterSize[ir.FourByteEncoding](1024*1024, timezone)
	default:
		log.Fatalf("unsupported encoding: %v", encoding)
	}
	if nil != err {
		log.Fatalf("NewWriterSize failed: %v", err)
	}
	return irWriter
}

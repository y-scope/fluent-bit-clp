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

func EmptyBuffer(ctx *context.S3Context) error {
	index := strconv.Itoa(ctx.State.Index)
	fullFilePath := filepath.Join(ctx.Config.Path, ctx.Config.File,"_",index)
	// If the file doesn't exist, create it. Will still cause error if there
	// is no directory
	f, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	irWriter := OpenIrWriter(constant.IRBufferSize, ctx.Config.IREncoding, ctx.Config.TimeZone)
	defer irWriter.Close()

	for _, event := range ctx.EventBuffer {
		_, err := irWriter.Write(event)
		if nil != err {
			log.Printf("failed to write event: %v", event)
		}
	}
	_, err = irWriter.WriteTo(f)
	return err
}
	
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
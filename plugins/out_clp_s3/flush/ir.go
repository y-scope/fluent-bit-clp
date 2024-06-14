package flush

import (
	"fmt"
	"log"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

// Opens an IR writer
//
// Parameters:
//   - size: Starting size of internal buffer
//   - encoding: Type of IR to encode
//   - timezone: Time zone of the source producing the log events, so that local times (any time
//     that is not a unix timestamp) are handled correctly
//
// Returns:
//   - ir.Writer
//   - err: error from NewWriterSize, unsuported encoding
func OpenIRWriter(size int, encoding string, timezone string) (*ir.Writer, error) {
	var irWriter *ir.Writer
	var err error
	switch encoding {
	case "EightByte":
		irWriter, err = ir.NewWriterSize[ir.EightByteEncoding](1024*1024, timezone)
	case "FourByte":
		irWriter, err = ir.NewWriterSize[ir.FourByteEncoding](1024*1024, timezone)
	default:
		log.Fatalf("unsupported encoding %s",encoding)
	}
	if err != nil {
		return nil, err
	}
	return irWriter, nil
}

// Encodes events into IR.
//
// Parameters:
//   - irWriter
//   - eventBuffer: a slice of log events to be encoded
//
// Returns:
//   - err: error if an event could not be written
func EncodeIR(irWriter *ir.Writer, eventBuffer []ffi.LogEvent) error {
	for _, event := range eventBuffer {
		_, err := irWriter.Write(event)
		if err != nil {
			err = fmt.Errorf("failed to encode event %v into ir: %w", event, err)
			return err
		}
	}
	return nil
}


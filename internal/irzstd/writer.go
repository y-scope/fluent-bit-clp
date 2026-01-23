// Package implements writer that converts log events to Zstd compressed IR. Effectively chaining
// together [ir.Writer] and [zstd.Encoder] in series.

package irzstd

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

type Writer interface {
	// Converts log events to Zstd compressed IR and outputs to the Zstd buffer.
	//
	// Parameters:
	//   - logEvents: A slice of log events to be encoded
	//
	// Returns:
	//   - numEvents: Number of log events successfully written to IR writer buffer
	//   - err
	WriteIrZstd([]ffi.LogEvent) (int, error)

	// Closes IR stream and Zstd frame. After calling close, Writer must be Reset() prior to calling
	// write.
	//
	// Returns:
	//   - err
	CloseStreams() error

	// Closes Writer.
	//
	// Returns:
	//   - err
	Close() error

	// Getter for closed.
	//
	// Returns:
	//   - closed: Boolean that is true if IR and Zstd streams are closed.
	GetClosed() bool

	// Reinitialize Writer after calling CloseStreams().
	//
	// Returns:
	//   - err
	Reset() error

	// Getter for useDiskBuffer.
	//
	// Returns:
	//   - useDiskBuffer: On/off for disk buffering
	GetUseDiskBuffer() bool

	// Getter for Zstd Output.
	//
	// Returns:
	//   - zstdOutput: Reader for Zstd output
	GetZstdOutput() io.Reader

	// Get size of Zstd output.
	//
	// Returns:
	//	 - size: Bytes written
	//   - err
	GetZstdOutputSize() (int, error)

	// Checks if writer is empty. True if no events are buffered.
	//
	// Returns:
	//   - empty: Boolean value that is true if buffer is empty
	//   - err
	CheckEmpty() (bool, error)
}

// Writes log events to a IR Writer.
//
// Parameters:
//   - irWriter: CLP IR writer to write each log event with
//   - logEvents: A slice of log events to be encoded
//
// Returns:
//   - numEvents: Number of log events successfully written to IR writer buffer
//   - err: Error if an event could not be written
func writeIr(irWriter *ir.Writer, logEvents []ffi.LogEvent) (int, error) {
	var numEvents int
	for _, event := range logEvents {
		_, err := irWriter.Write(event)
		if err != nil {
			err = fmt.Errorf("failed to encode event %v into ir: %w", event, err)
			return numEvents, err
		}
		numEvents += 1
	}
	return numEvents, nil
}

// Opens a new [ir.Writer] and [zstd.Encoder].
//
// Parameters:
//   - zstdOutput: Output location for Zstd
//   - timezone: Time zone of the log source
//   - size: Byte length
//
// Returns:
//   - irWriter: Writer for CLP IR
//   - ZstdWriter: Writer for Zstd
//   - err: Error opening IR/Zstd writer
func newIrZstdWriters(
	zstdOutput io.Writer,
	timezone string,
	size int,
) (*ir.Writer, *zstd.Encoder, error) {
	// IR buffer using bytes.Buffer internally, so it will dynamically grow if undersized. Using
	// FourByteEncoding as default encoding.
	irWriter, err := ir.NewWriterSize[ir.FourByteEncoding](size, timezone)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening IR writer: %w", err)
	}

	zstdWriter, err := zstd.NewWriter(zstdOutput)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening Zstd writer: %w", err)
	}
	return irWriter, zstdWriter, err
}

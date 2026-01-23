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
}

// Writes log events to a IR Writer.
//
// Parameters:
//   - irWriter: CLP IR writer to write each log event with
//   - logEvents: A slice of log events to be encoded
//
// Returns:
//   - numBytes: Total IR bytes written for the batch
//   - numEvents: Number of log events successfully written to IR writer buffer
//   - err: Error if an event could not be written
func writeIr(irWriter *ir.Writer, logEvents []ffi.LogEvent) (int, int, error) {
	var numEvents int
	var numBytes int
	for _, event := range logEvents {
		n, err := irWriter.WriteLogEvent(event)
		numBytes += n
		if err != nil {
			err = fmt.Errorf("failed to encode event %v into ir: %w", event, err)
			return numBytes, numEvents, err
		}
		numEvents += 1
	}
	return numBytes, numEvents, nil
}

// Opens a new [ir.Writer] and [zstd.Encoder].
//
// Parameters:
//   - zstdOutput: Output destination for Zstd
//   - irOutput: Output destination for IR
//
// Returns:
//   - irWriter: Writer for CLP IR
//   - zstdWriter: Writer for Zstd
//   - err: Error opening IR/Zstd writer
func newIrZstdWriters(
	zstdOutput io.Writer,
	irOutput io.Writer,
) (*ir.Writer, *zstd.Encoder, error) {
	zstdWriter, err := zstd.NewWriter(zstdOutput)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening Zstd writer: %w", err)
	}

	irWriter, err := ir.NewWriter[ir.FourByteEncoding](irOutput)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening IR writer: %w", err)
	}
	return irWriter, zstdWriter, err
}

// Package implements writer that converts log events to Zstd compressed IR. Effectively chaining
// together [ir.Writer] and [zstd.Encoder] in series.

package irzstd

import (
	"fmt"
	"io"

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
	Empty() (bool, error)
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

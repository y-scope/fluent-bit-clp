package irzstd

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

// Converts log events into Zstd compressed IR. Log events are immediately converted to Zstd
// compressed IR and stored in [memoryWriter.zstdBuffer]. Data is buffered in memory until the
// upload size threshold is reached, then sent to S3. Unlike [diskWriter], there is no crash
// recovery since buffers are in memory.
type memoryWriter struct {
	zstdBuffer *bytes.Buffer
	irWriter   *ir.Writer
	zstdWriter *zstd.Encoder
}

// Opens a new [memoryWriter] with a memory buffer for Zstd output. For use when use_disk_store is
// off.
//
// Returns:
//   - memoryWriter: Memory writer for Zstd compressed IR
//   - err: Error opening Zstd/IR writers
func NewMemoryWriter() (*memoryWriter, error) {
	var zstdBuffer bytes.Buffer

	zstdWriter, err := zstd.NewWriter(&zstdBuffer)
	if err != nil {
		return nil, fmt.Errorf("error opening Zstd writer: %w", err)
	}

	irWriter, err := ir.NewWriter[ir.FourByteEncoding](zstdWriter)
	if err != nil {
		return nil, fmt.Errorf("error opening IR writer: %w", err)
	}

	memoryWriter := memoryWriter{
		irWriter:   irWriter,
		zstdWriter: zstdWriter,
		zstdBuffer: &zstdBuffer,
	}

	return &memoryWriter, nil
}

// Converts log events to Zstd compressed IR and outputs to the Zstd buffer.
//
// Parameters:
//   - logEvents: A slice of log events to be encoded
//
// Returns:
//   - numEvents: Number of log events successfully written to IR writer buffer
//   - err: Error writing IR/Zstd
func (w *memoryWriter) WriteIrZstd(logEvents []ffi.LogEvent) (int, error) {
	_, numEvents, err := writeIr(w.irWriter, logEvents)
	if err != nil {
		return numEvents, err
	}
	return numEvents, err
}

// Closes IR stream and Zstd frame. Add trailing byte(s) required for IR/Zstd decoding. After
// calling close, [memoryWriter] must be reset prior to calling write.
//
// Returns:
//   - err: Error closing buffers
func (w *memoryWriter) CloseStreams() error {
	if err := w.irWriter.Close(); err != nil {
		return err
	}
	w.irWriter = nil

	return w.zstdWriter.Close()
}

// Reinitialize [memoryWriter] after calling CloseStreams(). Resets individual IR and Zstd writers
// and associated buffers.
//
// Returns:
//   - err: Error opening IR writer
func (w *memoryWriter) Reset() error {
	var err error
	w.zstdBuffer.Reset()
	w.zstdWriter.Reset(w.zstdBuffer)

	w.irWriter, err = ir.NewWriter[ir.FourByteEncoding](w.zstdWriter)
	if err != nil {
		return err
	}

	return nil
}

// Getter for Zstd Output.
//
// Returns:
//   - zstdOutput: Reader for Zstd output
func (w *memoryWriter) GetZstdOutput() io.Reader {
	return w.zstdBuffer
}

// Get size of Zstd output. [zstd] does not provide the amount of bytes written with each write.
// Instead, calling Len() on buffer.
//
// Returns:
//   - size: Bytes written
//   - err: nil error to comply with interface
func (w *memoryWriter) GetZstdOutputSize() (int, error) {
	return w.zstdBuffer.Len(), nil
}

// Closes [memoryWriter]. Currently used during recovery only, and advise caution using elsewhere.
// Using [ir.Writer.Serializer.Close] instead of [ir.Writer.Close] so EndofStream byte is not
// added. It is preferable to add postamble on recovery so that IR is in the same state
// (i.e. not terminated) for an abrupt crash and a graceful exit. Function does not call
// [zstd.Encoder.Close] as it does not explicitly free memory and may add undesirable null frame.
//
// Returns:
//   - err: Error closing irWriter, error closing files
func (w *memoryWriter) Close() error {
	if w.irWriter != nil {
		err := w.irWriter.Serializer.Close()
		if err != nil {
			return fmt.Errorf("error could not close irWriter: %w", err)
		}
	}
	return nil
}

// Checks if writer is empty. True if no events are buffered. Try to avoid calling this as will
// flush Zstd Writer potentially creating unnecessary frames.
//
// Returns:
//   - empty: Boolean value that is true if buffer is empty
//   - err: nil error to comply with interface
func (w *memoryWriter) CheckEmpty() (bool, error) {
	w.zstdWriter.Flush()

	empty := w.zstdBuffer.Len() == 0
	return empty, nil
}

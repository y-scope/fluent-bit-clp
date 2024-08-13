package irzstd

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

// Converts log events into Zstd compressed IR. Log events provided to writer are immediately
// converted to Zstd compressed IR and stored in [memoryWriter.ZstdBuffer].  After the Zstd buffer
// receives logs, they are immediately sent to s3.
type memoryWriter struct {
	zstdBuffer *bytes.Buffer
	irWriter   *ir.Writer
	size       int
	timezone   string
	zstdWriter *zstd.Encoder
}

// Opens a new [memoryWriter] with a memory buffer for Zstd output. For use when use_disk_store is
// off.
//
// Parameters:
//   - timezone: Time zone of the log source
//   - size: Byte length
//
// Returns:
//   - memoryWriter: Memory writer for Zstd compressed IR
//   - err: Error opening Zstd/IR writers
func NewMemoryWriter(timezone string, size int) (*memoryWriter, error) {
	var zstdBuffer bytes.Buffer
	irWriter, zstdWriter, err := newIrZstdWriters(&zstdBuffer, timezone, size)
	if err != nil {
		return nil, err
	}

	memoryWriter := memoryWriter{
		size:       size,
		timezone:   timezone,
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
//   - err: Error writing IR/Zstd
func (w *memoryWriter) WriteIrZstd(logEvents []ffi.LogEvent) error {
	err := writeIr(w.irWriter, logEvents)
	if err != nil {
		return err
	}

	_, err = w.irWriter.WriteTo(w.zstdWriter)
	return err
}

// Closes IR stream and Zstd frame. Add trailing byte(s) required for IR/Zstd decoding. After
// calling close, [memoryWriter] must be reset prior to calling write.
//
// Returns:
//   - err: Error closing buffers
func (w *memoryWriter) CloseStreams() error {
	_, err := w.irWriter.CloseTo(w.zstdWriter)
	if err != nil {
		return err
	}

	w.irWriter = nil

	err = w.zstdWriter.Close()
	return err
}

// Reinitialize [memoryWriter] after calling CloseStreams(). Resets individual IR and Zstd writers
// and associated buffers.
//
// Returns:
//   - err: Error opening IR writer
func (w *memoryWriter) Reset() error {
	var err error
	w.irWriter, err = ir.NewWriterSize[ir.FourByteEncoding](w.size, w.timezone)
	if err != nil {
		return err
	}

	w.zstdBuffer.Reset()
	w.zstdWriter.Reset(w.zstdBuffer)
	return nil
}

// Getter for useDiskBuffer.
//
// Returns:
//   - useDiskBuffer: On/off for disk buffering
func (w *memoryWriter) GetUseDiskBuffer() bool {
	return false
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

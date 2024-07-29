// Package implements writer that converts log events to Zstd compressed IR. Effectively chaining
// together [ir.Writer] and [zstd.Encoder] in series.

package irzstd

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/klauspost/compress/zstd"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

// 2 MB threshold to buffer IR before compressing to Zstd when use_disk_buffer is on.
const irSizeThreshold = 2 << 20

// Converts log events into Zstd compressed IR. Writer can be initialized with use_disk_buffer
// on/off depending on user configuration.
//
// Behavior with use_disk_buffer off:
// Log events provided to writer are immediately converted to Zstd compressed IR and stored in
// [Writer.ZstdBuffer]. With use_disk_buffer off, ZstdBuffer is a memory buffer. After the Zstd
// buffer receives logs, they are immediately sent to s3. There is no IR buffer and it is set to
// nil.
//
// Behavior with use_disk_buffer on:
// Logs events are not immediately converted to Zstd compressed IR, and instead compressed using
// "trash compactor" design. Log events are converted to uncompressed IR and buffered into "bins".
// Uncompressed IR represents uncompressed trash in "trash compactor". Once the bin is full, the
// bin is "compacted" into its own separate Zstd frame. The compressor is explicitly closed after
// receiving input terminating the Zstd frame. Stacks of Zstd frames are then sent to S3.  For
// majority of runtime, log events are stored as a mixture uncompressed IR and compressed
// Zstd frames. A simpler approach would be to send all the events for one S3 upload to the
// streaming compressor and only close the stream when the upload size is reached. However, the
// streaming compressor will keep frames/blocks open in between receipt of Fluent Bit chunks. Open
// frames/blocks may not be recoverable after an abrupt crash. Closed frames on the other hand are
// valid Zstd and can be send to s3 on startup. It is not explicitly necessary to buffer IR into
// "bins" (i.e. Fluent Bit chunks could be directly "compacted"); however, if the chunks are
// small, the compression ratio would deteriorate. "Trash compactor" design provides protection from
// log loss during abrupt crashes and maintains a high compression ratio.
type Writer struct {
	useDiskBuffer bool
	irBuffer      io.ReadWriter
	zstdBuffer    io.ReadWriter
	irWriter      *ir.Writer
	size          int
	timezone      string
	tagKey        string
	irTotalBytes  int
	zstdWriter    *zstd.Encoder
}

// Opens a new [Writer].
//
// Parameters:
//   - timezone: Time zone of the log source
//   - size: Byte length
//   - useDiskBuffer: On/off for disk buffering
//   - irBuffer: Buffer for IR
//   - ZstdBuffer: Buffer for Zstd compressed IR
//
// Returns:
//   - Writer: Writer for Zstd compressed IR
//   - err: Error opening Zstd writer, error opening IR writer
func NewWriter(
	timezone string,
	size int,
	tagKey string,
	useDiskBuffer bool,
	irBuffer io.ReadWriter,
	zstdBuffer io.ReadWriter,
) (*Writer, error) {
	zstdWriter, err := zstd.NewWriter(zstdBuffer)
	if err != nil {
		return nil, fmt.Errorf("error opening zstd writer: %w", err)
	}

	// IR buffer using bytes.Buffer internally, so it will dynamically grow if undersized. Using
	// FourByteEncoding as default encoding.
	irWriter, err := ir.NewWriterSize[ir.FourByteEncoding](size, timezone)
	if err != nil {
		return nil, fmt.Errorf("error opening IR writer: %w", err)
	}

	writer := Writer{
		useDiskBuffer: useDiskBuffer,
		size:          size,
		tagKey:        tagKey,
		timezone:      timezone,
		irBuffer:      irBuffer,
		zstdBuffer:    zstdBuffer,
		irWriter:      irWriter,
		zstdWriter:    zstdWriter,
	}

	return &writer, nil
}

// Converts log events to Zstd compressed IR and outputs to the Zstd buffer. IR may be temporarily
// stored in the IR buffer until it surpasses [irSizeThreshold] with compression to Zstd pushed out
// to a later call. See [Writer] for more specific details on behaviour.
//
// Parameters:
//   - logEvents: A slice of log events to be encoded
//
// Returns:
//   - err: Error writing IR/Zstd, error flushing buffers
func (w *Writer) WriteIrZstd(logEvents []ffi.LogEvent) error {
	// Write log events to IR writer buffer.
	err := writeIr(w.irWriter, logEvents)
	if err != nil {
		return err
	}

	// If disk buffering is off, write directly to the Zstd buffer (skipping the IR buffer).
	if !w.useDiskBuffer {
		_, err = w.irWriter.WriteTo(w.zstdWriter)
		return err
	}

	numBytes, err := w.irWriter.WriteTo(w.irBuffer)
	if err != nil {
		return err
	}

	w.irTotalBytes += int(numBytes)

	// If total bytes greater than IR size threshold, compress IR into Zstd frame. Else keep
	// accumulating IR in the buffer until threshold is reached.
	if w.irTotalBytes >= irSizeThreshold {
		err := w.flushIrBuffer()
		if err != nil {
			return fmt.Errorf("error flushing IR buffer: %w", err)
		}
	}

	return nil
}

// Closes IR stream and Zstd frame. Add trailing byte(s) required for IR/Zstd decoding. If
// UseDiskBuffer is true, the IR buffer is also flushed before ending stream. After calling close,
// [Writer] must be reset prior to calling write.
//
// Returns:
//   - err: Error flushing/closing buffers
func (w *Writer) Close() error {
	// IR buffer may not be empty, so must be flushed prior to adding trailing EndOfStream byte. If
	// not using disk buffering, IR writer buffer should always be empty since it is always flushed
	// to Zstd buffer on write.
	if w.useDiskBuffer {
		err := w.flushIrBuffer()
		if err != nil {
			return fmt.Errorf("error flushing IR buffer: %w", err)
		}
	}

	_, err := w.irWriter.CloseTo(w.zstdWriter)
	if err != nil {
		return err
	}
	w.irWriter = nil

	err = w.zstdWriter.Close()
	if err != nil {
		return err
	}

	if !w.useDiskBuffer {
		return nil
	}

	err = diskBufferSeek(w.zstdBuffer, 0, io.SeekStart)
	if err != nil {
		return err
	}

	return nil
}

// Reinitializes [Writer] after calling close. Resets individual IR and Zstd writers and
// associated buffers.
//
// Returns:
//   - err: Error opening IR writer, error IR buffer not empty, error with type assertion
func (w *Writer) Reset() error {
	// Make a new IR writer to get new preamble.
	var err error
	w.irWriter, err = ir.NewWriterSize[ir.FourByteEncoding](w.size, w.timezone)
	if err != nil {
		return err
	}

	if !w.useDiskBuffer {
		buf, ok := w.zstdBuffer.(*bytes.Buffer)
		if !ok {
			return fmt.Errorf("error type assertion from buffer to bytes.Buffer failed")
		}
		buf.Reset()
		w.zstdWriter.Reset(w.zstdBuffer)
		return nil
	}

	// Flush should be called prior to reset, so buffer should be empty. There may be a future
	// use case to truncate a non-empty IR buffer; however, there is currently no use case
	// so safer to throw an error.
	if w.irTotalBytes != 0 {
		return fmt.Errorf("error IR buffer is not empty")
	}

	err = truncateDiskBuffer(w.zstdBuffer)
	if err != nil {
		return err
	}

	// Re-initialize Zstd writer to receive more input.
	w.zstdWriter.Reset(w.zstdBuffer)

	return nil
}

// Gets the size of a Zstd disk buffer. [zstd] does not provide the amount of bytes written with
// each write. Therefore, cannot keep track of size with variable as implemented for IR with
// [IrTotalBytes]. Instead, call stat to get size.
//
// Parameters:
//   - buffer: Disk buffer
//
// Returns:
//   - size: Size of input file
//   - err: Error asserting type, error from stat
func (w *Writer) GetZstdDiskBufferSize() (int, error) {
	file, ok := w.zstdBuffer.(*os.File)
	if !ok {
		return 0, fmt.Errorf("error type assertion from buffer to os.File failed")
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(fileInfo.Size()), nil
}

// Compresses contents of the IR buffer and outputs it to the Zstd buffer. The IR buffer is then
// reset.
//
// Returns:
//   - err: Error called with non-existent buffer, error compressing to Zstd, error resetting Zstd
//
// Writer, error with type assertion, error truncating file
func (w *Writer) flushIrBuffer() error {
	if (w.irBuffer == nil) || (w.zstdBuffer == nil) {
		return fmt.Errorf("error flush called with non-existent buffer")
	}

	// Flush is called during Close(), and possible that the IR buffer is empty.
	if w.irTotalBytes == 0 {
		return nil
	}

	log.Printf("flushing IR buffer %s", w.tagKey)

	err := diskBufferSeek(w.irBuffer, 0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = io.Copy(w.zstdWriter, w.irBuffer)
	if err != nil {
		return err
	}

	err = w.zstdWriter.Close()
	if err != nil {
		return err
	}

	// The Zstd buffer is not reset since it should keep accumulating frames until ready to upload.
	w.zstdWriter.Reset(w.zstdBuffer)

	err = truncateDiskBuffer(w.irBuffer)
	if err != nil {
		return err
	}

	w.irTotalBytes = 0

	return nil
}

// Writes log events to a IR Writer.
//
// Parameters:
//   - irWriter: CLP IR writer to write each log event with
//   - logEvents: A slice of log events to be encoded
//
// Returns:
//   - err: error if an event could not be written
func writeIr(irWriter *ir.Writer, logEvents []ffi.LogEvent) error {
	for _, event := range logEvents {
		_, err := irWriter.Write(event)
		if err != nil {
			err = fmt.Errorf("failed to encode event %v into ir: %w", event, err)
			return err
		}
	}
	return nil
}

// Getter for Zstd buffer. Downgrades from io.ReadWriter to io.Reader for safety.
func (w *Writer) GetZstdBuffer() io.Reader {
	return w.zstdBuffer
}

// Truncates contents of disk buffer.
//
// Parameters:
//   - diskBuffer: Buffer file
//
// Returns:
//   - err: error with type assertion, error with truncate
func truncateDiskBuffer(diskBuffer io.ReadWriter) error {
	file, ok := diskBuffer.(*os.File)
	if !ok {
		return fmt.Errorf("error type assertion from buffer to os.File failed")
	}

	file.Seek(0, io.SeekStart)

	err := file.Truncate(0)
	if err != nil {
		return err
	}

	return nil
}

// Seek for disk buffer.
//
// Parameters:
//   - diskBuffer: Buffer file
//   - offset: Byte offset
//   - whence: Seek whence values. Either io.SeekStart, io.SeekCurrent, and io.SeekEnd.
//
// Returns:
//   - err: error with type assertion, error with seek
func diskBufferSeek(diskBuffer io.ReadWriter, offset int64, whence int) error {
	file, ok := diskBuffer.(*os.File)
	if !ok {
		return fmt.Errorf("error type assertion from buffer to os.File failed")
	}

	_, err := file.Seek(offset, whence)
	if err != nil {
		return fmt.Errorf("error seeking disk buffer: %w", err)
	}

	return nil
}

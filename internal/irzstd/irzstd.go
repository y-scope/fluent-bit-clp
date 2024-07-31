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
	irFile        *os.File
	zstdFile      *os.File
	zstdMemBuf    *bytes.Buffer
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
//   - irFile: IR file for disk buffer
//   - zstdFile: Zstd file for disk buffer
//   - zstdMemBuf: Memory buffer when disk buffer off
//
// Returns:
//   - Writer: Writer for Zstd compressed IR
//   - err: Error opening Zstd writer, error opening IR writer, error getting size of disk buffer
func NewWriter(
	timezone string,
	size int,
	tagKey string,
	useDiskBuffer bool,
	irFile *os.File,
	zstdFile *os.File,
	zstdMemBuf *bytes.Buffer,
) (*Writer, error) {

	var zstdOuput io.Writer

	if useDiskBuffer {
		zstdOuput = zstdFile
	} else {
		zstdOuput = zstdMemBuf
	}

	zstdWriter, err := zstd.NewWriter(zstdOuput)
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
		irFile:        irFile,
		zstdFile:      zstdFile,
		irWriter:      irWriter,
		zstdWriter:    zstdWriter,
	}

	if !useDiskBuffer {
		return &writer, nil
	}

	irFileSize, zstdFileSize, err := writer.GetFileSizes()
	if err != nil {
		return nil, fmt.Errorf("error getting size of disk buffer: %w", err)
	}

	if (irFileSize > 0) || (zstdFileSize > 0) {
		// During recovery, IR buffer may not be empty, so the size must be set. In addition,
		// the non-empty disk buffers already have existing preamble so remove it.
		writer.irTotalBytes = irFileSize
		irWriter.Reset()
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

	numBytes, err := w.irWriter.WriteTo(w.irFile)
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

	_, err = w.zstdFile.Seek(0, io.SeekStart)
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
	var err error
	w.irWriter, err = ir.NewWriterSize[ir.FourByteEncoding](w.size, w.timezone)
	if err != nil {
		return err
	}

	if !w.useDiskBuffer {
		w.zstdMemBuf.Reset()
		w.zstdWriter.Reset(w.zstdMemBuf)
		return nil
	}

	// Flush should be called prior to reset, so buffer should be empty. There may be a future
	// use case to truncate a non-empty IR buffer; however, there is currently no use case
	// so safer to throw an error.
	if w.irTotalBytes != 0 {
		return fmt.Errorf("error IR buffer is not empty")
	}

	_, err = w.zstdFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = w.zstdFile.Truncate(0)
	if err != nil {
		return err
	}

	w.zstdWriter.Reset(w.zstdFile)

	return nil
}

// Compresses contents of the IR buffer and outputs it to the Zstd buffer. The IR buffer is then
// reset.
//
// Returns:
//   - err: Error called with non-existent buffer, error compressing to Zstd, error resetting Zstd
//
// Writer, error with type assertion, error truncating file
func (w *Writer) flushIrBuffer() error {
	if (w.irFile == nil) || (w.zstdFile == nil) {
		return fmt.Errorf("error flush called with non-existent buffer")
	}

	// Flush is called during Close(), and possible that the IR buffer is empty.
	if w.irTotalBytes == 0 {
		return nil
	}

	log.Printf("flushing IR buffer %s", w.tagKey)

	_, err := w.irFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = io.Copy(w.zstdWriter, w.irFile)
	if err != nil {
		return err
	}

	err = w.zstdWriter.Close()
	if err != nil {
		return err
	}

	// The Zstd file is not truncated since it should keep accumulating frames until ready to
	// upload.
	w.zstdWriter.Reset(w.zstdFile)

	_, err = w.irFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = w.irFile.Truncate(0)
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

// Getter for IR File.
func (w *Writer) GetIrFile() *os.File {
	return w.irFile
}

// Getter for Zstd File.
func (w *Writer) GetZstdFile() *os.File {
	return w.zstdFile
}

// Getter for Zstd Output.
func (w *Writer) GetZstdOutput() io.Reader {
	if !w.useDiskBuffer {
		return w.zstdMemBuf
	}
	return w.zstdFile
}

// Get size of IR and Zstd files. In general, can use [irTotalBytes] to track size of IR file;
// however, [irTotalBytes] will only track writes by current process and will not have info for
// recovered stores. For recovered stores, must use stat to get size. [zstd] does not provide the
// amount of bytes written with each write. Therefore, cannot keep track of size with variable as
// implemented for IR with [IrTotalBytes]. Instead, must always use stat.
//
// Returns:
//   - err: Error called with useDiskBuffer off, error calling stat
func (w *Writer) GetFileSizes() (int, int, error) {

	if !w.useDiskBuffer {
		return 0, 0, fmt.Errorf("error cannot get file sizes when useDiskBuffer is off")
	}

	irFileInfo, err := w.irFile.Stat()
	if err != nil {
		return 0, 0, err
	}

	irFileSize := int(irFileInfo.Size())

	zstdFileInfo, err := w.zstdFile.Stat()
	if err != nil {
		return 0, 0, err
	}

	zstdFileSize := int(zstdFileInfo.Size())

	return irFileSize, zstdFileSize, err
}

// Package implements writer that converts log events to Zstd compressed IR. Effectively chaining
// together [ir.Writer] and [zstd.Encoder] in series.

package irzstd

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

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
	irPath        string
	irFile        *os.File
	zstdFile      *os.File
	zstdMemBuf    *bytes.Buffer
	irWriter      *ir.Writer
	size          int
	timezone      string
	irTotalBytes  int
	zstdWriter    *zstd.Encoder
}

// Opens a new [Writer] with a memory buffer for Zstd Output. For use when use_disk_store is off.
// Writer does not use IR buffer.
//
// Parameters:
//   - timezone: Time zone of the log source
//   - size: Byte length
//
// Returns:
//   - Writer: Writer for Zstd compressed IR
//   - err: Error opening Zstd/IR writers
func NewMemWriter(timezone string, size int) (*Writer, error) {
	var membuf bytes.Buffer
	irWriter, zstdWriter, err := newIrZstdWriters(&membuf, timezone, size)
	if err != nil {
		return nil, err
	}

	writer := Writer{
		size:       size,
		timezone:   timezone,
		irWriter:   irWriter,
		zstdWriter: zstdWriter,
		zstdMemBuf: &membuf,
	}

	return &writer, nil
}

// Opens a new [Writer] using disk files for IR and Zstd buffers. For use when use_disk_store
// is on.
//
// Parameters:
//   - timezone: Time zone of the log source
//   - size: Byte length
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
//
// Returns:
//   - Writer: Writer for Zstd compressed IR
//   - err: Error creating new buffers, error opening Zstd/IR writers
func NewDiskWriter(
	timezone string,
	size int,
	irPath string,
	zstdPath string,
) (*Writer, error) {
	irFile, zstdFile, err := newFileBuffers(irPath, zstdPath)
	if err != nil {
		return nil, err
	}

	irWriter, zstdWriter, err := newIrZstdWriters(zstdFile, timezone, size)
	if err != nil {
		return nil, err
	}

	writer := Writer{
		useDiskBuffer: true,
		size:          size,
		timezone:      timezone,
		irPath:        irPath,
		irFile:        irFile,
		zstdFile:      zstdFile,
		irWriter:      irWriter,
		zstdWriter:    zstdWriter,
	}

	return &writer, nil
}

// Recovers a [writer] opening buffer files from a previous execution of output plugin.
// Recovery of files necessitates that use_disk_store is on. IR preamble is removed for
// recovered store.
//
// Parameters:
//   - timezone: Time zone of the log source
//   - size: Byte length
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
//
// Returns:
//   - Writer: Writer for Zstd compressed IR
//   - err: Error opening buffers, error opening Zstd/IR writers, error getting file sizes
func RecoverWriter(
	timezone string,
	size int,
	irPath string,
	zstdPath string,
) (*Writer, error) {
	irFile, zstdFile, err := openBufferFiles(irPath, zstdPath)
	if err != nil {
		return nil, fmt.Errorf("error opening files: %w", err)
	}

	irWriter, zstdWriter, err := newIrZstdWriters(zstdFile, timezone, size)
	if err != nil {
		return nil, err
	}

	writer := Writer{
		useDiskBuffer: true,
		size:          size,
		timezone:      timezone,
		irPath:        irPath,
		irFile:        irFile,
		zstdFile:      zstdFile,
		irWriter:      irWriter,
		zstdWriter:    zstdWriter,
	}

	irFileSize, _, err := writer.GetFileSizes()
	if err != nil {
		return nil, fmt.Errorf("error getting size of disk buffer: %w", err)
	}

	// During recovery, IR buffer may not be empty, so the size must be set. In addition,
	// the non-empty disk buffers already have existing preamble so remove it. Disk buffer
	// must have non-zero size or else would be deleted in recover.
	writer.irTotalBytes = irFileSize
	irWriter.Reset()

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
func (w *Writer) CloseStreams() error {
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

// Reinitialize [Writer] after calling [CloseStreams]. Resets individual IR and Zstd writers and
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

// Closes [Writer]. Currently used during recovery only, and advise caution using elsewhere.
// Using [ir.Writer.Serializer.Close] instead of [ir.Writer.Close] so EndofStream byte is not
// added. It is preferable to add postamble on recovery so that IR is in the same state
// (i.e. not terminated) for an abrupt crash and a graceful exit. Function does not call
// [zstd.Encoder.Close] as it does not explicitly free memory and may add undesirable null frame.
//
// Returns:
//   - err: Error closing irWriter, error closing files
func (w *Writer) Close() error {
	if w.irWriter != nil {
		err := w.irWriter.Serializer.Close()
		if err != nil {
			return fmt.Errorf("error could not close irWriter: %w", err)
		}
	}

	if !w.useDiskBuffer {
		return nil
	}

	err := w.irFile.Close()
	if err != nil {
		return fmt.Errorf("error could not close IR file %s: %w", w.irPath, err)
	}

	err = w.zstdFile.Close()
	if err != nil {
		return fmt.Errorf("error could not close Zstd file %s: %w", w.irPath, err)
	}

	return nil
}

// Getter for Zstd Output.
// Returns:
//   - zstdOutput: Reader for writer Zstd output
func (w *Writer) GetZstdOutput() io.Reader {
	if !w.useDiskBuffer {
		return w.zstdMemBuf
	}
	return w.zstdFile
}

// Getter for useDiskBuffer.
// Returns:
//   - useDiskBuffer: On/off for disk buffering
func (w *Writer) GetUseDiskBuffer() bool {
	return w.useDiskBuffer
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

	log.Printf("flushing IR buffer %s", filepath.Base(w.irPath))

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

// Creates file buffers to hold logs prior to sending to s3.
//
// Parameters:
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
//
// Returns:
//   - irFile: File for IR
//   - zstdFile: File for Zstd
//   - err: Error creating file
func newFileBuffers(
	irPath string,
	zstdPath string,
) (*os.File, *os.File, error) {
	irFile, err := createFile(irPath)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating file %s: %w", irPath, err)
	}
	log.Printf("created file %s", irPath)

	zstdFile, err := createFile(zstdPath)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating file %s: %w", zstdPath, err)
	}
	log.Printf("created file %s", zstdPath)

	return irFile, zstdFile, nil
}

// Creates a new file.
//
// Parameters:
//   - path: Path to file
//
// Returns:
//   - f: The created file
//   - err: Could not create directory, could not create file
func createFile(path string) (*os.File, error) {
	// Make directory if does not exist.
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0o751)
	if err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", dir, err)
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o751)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %w", path, err)
	}
	return f, nil
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

// Opens IR and Zstd disk buffer files. Zstd file whence is [io.SeekEnd].
//
// Parameters:
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
//
// Returns:
//   - err: error opening files
func openBufferFiles(irPath string, zstdPath string) (*os.File, *os.File, error) {
	irFile, err := os.OpenFile(irPath, os.O_RDWR, 0o751)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening ir file %s: %w", irPath, err)
	}

	zstdFile, err := os.OpenFile(zstdPath, os.O_RDWR, 0o751)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening zstd file %s: %w", zstdPath, err)
	}

	// Seek to end of Zstd disk buffer. Not using append flag since we need to seek later and
	// docs provide a warning against seeking & opening with append flag.
	// https://pkg.go.dev/os#File.Seek
	_, err = zstdFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, nil, fmt.Errorf("error seeking zstd file %s: %w", zstdPath, err)
	}

	return irFile, zstdFile, nil
}

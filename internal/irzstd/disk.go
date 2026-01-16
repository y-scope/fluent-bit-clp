package irzstd

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

// 2 MB threshold to buffer IR before compressing to Zstd.
const irSizeThreshold = 2 << 20

// dirPermission is the permission mode for created directories.
const dirPermission = 0o750

// filePermission is the permission mode for created files.
const filePermission = 0o600

// errFlushingIrBuffer is a format string for IR buffer flush errors.
const errFlushingIrBuffer = "error flushing IR buffer: %w"

// errCreatingFile is a format string for file creation errors.
const errCreatingFile = "error creating file %s: %w"

// logCreatedFile is a log format for file creation messages.
const logCreatedFile = "created file %s"

// errIrBufferNotEmpty is returned when attempting to reset a non-empty IR buffer.
var errIrBufferNotEmpty = errors.New("error IR buffer is not empty")

// errFlushNonExistentBuffer is returned when flush is called with nil buffers.
var errFlushNonExistentBuffer = errors.New("error flush called with non-existent buffer")

// Converts log events into Zstd compressed IR using "trash compactor" design. Log events are
// converted to uncompressed IR and buffered into "bins". Uncompressed IR represents uncompressed
// trash in "trash compactor". Once the bin is full, the bin is "compacted" into its own separate
// Zstd frame. The compressor is explicitly closed after receiving input terminating the Zstd frame.
// Stacks of Zstd frames are then sent to S3.  For majority of runtime, log events are stored as a
// mixture uncompressed IR and compressed Zstd frames. A simpler approach would be to send all the
// events for one S3 upload to the streaming compressor and only close the stream when the upload
// size is reached. However, the streaming compressor will keep frames/blocks open in between
// receipt of Fluent Bit chunks. Open frames/blocks may not be recoverable after an abrupt crash.
// Closed frames on the other hand are valid Zstd and can be send to s3 on startup. It is not
// explicitly necessary to buffer IR into "bins" (i.e. Fluent Bit chunks could be directly
// "compacted"); however, if the chunks are small, the compression ratio would deteriorate. "Trash
// compactor" design provides protection from log loss during abrupt crashes and maintains a high
// compression ratio.
type DiskWriter struct {
	irPath       string // Path variable for debugging
	zstdPath     string // Path variable for debugging
	irFile       *os.File
	zstdFile     *os.File
	irWriter     *ir.Writer
	size         int
	timezone     string
	irTotalBytes int
	zstdWriter   *zstd.Encoder
}

// Opens a new [DiskWriter] using files for IR and Zstd buffers. For use when use_disk_store
// is on.
//
// Parameters:
//   - timezone: Time zone of the log source
//   - size: Byte length
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
//
// Returns:
//   - DiskWriter: Disk writer for Zstd compressed IR
//   - err: Error creating new buffers, error opening Zstd/IR writers
func NewDiskWriter(
	timezone string,
	size int,
	irPath string,
	zstdPath string,
) (*DiskWriter, error) {
	irFile, zstdFile, err := newFileBuffers(irPath, zstdPath)
	if err != nil {
		return nil, err
	}

	irWriter, zstdWriter, err := newIrZstdWriters(zstdFile)
	if err != nil {
		return nil, err
	}

	writer := DiskWriter{
		size:       size,
		timezone:   timezone,
		irPath:     irPath,
		irFile:     irFile,
		zstdPath:   zstdPath,
		zstdFile:   zstdFile,
		irWriter:   irWriter,
		zstdWriter: zstdWriter,
	}

	return &writer, nil
}

// Recovers a [DiskWriter] opening buffer files from a previous execution of output plugin.
// Recovery of files necessitates that use_disk_store is on. IR preamble is removed for
// recovered store. Avoid use with empty disk stores as there will be no preamble.
//
// Parameters:
//   - timezone: Time zone of the log source
//   - size: Byte length
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
//
// Returns:
//   - DiskWriter: Disk writer for Zstd compressed IR
//   - err: Error opening buffers, error opening Zstd/IR writers, error getting file sizes
func RecoverWriter(
	timezone string,
	size int,
	irPath string,
	zstdPath string,
) (*DiskWriter, error) {
	irFile, zstdFile, err := openBufferFiles(irPath, zstdPath)
	if err != nil {
		return nil, fmt.Errorf("error opening files: %w", err)
	}

	irWriter, zstdWriter, err := newIrZstdWriters(zstdFile)
	if err != nil {
		return nil, err
	}

	writer := DiskWriter{
		size:       size,
		timezone:   timezone,
		irPath:     irPath,
		irFile:     irFile,
		zstdPath:   zstdPath,
		zstdFile:   zstdFile,
		irWriter:   irWriter,
		zstdWriter: zstdWriter,
	}

	irFileSize, err := writer.getIrFileSize()
	if err != nil {
		return nil, fmt.Errorf("error getting size of IR file: %w", err)
	}

	// During recovery, IR buffer may not be empty, so the size must be set. In addition,
	// the non-empty disk buffers already have existing preamble so remove it. Disk buffer
	// must have non-zero size or else would be deleted in recover.
	writer.irTotalBytes = irFileSize

	return &writer, nil
}

// Converts log events to Zstd compressed IR and outputs to the Zstd file. IR is temporarily
// stored in the IR file until it surpasses [irSizeThreshold] with compression to Zstd pushed out
// to a later call. See [DiskWriter] for more specific details on behaviour.
//
// Parameters:
//   - logEvents: A slice of log events to be encoded
//
// Returns:
//   - numEvents: Number of log events successfully written to IR writer buffer
//   - err: Error writing IR/Zstd, error flushing buffers
func (w *DiskWriter) WriteIrZstd(logEvents []ffi.LogEvent) (int, error) {
	numBytes, numEvents, err := writeIr(w.irWriter, logEvents)
	if err != nil {
		return numEvents, err
	}

	w.irTotalBytes += int(numBytes)

	// If total bytes greater than IR size threshold, compress IR into Zstd frame. Else keep
	// accumulating IR in the buffer until threshold is reached.
	if w.irTotalBytes >= irSizeThreshold {
		err := w.flushIrBuffer()
		if err != nil {
			return numEvents, fmt.Errorf(errFlushingIrBuffer, err)
		}
	}

	return numEvents, nil
}

// Closes IR stream and Zstd frame. Add trailing byte(s) required for IR/Zstd decoding.
// The IR buffer is also flushed before ending stream. After calling close,
// [DiskWriter] must be reset prior to calling write.
//
// Returns:
//   - err: Error flushing/closing buffers
func (w *DiskWriter) CloseStreams() error {
	// IR buffer may not be empty, so must be flushed prior to adding trailing EndOfStream byte.
	err := w.flushIrBuffer()
	if err != nil {
		return fmt.Errorf(errFlushingIrBuffer, err)
	}

	err = w.irWriter.Close()
	if err != nil {
		return err
	}

	w.irWriter = nil

	err = w.zstdWriter.Close()
	if err != nil {
		return err
	}

	_, err = w.zstdFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	return nil
}

// Reinitialize [DiskWriter] after calling CloseStreams(). Resets individual IR and Zstd writers and
// associated buffers.
//
// Returns:
//   - err: Error opening IR writer, error IR buffer not empty
func (w *DiskWriter) Reset() error {
	// Flush should be called prior to reset, so buffer should be empty. There may be a future
	// use case to truncate a non-empty IR buffer; however, there is currently no use case
	// so safer to throw an error.
	if w.irTotalBytes != 0 {
		return errIrBufferNotEmpty
	}

	_, err := w.zstdFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = w.zstdFile.Truncate(0)
	if err != nil {
		return err
	}

	w.zstdWriter.Reset(w.zstdFile)

	w.irWriter, err = ir.NewWriter[ir.FourByteEncoding](w.zstdWriter)
	if err != nil {
		return err
	}

	return nil
}

// Closes [DiskWriter]. Currently used during recovery only, and advise caution using elsewhere.
// Using [ir.Writer.Serializer.Close] instead of [ir.Writer.Close] so EndofStream byte is not
// added. It is preferable to add postamble on recovery so that IR is in the same state
// (i.e. not terminated) for an abrupt crash and a graceful exit. Function does not call
// [zstd.Encoder.Close] as it does not explicitly free memory and may add undesirable null frame.
//
// Returns:
//   - err: Error closing irWriter, error closing files
func (w *DiskWriter) Close() error {
	if w.irWriter != nil {
		err := w.irWriter.Serializer.Close()
		if err != nil {
			return fmt.Errorf("error could not close irWriter: %w", err)
		}
	}

	err := w.irFile.Close()
	if err != nil {
		return fmt.Errorf("error could not close IR file %s: %w", w.irPath, err)
	}

	err = w.zstdFile.Close()
	if err != nil {
		return fmt.Errorf("error could not close Zstd file %s: %w", w.zstdPath, err)
	}

	return nil
}

// Getter for useDiskBuffer.
//
// Returns:
//   - useDiskBuffer: On/off for disk buffering
func (*DiskWriter) GetUseDiskBuffer() bool {
	return true
}

// Getter for Zstd Output.
//
// Returns:
//   - zstdOutput: Reader for Zstd output
func (w *DiskWriter) GetZstdOutput() io.Reader {
	return w.zstdFile
}

// Get size of Zstd output. [zstd] does not provide the amount of bytes written with each write.
// Therefore, cannot keep track of size with variable as implemented for IR with [IrTotalBytes].
// Instead, must always use stat.
//
// Returns:
//   - err: Error calling stat
func (w *DiskWriter) GetZstdOutputSize() (int, error) {
	zstdFileInfo, err := w.zstdFile.Stat()
	if err != nil {
		return 0, err
	}

	zstdFileSize := int(zstdFileInfo.Size())

	return zstdFileSize, err
}

// Compresses contents of the IR file and outputs it to the Zstd file. The IR file is then
// truncated.
//
// Returns:
//   - err: Error nil files, error from Zstd Encoder, error from operations on file
func (w *DiskWriter) flushIrBuffer() error {
	if (w.irFile == nil) || (w.zstdFile == nil) {
		return errFlushNonExistentBuffer
	}

	// Flush is called during Close(), and possible that the IR buffer is empty.
	if w.irTotalBytes == 0 {
		return nil
	}

	log.Printf("flushing IR buffer %s", filepath.Base(w.irPath))

	if err := w.compressIrToZstd(); err != nil {
		return err
	}

	if err := w.truncateIrFile(); err != nil {
		return err
	}

	w.irTotalBytes = 0

	return nil
}

// compressIrToZstd copies IR file contents to Zstd writer and closes the frame.
func (w *DiskWriter) compressIrToZstd() error {
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

	return nil
}

// truncateIrFile resets the IR file for the next batch of writes.
func (w *DiskWriter) truncateIrFile() error {
	_, err := w.irFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	return w.irFile.Truncate(0)
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
) (irFile *os.File, zstdFile *os.File, err error) {
	irFile, err = createFile(irPath)
	if err != nil {
		return nil, nil, fmt.Errorf(errCreatingFile, irPath, err)
	}
	log.Printf(logCreatedFile, irPath)

	zstdFile, err = createFile(zstdPath)
	if err != nil {
		return nil, nil, fmt.Errorf(errCreatingFile, zstdPath, err)
	}
	log.Printf(logCreatedFile, zstdPath)

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
	err := os.MkdirAll(dir, dirPermission)
	if err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", dir, err)
		return nil, err
	}

	//nolint:gosec // path is validated by caller
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, filePermission)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %w", path, err)
	}
	return f, nil
}

// Opens IR and Zstd disk buffer files. Zstd file whence is [io.SeekEnd].
//
// Parameters:
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
//
// Returns:
//   - irFile: File for IR
//   - zstdFile: File for Zstd
//   - err: error opening files
func openBufferFiles(
	irPath string,
	zstdPath string,
) (irFile *os.File, zstdFile *os.File, err error) {
	//nolint:gosec // path is validated by caller
	irFile, err = os.OpenFile(irPath, os.O_RDWR, filePermission)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening ir file %s: %w", irPath, err)
	}

	//nolint:gosec // path is validated by caller
	zstdFile, err = os.OpenFile(zstdPath, os.O_RDWR, filePermission)
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

// Get size of IR file. In general, can use [irTotalBytes] to track size of IR file;
// however, [irTotalBytes] will only track writes by current process and will not have info for
// recovered stores.
//
// Returns:
//   - err: Error calling stat
func (w *DiskWriter) getIrFileSize() (int, error) {
	irFileInfo, err := w.irFile.Stat()
	if err != nil {
		return 0, err
	}

	irFileSize := int(irFileInfo.Size())
	return irFileSize, err
}

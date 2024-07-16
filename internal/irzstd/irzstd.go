// Package implements writer with that converts log events to Zstd compressed IR. Effectively chains together [ir.Writer] and [zstd.Encoder] in series.

package irzstd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"log"

	"github.com/klauspost/compress/zstd"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

// 2 MB threshold to buffer IR before compressing to Zstd when disk store is on.
// const irSizeThreshold = 2 << 20
const irSizeThreshold = 10000

// Converts log events into Zstd compressed IR. Writer can be initialized with disk store on/off
// depending on user configuration.
//
// Behavior with disk store off:
// Log events provided to writer are immediately converted to Zstd compressed IR and stored in
// [IrZstdWriter.ZstdStore]. With disk store off, Zstd store is a memory buffer. After the Zstd
// store recieves logs, they are immediately sent to s3. There is no IR store and it is set to nil.
//
// Behavior with disk store on:
// Logs events are not immediately converted to zstd compressed IR, and instead compressed using
// "trash compactor" design. Log events are converted to uncompressed IR and buffered into "bins".
// Uncompressed IR represents uncompressed trash in "trash compactor". Once the bin is full, the
// bin is "compacted" into its own seperate Zstd frame. The compressor is explicitly closed
// after recieving input terminating the Zstd frame. Stacks of Zstd frames are then sent to S3.
// For majority of runtime, log events are stored as a mixture uncompressed IR and compressed
// Zstd frames. A simpler approach would be to send all the events for one S3 upload to the streaming
// compressor and only close the stream when the upload size is reached. However, the streaming
// compressor will keep frames/blocks open in between receipt of Fluent Bit chunks. Open
// frames/blocks may not be recoverable after an abrupt crash. Closed frames on the other hand are
// valid Zstd and can be send to s3 on startup. It is not explicity neccesary to buffer IR into
// "bins" (i.e. Fluent Bit chunks could be directly "compacted"); however, if the chunks are
// small, the compression ratio would deteriorate. "Trash compactor" design provides protection
// from log loss during abrupt crashes and maintains a high compression ratio.
type IrZstdWriter struct {
	DiskStore    bool
	IrStore      io.ReadWriter
	ZstdStore    io.ReadWriter
	IrWriter     *ir.Writer
	Size         int
	Timezone     string
	IrTotalBytes int
	ZstdWriter   *zstd.Encoder
}

// Opens a new [IrZstdWriter].
//
// Parameters:
//   - timezone: Time zone of the log source
//   - size: Byte length
//   - diskStore: On/off for disk store
//   - irStore: Location to store IR
//   - ZstdStore: Location to store Zstd compressed IR
//
// Returns:
//   - IrZstdWriter: Writer for Zstd compressed IR
//   - err: Error opening Zstd Writer, error opening IR Writer
func NewIrZstdWriter(timezone string, size int, diskStore bool, irStore io.ReadWriter, zstdStore io.ReadWriter) (*IrZstdWriter, error) {
	// Create Zstd writer with Zstd Store as its output.
	zstdWriter, err := zstd.NewWriter(zstdStore)
	if err != nil {
		return nil, fmt.Errorf("error opening zstd writer: %w", err)
	}

	// IR buffer using bytes.Buffer internally, so it will dynamically grow if undersized. Using
	// FourByteEncoding as default encoding.
	irWriter, err := ir.NewWriterSize[ir.FourByteEncoding](size, timezone)
	if err != nil {
		return nil, fmt.Errorf("error opening IR writer: %w", err)
	}

	IrZstdWriter := IrZstdWriter{
		DiskStore:  diskStore,
		Size:       size,
		Timezone:   timezone,
		IrStore:    irStore,
		ZstdStore:  zstdStore,
		IrWriter:   irWriter,
		ZstdWriter: zstdWriter,
	}

	return &IrZstdWriter, nil
}

// Converts log events to Zstd compressed IR and outputs to Zstd Store. IR may be temporarily
// stored in IR store until store surpasses [irSizeThreshold] with compression to Zstd pushed out to
// a later call. See [IrZstdWriter] for more specific details on behaviour.
//
// Parameters:
//   - logEvents: A slice of log events to be encoded
//
// Returns:
//   - err: Error writting IR/Zstd, error flushing stores
func (w *IrZstdWriter) WriteIrZstd(logEvents []ffi.LogEvent) error {
	// Write log events to irWriter buffer.
	err := writeIr(w.IrWriter, logEvents)
	if err != nil {
		return err
	}

	// If disk store off, skip writing to IR store.
	if !w.DiskStore {
		// Flush irWriter buffer to zstdWriter.
		_, err := w.IrWriter.WriteTo(w.ZstdWriter)
		if err != nil {
			return err
		}

		return nil
	}

	// Flush irWriter buffer to IR store.
	numBytes, err := w.IrWriter.WriteTo(w.IrStore)
	if err != nil {
		return err
	}

	// Increment total bytes written.
	w.IrTotalBytes += int(numBytes)

	// If total bytes greater than IR threshold, compress IR into Zstd frame. Else keep
	// accumulating IR in store until threshold is reached.
	if (w.IrTotalBytes) >= irSizeThreshold {
		err := w.FlushIrStore()
		if err != nil {
			return fmt.Errorf("error flushing IR store: %w", err)
		}
	}

	return nil
}

// Closes IR stream and Zstd frame. Add trailing byte(s) required for IR/Zstd decoding. If disk
// store on, the IR store is also flushed before ending stream. After calling close, [IrZstdWriter]
// must be reset prior to calling write.
//
// Returns:
//   - err: Error flushing or closing stores
func (w *IrZstdWriter) Close() error {
	// IR store may not be empty, so must be flushed prior to adding trailing EndOfStream byte.
	// If not using disk store, irWriter buffer should always be empty since it is always
	// flushed to Zstd Store on write.
	if w.DiskStore {
		err := w.FlushIrStore()
		if err != nil {
			return fmt.Errorf("error flushing IR store: %w", err)
		}
	}

	// Add EndOfStream byte to IR and flush to zstdWriter.
	_, err := w.IrWriter.CloseTo(w.ZstdWriter)
	if err != nil {
		return err
	}

	// Setting to nil to prevent accidental use. Also, cannot reuse resource like zstdWriter.
	w.IrWriter = nil

	// Terminate Zstd frame.
	err = w.ZstdWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

// Reinitializes [IrZstdWriter] after calling close. Resets individual IR and Zstd writers and
// associated stores.
//
// Returns:
//   - err: Error opening IR writer, error ir store not empty, error with type assertion
func (w *IrZstdWriter) Reset() error {
	// Make a new IR writer to get new preamble.
	var err error
	w.IrWriter, err = ir.NewWriterSize[ir.FourByteEncoding](w.Size, w.Timezone)
	if err != nil {
		return err
	}

	// If using memory store, clear buffer.
	if !w.DiskStore {
		buf, ok := w.ZstdStore.(*bytes.Buffer)
		if !ok {
			return fmt.Errorf("error type assertion from store to buf failed")
		}
		buf.Reset()
		return nil
	} else {
		// Flush should be called prior to reset, so store should be emtpy. There may be a future
		// use case to truncate a non-empty IR store; however, there is currently no use case
		// so safer to throw an error.
		if w.IrTotalBytes != 0 {
			return fmt.Errorf("error ir store is not empty")
		}

		zstdFile, ok := w.ZstdStore.(*os.File)
		if !ok {
			return fmt.Errorf("error type assertion from store to file failed")
		}

		err = zstdFile.Truncate(0)
		if err != nil {
			return err
		}
	}

	// Re-initialize Zstd writer to recieve more input.
	w.ZstdWriter.Reset(w.ZstdStore)

	return nil
}

// Gets the size of a disk store. [zstd] does not provide the amount of bytes written with
// each write. Therefore, cannot keep track of size with variable as implemented for IR with
// [IrTotalBytes]. Instead, call stat to get size.
//
// Parameters:
//   - store: Disk store
//
// Returns:
//   - size: Size of input file
//   - err: Error asserting type, error from stat
func GetDiskStoreSize(store io.ReadWriter) (int, error) {
	file, ok := store.(*os.File)
	if !ok {
		return 0, fmt.Errorf("error type assertion from store to file failed")
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(fileInfo.Size()), nil
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

// Compresses contents of IR store and outputs it to Zstd Store. IR store is then reset.
//
// Returns:
//   - err: Error called with non-existant store, error compressing to Zstd, error resetting Zstd
//
// Writer, error truncating file
func (w *IrZstdWriter) FlushIrStore() error {
	// Assert stores exist
	if (w.IrStore == nil) || (w.ZstdStore == nil) {
		return fmt.Errorf("error flush called with non-existant store")
	}

	// No need to flush if the store is empty.
	// Flush is called during Close(), and possible that IR store is empty.
	if w.IrTotalBytes == 0 {
		return nil
	}

	fmt.Println(w.IrTotalBytes)

	file, _ := w.IrStore.(*os.File)
	file.Seek(0,io.SeekStart)

	// Compressed IR to Zstd.
	_, err := io.Copy(w.ZstdWriter, w.IrStore)
	if err != nil {
		return err
	}

	log.Printf("flushed IR store %s", file.Name())

	// Terminate Zstd frame.
	err = w.ZstdWriter.Close()
	if err != nil {
		return err
	}

	// Re-initialize Zstd writer to recieve more input.
	// The Zstd store is not reset since it should keep accumulating frames until ready to upload.
	w.ZstdWriter.Reset(w.ZstdStore)

	irFile, ok := w.IrStore.(*os.File)
	if !ok {
		return fmt.Errorf("error type assertion from store to file failed")
	}

	file.Seek(0,io.SeekStart)

	// Reset IR Store.
	err = irFile.Truncate(0)
	if err != nil {
		return err
	}

	w.IrTotalBytes = 0

	return nil
}

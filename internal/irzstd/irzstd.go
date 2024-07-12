package irzstd

// using irzstd to prevent namespace collision with [ir].

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/klauspost/compress/zstd"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

// 2 MB
const irSizeThreshold = 2 << 20

const IrDir = "ir"
const ZstdDir = "zstd"

// Converts log events into Zstd compressed IR. Effectively chains [ir.Writer] then [zstd.Encoder] in series.
// Compressed IR output is [io.Writer] provided to [zstd.Encoder].
type IrZstdWriter struct {
	Store        bool
	Size         int
	Timezone     string
	IrTotalBytes int
	IrStore      io.ReadWriter
	ZstdStore    io.ReadWriter
	IrWriter     *ir.Writer
	ZstdWriter   *zstd.Encoder
}

// Creates a new irZstdWriter
//
// Parameters:
//   - writer: Msgpack data
//   - length: Byte length
//   - tag: Fluent Bit tag
//   - S3Config: Plugin configuration
//
// Returns:
//   - code: Fluent Bit success code (OK, RETRY, ERROR)
//   - err: Error if flush fails
func NewIrZstdWriter(timezone string, size int, store bool, irStore io.ReadWriter, zstdStore io.ReadWriter) (*IrZstdWriter, error) {

	// Create Zstd writer with zstdStore as its output.
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
		Store:      store,
		Size:       size,
		Timezone:   timezone,
		IrStore:    irStore,
		ZstdStore:  zstdStore,
		IrWriter:   irWriter,
		ZstdWriter: zstdWriter,
	}

	return &IrZstdWriter, nil
}

// TODO: Improve error handling for partially written bytes.
func (w *IrZstdWriter) WriteIrZstd(logEvents []ffi.LogEvent) (error) {

	// Write log events to irWriter buffer.
	err := writeIr(w.IrWriter, logEvents)
	if err != nil {
		return err
	}

	// If no disk store, skip writing to ir store.
	if !w.Store {
		// Flush irWriter buffer to zstdWriter.
		_, err := w.IrWriter.WriteTo(w.ZstdWriter)
		if err != nil {
			return err
		}

		return nil
	}

	// Flush irWriter buffer to ir disk store.
	numBytes, err := w.IrWriter.WriteTo(w.IrStore)
	if err != nil {
		return err
	}

	// Increment total bytes written.
	w.IrTotalBytes += int(numBytes)

	// If total bytes greater than ir threshold, compress IR into Zstd frame. Else keep
	// accumulating IR in store until threshold is reached.
	if (w.IrTotalBytes) >= irSizeThreshold {
		err := w.FlushIrStore()
		if err != nil {
			return fmt.Errorf("error flushing IR store: %w", err)
		}
	}

	return nil
}

func (w *IrZstdWriter) EndStream() error {
	// Null terminate ir stream and flush to zstdwriter
	_, err := w.IrWriter.CloseTo(w.ZstdWriter)
	if err != nil {
		return err
	}

	err = w.ZstdWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

func (w *IrZstdWriter) Reset() error {
	// Make a new IR writer to get a new preamble.
	var err error
	w.IrWriter, err = ir.NewWriterSize[ir.FourByteEncoding](w.Size, w.Timezone)
	if err != nil {
		return err
	}

	if !w.Store {
		buf, ok := w.ZstdStore.(*bytes.Buffer)
		if !ok {
			return fmt.Errorf("error type assertion from store to buf failed")
		}
		buf.Reset()
		return nil
	}

	zstdFile, ok := w.ZstdStore.(*os.File)
	if !ok {
		return  fmt.Errorf("error type assertion from store to file failed")
	}

	// Reset Zstd Store.
	err = zstdFile.Truncate(0)
	if err != nil {
		return err
	}

	// Re-initialize Zstd writer to recieve more input.
	w.ZstdWriter.Reset(w.ZstdStore)

	return nil
}

func (w *IrZstdWriter) GetZstdStoreSize() (int, error) {
	fileInfo, err := w.ZstdStore.(*os.File).Stat()
	if err != nil {
		return 0, err
	}
	return int(fileInfo.Size()), nil
}

// Writes log events to a IR Writer.
//
// Parameters:
//   - irWriter: CLP IR writer to write each log event with
//   - eventBuffer: A slice of log events to be encoded
//
// Returns:
//   - err: error if an event could not be written
func writeIr(irWriter *ir.Writer, eventBuffer []ffi.LogEvent) error {
	for _, event := range eventBuffer {
		_, err := irWriter.Write(event)
		if err != nil {
			err = fmt.Errorf("failed to encode event %v into ir: %w", event, err)
			return err
		}
	}
	return nil
}

func (w *IrZstdWriter) FlushIrStore() (error) {

	if (w.IrStore == nil) || (w.ZstdStore == nil) {
		return fmt.Errorf("error flush called with non-existant store")
	}

	_, err := io.Copy(w.ZstdWriter, w.IrStore)
	if err != nil {
		return err
	}

	// Terminate Zstd frame.
	err = w.ZstdWriter.Close()
	if err != nil {
		return err
	}

	// Re-initialize Zstd writer to recieve more input.
	w.ZstdWriter.Reset(w.ZstdStore)

	irFile, ok := w.IrStore.(*os.File)
	if !ok {
		return fmt.Errorf("error type assertion from store to file failed")
	}

	// Reset Ir Store.
	err = irFile.Truncate(0)
	if err != nil {
		return err
	}
	w.IrTotalBytes = 0

	return nil
}

func NewStores(store bool,storeDir string,tagkey string,) (io.ReadWriter,io.ReadWriter, error) {
	var irStore io.ReadWriter
	var zstdStore io.ReadWriter

	if store {
		// Create file to store ir to disk.
		irStoreName := fmt.Sprintf("%s.ir", tagkey)
		irStoreDir := filepath.Join(storeDir, IrDir)
		irFile, err := CreateFile(irStoreDir, irStoreName)
		if err != nil {
			return nil, nil, fmt.Errorf("error creating file: %w", err)
		}
		log.Printf("created file %s", irFile.Name())
		irStore = irFile

		// Create file to store zstd to disk.
		zstdStoreName := fmt.Sprintf("%s.zst", tagkey)
		zstdStoreDir := filepath.Join(storeDir, ZstdDir)
		zstdFile, err := CreateFile(zstdStoreDir, zstdStoreName)
		if err != nil {
			return nil, nil, fmt.Errorf("error creating file: %w", err)
		}
		log.Printf("created file %s", zstdFile.Name())
		zstdStore = zstdFile

	} else {
		// Store zstd directly in memory. No ir file needed since ir is immediately compressed.
		irStore = nil
		var membuf bytes.Buffer
		zstdStore = &membuf
	}

	return irStore, zstdStore, nil
}

// Creates a new file to output IR. A new file is created for every Fluent Bit chunk.
// The system timestamp is added as a suffix.
//
// Parameters:
//   - path: Directory path to create to write files inside
//   - file: File name prefix
//
// Returns:
//   - f: The created file
//   - err: Could not create directory, could not create file
func CreateFile(path string, file string) (*os.File, error) {
	// Make directory if does not exist.
	err := os.MkdirAll(path, 0o751)
	if err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", path, err)
		return nil, err
	}

	fullFilePath := filepath.Join(path, file)

	// Try to open the file exclusively. If it already exists something has gone wrong. Even if
	// program crashed should have been deleted on startup.
	f, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o751)
	if err != nil {
		// Check if the error is due to the file already existing
		if errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("file %s already exists", fullFilePath)
		}
		return nil, fmt.Errorf("failed to create file %s: %w", fullFilePath, err)
	}
	return f, nil
}



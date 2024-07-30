// Package provides ability to recover disk stores on startup and send to s3.

package recovery

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/y-scope/fluent-bit-clp/internal/irzstd"
	"github.com/y-scope/fluent-bit-clp/internal/outctx"
	"github.com/y-scope/fluent-bit-clp/plugins/out_clp_s3/flush"
)

// If useDiskBuffer is set, close all files prior to exit. Graceful exit will only be called
// if Fluent Bit recieves a kill signal and not during an abrupt crash. Plugin is only
// given a limited time to clean up resources, so stores are not sent to s3. Instead
// they are sent during startup.
//
// Parameters:
//   - ctx: Plugin context
//
// Returns:
//   - err: Error closing file
func GracefulExit(ctx *outctx.S3Context) error {
	if !ctx.Config.UseDiskBuffer {
		return nil
	}

	for _, tag := range ctx.Tags {
		err := closeBufferFiles(tag.Writer)
		if err != nil {
			return err
		}
	}

	return nil
}

// Sends existing disk buffers to S3.
//
// Parameters:
//   - ctx: Plugin context
//
// Returns:
//   - err: Error retrieving files, error files not valid, error flushing existing buffer
//
// error sending to s3
func InitDiskBuffers(ctx *outctx.S3Context) error {
	irFiles, zstdFiles, err := getBufferFiles(ctx)
	if err != nil {
		return err
	}

	err = checkFilesValid(irFiles, zstdFiles)
	if err != nil {
		return err
	}

	for tagKey, irFileInfo := range irFiles {
		err := flushExistingBuffer(tagKey, irFileInfo, zstdFiles, ctx)
		if err != nil {
			return fmt.Errorf("error flushing existing buffer '%s': %w", tagKey, err)
		}
	}

	return nil
}

// Retrieves IR and Zstd disk buffer file data. For both IR and Zstd directories,
// returns map with FluentBit tag as keys and FileInfo as values.
//
// Parameters:
//   - ctx: Plugin context
//
// Returns:
//   - irFiles: IR file map
//   - ZstdFiles: Zstd file map
//   - err: Error reading directory
func getBufferFiles(
	ctx *outctx.S3Context,
) (map[string]os.FileInfo, map[string]os.FileInfo, error) {
	irBufferPath, zstdBufferPath := getBufferPaths(ctx)
	irFiles, err := readDirectory(irBufferPath)
	if err != nil {
		return nil, nil, err
	}

	zstdFiles, err := readDirectory(zstdBufferPath)
	if err != nil {
		return nil, nil, err
	}

	return irFiles, zstdFiles, nil
}

// Reads directory and returns map containing FileInfo for each file.
//
// Parameters:
//   - dir: Path of disk buffer directory
//
// Returns:
//   - files: Map with Fluent Bit tag as keys and FileInfo as values
//   - err: Error reading directory, error retrieving FileInfo, error directory contains irregular
// files
func readDirectory(dir string) (map[string]os.FileInfo, error) {
	files := make(map[string]os.FileInfo)

	dirEntries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		log.Printf("Recovered storage directory %s not found during startup", dir)
		return files, nil
	} else if err != nil {
		return nil, fmt.Errorf("error reading directory '%s': %w", dir, err)
	}

	for _, dirEntry := range dirEntries {
		fileInfo, err := getFileInfo(dirEntry)
		if err != nil {
			return nil, err
		}
		tagKey := strings.TrimSuffix(fileInfo.Name(), filepath.Ext(fileInfo.Name()))
		files[tagKey] = fileInfo
	}

	return files, nil
}

// Gets fileInfo.
//
// Parameters:
//   - dirEntry: Directory entry
//
// Returns:
//   - fileInfo: FileInfo for the directory entry
//   - err: Error retrieving fileInfo, error file is not a regular file
func getFileInfo(dirEntry fs.DirEntry) (os.FileInfo, error) {
	fileName := dirEntry.Name()
	fileInfo, err := dirEntry.Info()
	if err != nil {
		return nil, fmt.Errorf("error retrieving FileInfo for '%s': %w", fileName, err)
	}

	if !fileInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("error %s is not a regular file: %w", fileName, err)
	}

	return fileInfo, nil
}

// Checks if files in diskBufferDir are valid. After assertions, irFiles and zstdFiles are the same
// length and have the same keys.
//
// Parameters:
//   - irFiles: Map with Fluent Bit tag as keys and IR Buffer FileInfo as values.
//   - zstdFiles:  Map with Fluent Bit tag as keys and Zstd Buffer FileInfo as values.
//
// Returns:
//   - err: Error files do not match
func checkFilesValid(irFiles map[string]fs.FileInfo, zstdFiles map[string]fs.FileInfo) error {
	if len(irFiles) != len(zstdFiles) {
		return fmt.Errorf("error files in IR and Zstd store do not match")
	}

	for tagKey := range irFiles {
		if _, ok := zstdFiles[tagKey]; !ok {
			return fmt.Errorf("error files in IR and zstd store do not match")
		}
	}

	return nil
}

// Flushes existing disk buffer to s3 on startup. Prior to sending, opens disk buffer files and
// creates new [outctx.Tag] with existing buffer files. Removes IR preamble for new [outctx.Tag],
// as existing stores should already have there own IR preamble.
//
// Parameters:
//   - tagKey: Fluent Bit tag
//   - irFileInfo: FileInfo for IR disk buffer file.
//   - zstdFiles: Map with Fluent Bit tag as keys and Zstd Buffer FileInfo as values.
//   - ctx: Plugin context
//
// Returns:
//   - err: error files do not match
func flushExistingBuffer(
	tagKey string,
	irFileInfo fs.FileInfo,
	zstdFiles map[string]fs.FileInfo,
	ctx *outctx.S3Context,
) error {
	// Don't need to check ok return value since we already checked if key exists.
	zstdFileInfo := zstdFiles[tagKey]

	irBufferPath, zstdBufferPath := getBufferPaths(ctx)
	irPath := filepath.Join(irBufferPath, irFileInfo.Name())
	zstdPath := filepath.Join(zstdBufferPath, zstdFileInfo.Name())

	irStoreSize := irFileInfo.Size()
	zstdStoreSize := zstdFileInfo.Size()

	if (irStoreSize == 0) && (zstdStoreSize == 0) {
		err := removeBufferFiles(irPath, zstdPath)
		// If both files are empty, and there is no error, it will skip tag. Creating unneccesary
		// tag is wasteful. Also prevents accumulation of old tags no longer being sent by Fluent
		// Bit.
		return err
	}

	irFile, zstdFile, err := openBufferFiles(irPath, zstdPath)
	if err != nil {
		return fmt.Errorf("error opening files: %w", err)
	}

	tag, err := flush.NewTag(
		tagKey,
		ctx.Config.TimeZone,
		int(irStoreSize),
		ctx.Config.UseDiskBuffer,
		irFile,
		zstdFile,
	)
	if err != nil {
		return fmt.Errorf("error creating tag: %w", err)
	}

	err = tag.Writer.ResetIrWriter()
	if err != nil {
		return fmt.Errorf("error removing IR preamble: %w", err)
	}

	ctx.Tags[tagKey] = tag

	log.Printf("Recovered stores for tag %s", tagKey)

	err = flush.FlushZstdToS3(tag, ctx)
	if err != nil {
		return fmt.Errorf("error flushing Zstd store to s3: %w", err)
	}

	return nil
}

// Removes IR and Zstd disk buffer files.
//
// Parameters:
//   - irPath: Path to IR disk buffer file
//   - zstdPath:Path to Zstd disk buffer file
//
// Returns:
//   - err: error files do not match
func removeBufferFiles(irPath string, zstdPath string) error {
	err := os.Remove(irPath)
	if err != nil {
		return fmt.Errorf("error deleting file '%s': %w", irPath, err)
	}
	err = os.Remove(zstdPath)
	if err != nil {
		return fmt.Errorf("error deleting file '%s': %w", zstdPath, err)
	}
	return nil
}

// Opens IR and Zstd disk buffer files.
//
// Parameters:
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
//
// Returns:
//   - err: error files do not match
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

// Retrieves paths for IR and Zstd disk buffer directories.
//
// Parameters:
//   - ctx: Plugin context
//
// Returns:
//   - irBufferPath: Path of IR disk buffer directory
//   - zstdBufferPath: Path of Zstd disk buffer directory
func getBufferPaths(ctx *outctx.S3Context) (string, string) {
	irBufferPath := filepath.Join(ctx.Config.DiskBufferPath, flush.IrDir)
	zstdBufferPath := filepath.Join(ctx.Config.DiskBufferPath, flush.ZstdDir)
	return irBufferPath, zstdBufferPath
}

// Closes IR and Zstd disk buffer files.
//
// Returns:
//   - err: Error with type assertion, error closing file
func closeBufferFiles(writer *irzstd.Writer) error {
	irFile, ok := writer.GetIrBuffer().(*os.File)
	if !ok {
		return fmt.Errorf("error type assertion from store to file failed")
	}

	irFileName := irFile.Name()
	err := irFile.Close()
	if err != nil {
		return fmt.Errorf("error could not close file %s: %w", irFileName, err)
	}

	zstdFile, ok := writer.GetZstdBuffer().(*os.File)
	if !ok {
		return fmt.Errorf("error type assertion from store to file failed")
	}

	zstdFileName := zstdFile.Name()
	err = zstdFile.Close()
	if err != nil {
		return fmt.Errorf("error could not close file %s: %w", zstdFileName, err)
	}
	return nil
}

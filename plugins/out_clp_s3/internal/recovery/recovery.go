// Package provides ability to recover disk buffer on startup and send to s3.

package recovery

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/y-scope/fluent-bit-clp/internal/outctx"
)

// If useDiskBuffer is set, close all files prior to exit. Graceful exit will only be called
// if Fluent Bit receives a kill signal and not during an abrupt crash. Plugin is only
// given a limited time to clean up resources, so output is not sent to s3. Instead
// they are sent during startup.
//
// Parameters:
//   - ctx: Plugin context
//
// Returns:
//   - err: Error closing file
func GracefulExit(ctx *outctx.S3Context) error {
	for _, eventManager := range ctx.EventManagers {
		err := eventManager.Writer.Close()
		if err != nil {
			return err
		}
		eventManager.Writer = nil
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
func RecoverBufferFiles(ctx *outctx.S3Context) error {
	irFiles, zstdFiles, err := getBufferFiles(ctx)
	if err != nil {
		return err
	}

	err = checkFilesValid(irFiles, zstdFiles)
	if err != nil {
		return err
	}

	for tag, irFileInfo := range irFiles {
		// Don't need to check ok return value since we already checked if key exists.
		zstdFileInfo := zstdFiles[tag]
		err := flushExistingBuffer(tag, irFileInfo, zstdFileInfo, ctx)
		if err != nil {
			return fmt.Errorf("error flushing existing buffer '%s': %w", tag, err)
		}
	}

	return nil
}

// Retrieves FileInfo for every file in IR and Zstd disk buffer directories. For both IR and Zstd
// directories, returns map with FluentBit tag as keys and FileInfo as values.
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
	irBufferPath, zstdBufferPath := ctx.GetBufferPaths()
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
//   - files: Map with FileInfo for all files in buffer directory. Fluent Bit tag is map key.
//   - err: Error reading directory, error retrieving FileInfo, error duplicate file
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
		tag := strings.TrimSuffix(fileInfo.Name(), filepath.Ext(fileInfo.Name()))

		if _, exists := files[tag]; exists {
			return nil, fmt.Errorf("error duplicate tag %s", tag)
		}
		files[tag] = fileInfo
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
//   - irFiles: Map with FileInfo for all files in IR buffer directory. Fluent Bit tag is map key.
//   - zstdFiles: Map with FileInfo for all files in Zstd buffer directory. Fluent Bit tag is map
//     key.
//
// Returns:
//   - err: Error files do not match
func checkFilesValid(irFiles map[string]fs.FileInfo, zstdFiles map[string]fs.FileInfo) error {
	if len(irFiles) != len(zstdFiles) {
		return fmt.Errorf("error files in IR and Zstd buffer directory do not match")
	}

	for tag := range irFiles {
		if _, ok := zstdFiles[tag]; !ok {
			return fmt.Errorf("error files in IR and Zstd buffer directory do not match")
		}
	}

	return nil
}

// Flushes existing disk buffer to s3 on startup. Prior to sending, opens disk buffer files and
// creates new [outctx.EventManager] using existing buffer files.
//
// Parameters:
//   - tag: Fluent Bit tag
//   - irFileInfo: FileInfo for IR disk buffer file
//   - zstdFileInfo: FileInfo for Zstd disk buffer file
//   - ctx: Plugin context
//
// Returns:
//   - err: error removing/open files, error creating event manager, error flushing to s3
func flushExistingBuffer(
	tag string,
	irFileInfo fs.FileInfo,
	zstdFileInfo fs.FileInfo,
	ctx *outctx.S3Context,
) error {
	irPath, zstdPath := ctx.GetBufferFilePaths(tag)

	irFileSize := irFileInfo.Size()
	zstdFileSize := zstdFileInfo.Size()

	if (irFileSize == 0) && (zstdFileSize == 0) {
		err := removeBufferFiles(irPath, zstdPath)
		// If both files are empty, and there is no error, it will skip tag. Creating unnecessary
		// event manager is wasteful. Also prevents accumulation of event mangers with tags no
		// longer being sent by Fluent Bit.
		return err
	}

	eventManager, err := ctx.RecoverEventManager(
		tag,
		int(irFileSize),
	)
	if err != nil {
		return fmt.Errorf("error recovering event manager with tag: %w", err)
	}

	log.Printf("Recovered disk buffers with tag %s", tag)

	err = eventManager.ToS3(ctx.Config, ctx.Uploader)
	if err != nil {
		return fmt.Errorf("error flushing Zstd to s3: %w", err)
	}

	return nil
}

// Removes IR and Zstd disk buffer files.
//
// Parameters:
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
//
// Returns:
//   - err: error removing files
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

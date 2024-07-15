// Package provides ability to recover disk stores on startup and send to s3.

package recovery

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/y-scope/fluent-bit-clp/internal/outctx"
	"github.com/y-scope/fluent-bit-clp/plugins/out_clp_s3/flush"
)

// If disk store is on, close all files prior to exit. Graceful exit will only be called
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

	if !ctx.Config.DiskStore {
		return nil
	}

	for _, tag := range ctx.Tags {
		err := closeStore(tag.Writer.IrStore)
		if err != nil {
			return err
		}

		err = closeStore(tag.Writer.ZstdStore)
		if err != nil {
			return err
		}
	}

	return nil
}

// If disk store is on, sends existing disk stores to S3.
//
// Parameters:
//   - ctx: Plugin context
//
// Returns:
//   - err: Error retrieving stores, error deleting empty stores, error getting size of IR store,
// error creating tag, error sending to s3
func FlushStores(ctx *outctx.S3Context) error {

	if !ctx.Config.DiskStore {
		return nil
	}

	irStoreDir := filepath.Join(ctx.Config.StoreDir, flush.IrDir)
	zstdStoreDir := filepath.Join(ctx.Config.StoreDir, flush.ZstdDir)

	irFiles, err := getFiles(irStoreDir)
	if err != nil {
		return err
	}

	zstdFiles, err := getFiles(zstdStoreDir)
	if err != nil {
		return err
	}

	// No files in recovery store so can start Fluent Bit without doing any recovery.
	if (irFiles == nil) && (zstdFiles == nil) {
		return nil
	}

	// Should contain equal amount of files. Also checks if one directory is missing since length
	// of files will be zero.
	if len(irFiles) != len(zstdFiles) {
		return fmt.Errorf("error files in IR and Zstd store do not match")
	}

	// Check if keys match.
	for fileName := range irFiles {
		if _, ok := zstdFiles[fileName]; !ok  {
			return fmt.Errorf("error files in IR and zstd store do not match")
		}
	}

	// After assertions, irFiles and Zstdfiles are the same length and have the same keys.
	for fileName, fileInfo := range irFiles {

		irPath := filepath.Join(irStoreDir, fileName)
		zstdPath := filepath.Join(zstdStoreDir, fileName)

		irStoreSize := fileInfo.Size()
		// Don't need to check _,ok return value since we already checked if key exists.
		zstdStoreSize := zstdFiles[fileName].Size()

		if (irStoreSize == 0) && (zstdStoreSize == 0) {
			err := os.Remove(irPath)
			if err != nil {
				return fmt.Errorf("error deleting file '%s': %w", fileName, err)
			}
			err = os.Remove(zstdPath)
			if err != nil {
				return fmt.Errorf("error deleting file '%s': %w", fileName, err)
			}
			// If both files are empty creating tag is wasteful. Also prevents accumulation of old
			// tags no longer being sent by Fluent Bit.
			continue
		}

		irStore, err := os.Open(irPath)
		if err != nil {
			return fmt.Errorf("error opening ir file %s: %w",irPath, err)
		}

		zstdStore, err := os.Open(zstdPath)
		if err != nil {
			return fmt.Errorf("error opening ir file %s: %w",zstdPath, err)
		}

		tagKey := fileName

		tag, err := flush.NewTag(tagKey,ctx.Config.TimeZone, int(irStoreSize),ctx.Config.DiskStore, irStore, zstdStore)
		if err != nil {
			return  fmt.Errorf("error creating tag: %w", err)
		}

		// Set size of IR store. Can avoid unnecessary flush of IR store if it is empty.
		tag.Writer.IrTotalBytes = int(irStoreSize)

		ctx.Tags[tagKey] = tag

		err = flush.FlushZstdToS3(tag, ctx)
		if err != nil {
			return fmt.Errorf("error flushing Zstd Store to s3: %w", err)
		}
	}
	return nil
}

// Reads directory and returns a map containg file data. Returns nil map if directory does not
// exist.
//
// Parameters:
//   - dir: Path of store directory
//
// Returns:
//   - files: map with file names as keys and [fs.FileInfo] as values. If directory does not exist, map is nil.
//   - err: Error reading directory, error retrieving FileInfo, error directory contains irregular files
func getFiles(dir string) (map[string]os.FileInfo, error) {

	dirEntry, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		log.Printf("Recovered storage directory %s not found during startup", dir)
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error reading directory '%s': %w", dir, err)
	}

	files := make(map[string]os.FileInfo)

	for _, file := range dirEntry {
		fileName := file.Name()
		fileInfo, err := file.Info()
		if err != nil {
			return nil, fmt.Errorf("error retrieving FileInfo for '%s': %w", fileName, err)
		}

		// Check if regular file (not directory, etc..)
		if !fileInfo.Mode().IsRegular() {
			return nil, fmt.Errorf("error %s is not a regular file: %w", fileName, err)
		}

		files[fileName] = fileInfo
	}
	return files, nil
}

// Closes file associated with store.
//
// Parameters:
//   - store: Disk store
//
// Returns:
//   - err: Error with type assertion, error closing file
func closeStore(store io.ReadWriter) error {

	file, ok := store.(*os.File)
	if !ok {
		return fmt.Errorf("error type assertion from store to file failed")
	}

	fileName := file.Name()
	err := file.Close()
	if err != nil {
		return fmt.Errorf("error could not close file %s: %w", fileName, err)
	}

	return nil
}
package flush

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/y-scope/fluent-bit-clp/internal/outctx"
	"github.com/y-scope/fluent-bit-clp/internal/irzstd"
)

// perhaps safer not to send to s3.
// will terminate all IR streams
func GracefulExit(ctx *outctx.S3Context) error {
	for _, tag := range ctx.Tags {

		//maybe flip this

		if ctx.Config.Store {
			irFile, ok := tag.IrStore.(*os.File)
			if !ok {
				return fmt.Errorf("error type assertion from store to file failed")
			}
			irFileName := irFile.Name()
			err := irFile.Close()
			if err != nil {
				return fmt.Errorf("error could not close file %s: %w", irFileName, err)
			}

			zstdFile, ok := tag.ZstdStore.(*os.File)
			if !ok {
				return fmt.Errorf("error type assertion from store to file failed")
			}
			zstdFileName := zstdFile.Name()
			err = zstdFile.Close()
			if err != nil {
				return fmt.Errorf("error could not close file %s: %w", zstdFileName, err)
			}
		}
	}
	return nil
}

func RecoverOnStart(ctx *outctx.S3Context) error {
	irStoreDir := filepath.Join(ctx.Config.StoreDir, IrDir)
	zstdStoreDir := filepath.Join(ctx.Config.StoreDir, ZstdDir)

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
		return fmt.Errorf("error files in IR and zstd store do not match")
	}

	for fileName, _ := range irFiles {
		if _, ok := zstdFiles[fileName]; !ok  {
			return fmt.Errorf("error files in IR and zstd store do not match")
		}
	}

	// After error checking, irFiles and Zstdfiles are the same length and have the same keys.
	for fileName, fileInfo := range irFiles {

		irPath := filepath.Join(irStoreDir, fileName)
		zstdPath := filepath.Join(zstdStoreDir, fileName)

		irStoreSize := fileInfo.Size()
		// Safe since we already checked if key exists.
		zstdStoreSize := zstdFiles[fileName].Size()

		if (irStoreSize == 0) && (zstdStoreSize) == 0 {
			err := os.Remove(irPath)
			if err != nil {
				return fmt.Errorf("error deleting file '%s': %w", fileName, err)
			}
			err = os.Remove(zstdPath)
			if err != nil {
				return fmt.Errorf("error deleting file '%s': %w", fileName, err)
			}
			// If both files are empty creating tag is wasteful. Also prevents accumulation of old tags.
			continue
		}

		irStore, err := os.Open(irPath)
		if err != nil {
			return fmt.Errorf("error opening ir file %s: %w",irPath, err)
		}

		irSize, err := irzstd.GetDiskStoreSize(irStore)

		zstdStore, err := os.Open(zstdPath)
		if err != nil {
			return fmt.Errorf("error opening ir file %s: %w",zstdPath, err)
		}

		tagKey := fileName

		tag, err := newTag(tagKey,ctx.Config.TimeZone,int(irStoreSize),ctx.Config.DiskStore, irStore, zstdStore)
		if err != nil {
			return  fmt.Errorf("error creating tag: %w", err)
		}

		// Can avoid unnecesary Flush of IR store if it is empty.
		tag.Writer.IrTotalBytes = irSize

		ctx.Tags[tagKey] = tag

		err = FlushZstdToS3(tag,ctx)
		if err != nil {
			return fmt.Errorf("error flushing Zstd Store to s3: %w", err)
		}
	}
	return nil
}

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
		if fileInfo.Mode().IsRegular() == false {
			return nil, fmt.Errorf("error %s is not a regular file: %w", fileName, err)
		}

		files[fileName] = fileInfo
	}
	return files, nil
}
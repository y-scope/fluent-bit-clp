// Package implements methods to send data to output. All data provided by Fluent Bit is encoded
// with Msgpack.

package flush

import (
	"C"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path/filepath"
	"time"
	"unsafe"

	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/fluent/fluent-bit-go/output"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"

	"github.com/y-scope/fluent-bit-clp/internal/decoder"
	"github.com/y-scope/fluent-bit-clp/internal/irzstd"
	"github.com/y-scope/fluent-bit-clp/internal/outctx"
)

// Tag key when tagging s3 objects with Fluent Bit tag.
const s3TagKey = "fluentBitTag"

// Names of disk store directories.
const IrDir = "ir"
const ZstdDir = "zstd"

// Flushes data to s3 in IR format. Decode of Msgpack based on [Fluent Bit reference].
//
// Parameters:
//   - data: Msgpack data
//   - length: Byte length
//   - tag: Fluent Bit tag
//   - S3Config: Plugin configuration
//
// Returns:
//   - code: Fluent Bit success code (OK, RETRY, ERROR)
//   - err: Error if flush fails
//
// [Fluent Bit reference]: https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
func ToS3(data unsafe.Pointer, length int, tagKey string, ctx *outctx.S3Context) (int, error) {
	// Buffer to store events from Fluent Bit chunk.
	var logEvents []ffi.LogEvent

	dec := decoder.New(data, length)

	// Loop through all records in Fluent Bit chunk.
	for {
		ts, record, err := decoder.GetRecord(dec)
		if err == io.EOF {
			// Chunk decoding finished. Break out of loop and send log events to output.
			break
		} else if err != nil {
			err = fmt.Errorf("error decoding data from stream: %w", err)
			return output.FLB_ERROR, err
		}

		timestamp := decodeTs(ts)
		msg, err := getMessage(record, ctx.Config)
		if err != nil {
			err = fmt.Errorf("failed to get message from record: %w", err)
			return output.FLB_ERROR, err
		}

		event := ffi.LogEvent{
			LogMessage: msg,
			Timestamp:  ffi.EpochTimeMs(timestamp.UnixMilli()),
		}
		logEvents = append(logEvents, event)
	}

	var err error
	tag, ok := ctx.Tags[tagKey]

	// If tag has not been encountered before, create a new tag.
	if !ok {
		irStore, zstdStore, err := irzstd.NewStores(ctx.Config.DiskStore,ctx.Config.StoreDir,tagKey)
		if err != nil {
			return output.FLB_RETRY, fmt.Errorf("error creating stores: %w", err)
		}

		tag, err = newTag(tagKey,ctx.Config.TimeZone,length,ctx.Config.DiskStore, irStore, zstdStore)
		if err != nil {
			return output.FLB_RETRY, fmt.Errorf("error creating tag: %w", err)
		}
		ctx.Tags[tagKey] = tag
	}

	err = tag.Writer.WriteIrZstd(logEvents)
	if err != nil {
		return output.FLB_ERROR, err
	}

	readyToUpload, err := checkUploadCriteria(tag,ctx.Config.DiskStore, ctx.Config.UploadSizeMb,ctx.Config.Timeout)
	if err != nil {
		return output.FLB_ERROR, fmt.Errorf("error checking upload criteria: %w", err)
	}

	if readyToUpload {
		err := FlushZstdToS3(tag,ctx)
		if err != nil {
			return output.FLB_ERROR, fmt.Errorf("error flushing Zstd store to s3: %w", err)
		}
	}

	return output.FLB_OK, nil
}

// Decodes timestamp provided by Fluent Bit engine into time.Time. If timestamp cannot be
// decoded, returns system time.
//
// Parameters:
//   - ts: Timestamp provided by Fluent Bit
//
// Returns:
//   - timestamp: time.Time timestamp
func decodeTs(ts interface{}) time.Time {
	var timestamp time.Time
	switch t := ts.(type) {
	case decoder.FlbTime:
		timestamp = t.Time
	case uint64:
		timestamp = time.Unix(int64(t), 0)
	default:
		fmt.Printf("time provided invalid, defaulting to now. Invalid type is %T", t)
		timestamp = time.Now()
	}
	return timestamp
}

// Retrieves message from a record object. The message can consist of the entire object or
// just a single key. For a single key, user should set use_single_key to true in fluent-bit.conf.
// In addition user, should set single_key to "log" which is default Fluent Bit key for unparsed
// messages; however, single_key can be set to another value. To prevent failure if the key is
// missing, user can specify allow_missing_key, and behaviour will fallback to the entire object.
//
// Parameters:
//   - record: JSON record from Fluent Bit with variable amount of keys
//   - config: Configuration based on fluent-bit.conf
//
// Returns:
//   - msg: Retrieved message
//   - err: Key not found, json.Unmarshal error, string type assertion error
func getMessage(jsonRecord []byte, config outctx.S3Config) (string, error) {
	// If use_single_key=false, return the entire record.
	if !config.UseSingleKey {
		return string(jsonRecord), nil
	}

	// If use_single_key=true, then look for key in record, and set message to the key's value.
	var record map[string]interface{}
	err := json.Unmarshal(jsonRecord, &record)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal json record %v: %w", jsonRecord, err)
	}

	singleKeyMsg, ok := record[config.SingleKey]
	if !ok {
		// If key not found in record, see if allow_missing_key=true. If missing key is
		// allowed, then return entire record.
		if config.AllowMissingKey {
			return string(jsonRecord), nil
			// If key not found in record and allow_missing_key=false, then return an error.
		} else {
			return "", fmt.Errorf("key %s not found in record %v", config.SingleKey, record)
		}
	}

	stringMsg, ok := singleKeyMsg.(string)
	if !ok {
		return "", fmt.Errorf("string type assertion for message failed %v", singleKeyMsg)
	}

	return stringMsg, nil
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

// Uploads log events to s3.
//
// Parameters:
//   - bucket: S3 bucket
//   - bucketPrefix: Directory prefix in s3
//   - io: Chunk of compressed IR
//   - tag: Fluent Bit tag
//   - id: Id of output plugin
//   - uploader: AWS s3 upload manager
//
// Returns:
//   - err: error uploading, error unescaping string
func uploadToS3(
	bucket string,
	bucketPrefix string,
	io io.ReadWriter,
	tagKey string,
	index int,
	id string,
	uploader *manager.Uploader,
) (string, error) {
	currentTime := time.Now()
	// Format the time as a string in RFC3339 format.
	timeString := currentTime.Format(time.RFC3339)

	fileName := fmt.Sprintf("%s_%d_%s_%s.zst", tagKey, timeString, id)
	fullFilePath := filepath.Join(bucketPrefix, fileName)

	// Upload the file to S3.
	tag := fmt.Sprintf("%s=%s", s3TagKey, tagKey)
	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(fullFilePath),
		Body:    io,
		Tagging: &tag,
	})
	if err != nil {
		return "", err
	}

	// Result location is less readable when escaped.
	uploadLocation, err := url.QueryUnescape(result.Location)
	if err != nil {
		return "", err
	}

	return uploadLocation, nil
}

func newTag(tagKey string, timezone string, size int, store bool, irStore io.ReadWriter, zstdStore io.ReadWriter) (*outctx.Tag, error) {
	writer, err := irzstd.NewIrZstdWriter(timezone, size, store, irStore, zstdStore)
	if err != nil {
		return nil, err
	}

	tag := outctx.Tag{
		Key: tagKey,
		Start: time.Now(),
		Writer: writer,
	}

	return &tag, nil
}

func FlushZstdToS3(tag *outctx.Tag, ctx *outctx.S3Context) (error) {
		err := tag.Writer.Close()
		if err != nil {
			return fmt.Errorf("error closing irzstd stream: %w", err)
		}

		outputLocation, err := uploadToS3(
			ctx.Config.S3Bucket,
			ctx.Config.S3BucketPrefix,
			tag.Writer.ZstdStore,
			tag.Key,
			tag.Index,
			ctx.Config.Id,
			ctx.Uploader,
		)
		if err != nil {
			err = fmt.Errorf("failed to upload chunk to s3, %w", err)
			return err
		}

		tag.Index += 1
		tag.Start = time.Now()

		log.Printf("chunk uploaded to %s", outputLocation)
		tag.Writer.Reset()

		return nil
}

func checkUploadCriteria(tag *outctx.Tag, diskStore bool, uploadSizeMb int, timeout time.Duration) (bool, error) {
	if !diskStore {
		return true, nil
	}

	storeSize, err := irzstd.GetStoreSize(tag.Writer.ZstdStore)
	if err != nil {
		return false, fmt.Errorf("error could not get size of Zstd store: %w", err)
	}

	UploadSize := uploadSizeMb << 20
	bestBefore := tag.Start.Add(timeout)

	if (storeSize >= UploadSize) || time.Now().After(bestBefore) {
		return true, nil
	}

	return false, nil
}

// Compresses contents of IR store and outputs it to Zstd Store. IR store is then reset.
//
// Returns:
//   - err: Error called with non-existant store, error compressing to Zstd, error reseting stores
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
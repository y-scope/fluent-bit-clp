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
	"log"
	"net/url"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/fluent/fluent-bit-go/output"

	"github.com/ugorji/go/codec"

	"github.com/y-scope/clp-ffi-go/ffi"

	"github.com/y-scope/fluent-bit-clp/internal/decoder"
	"github.com/y-scope/fluent-bit-clp/internal/irzstd"
	"github.com/y-scope/fluent-bit-clp/internal/outctx"
)

// Tag key when tagging s3 objects with Fluent Bit tag.
const s3TagKey = "fluentBitTag"

// Names of disk store directories.
const (
	IrDir   = "ir"
	ZstdDir = "zstd"
)

// Flushes data to s3 in IR format. Data may be buffered in stores depending on plugin
// configuration.
//
// Parameters:
//   - data: Msgpack data
//   - size: Byte length
//   - tag: Fluent Bit tag
//   - ctx: Plugin context
//
// Returns:
//   - code: Fluent Bit success code (OK, RETRY, ERROR)
//   - err: Error if flush fails
func ToS3(data unsafe.Pointer, size int, tagKey string, ctx *outctx.S3Context) (int, error) {
	dec := decoder.New(data, size)
	logEvents, err := decodeMsgpack(dec, ctx.Config)
	if err != nil {
		return output.FLB_ERROR, err
	}

	tag, ok := ctx.Tags[tagKey]

	// If tag does not exist yet, create new stores and tag. If disk store is on, stores are
	// created on disk and are used to buffer Fluent Bit chunks. If disk store is off, store
	// is in memory and chunks are not buffered.
	if !ok {
		irStore, zstdStore, err := newStores(ctx.Config.DiskStore, ctx.Config.StoreDir, tagKey)
		if err != nil {
			return output.FLB_RETRY, fmt.Errorf("error creating stores: %w", err)
		}

		tag, err = NewTag(
			tagKey,
			ctx.Config.TimeZone,
			size,
			ctx.Config.DiskStore,
			irStore,
			zstdStore,
		)

		if err != nil {
			return output.FLB_RETRY, fmt.Errorf("error creating tag: %w", err)
		}
		ctx.Tags[tagKey] = tag
	}

	err = tag.Writer.WriteIrZstd(logEvents)
	if err != nil {
		return output.FLB_ERROR, err
	}

	readyToUpload, err := checkUploadCriteria(tag, ctx.Config.DiskStore, ctx.Config.UploadSizeMb)
	if err != nil {
		return output.FLB_ERROR, fmt.Errorf("error checking upload criteria: %w", err)
	}

	if readyToUpload {
		err := FlushZstdToS3(tag, ctx)
		if err != nil {
			return output.FLB_ERROR, fmt.Errorf("error flushing Zstd store to s3: %w", err)
		}
	}

	return output.FLB_OK, nil
}

// Creates a new tag containing a new [irzstd.IrZstdWriter].
//
// Parameters:
//   - tagKey: Fluent Bit tag
//   - timezone: Time zone of the log source
//   - size: Byte length
//   - diskStore: On/off for disk store
//   - irStore: Location to store IR
//   - ZstdStore: Location to store Zstd compressed IR
//
// Returns:
//   - tag: Tag resources and metadata
//   - err: Error creating new writer
func NewTag(
	tagKey string,
	timezone string,
	size int,
	diskStore bool,
	irStore io.ReadWriter,
	zstdStore io.ReadWriter,
) (*outctx.Tag, error) {
	writer, err := irzstd.NewIrZstdWriter(timezone, size, diskStore, irStore, zstdStore)
	if err != nil {
		return nil, err
	}

	tag := outctx.Tag{
		Key:    tagKey,
		Writer: writer,
	}

	return &tag, nil
}

// Sends Zstd store to s3 and reset writer and stores for future uploads. Prior to upload,
// IR store is flushed and IR/Zstd streams are terminated.
//
// Parameters:
//   - tag: Tag resources and metadata
//   - ctx: Plugin context
//
// Returns:
//   - err: Error creating closing writer, error with type assertion, error uploading to s3,
// error resetting writer
func FlushZstdToS3(tag *outctx.Tag, ctx *outctx.S3Context) error {
	// Flush IR store if exists, and terminate IR/Zstd streams.
	err := tag.Writer.Close()
	if err != nil {
		return fmt.Errorf("error closing irzstd stream: %w", err)
	}

	if ctx.Config.DiskStore {
		zstdFile, ok := tag.Writer.ZstdStore.(*os.File)
		if !ok {
			return fmt.Errorf("error type assertion from store to file failed")
		}
		// Seek to start of Zstd store.
		zstdFile.Seek(0, io.SeekStart)
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

	// Increment index
	tag.Index += 1

	log.Printf("chunk uploaded to %s", outputLocation)

	err = tag.Writer.Reset()
	if err != nil {
		return err
	}

	return nil
}

// Decodes Msgpack Fluent Bit chunk into slice of log events. Decode of Msgpack based on
// [Fluent Bit reference].
//
// Parameters:
//   - decoder: Msgpack decoder
//   - config: Plugin configuration
//
// Returns:
//   - logEvents: Slice of log events
//   - err: Error decoding Msgpack, error retrieving log message from decoded object
//
// [Fluent Bit reference]:
// https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
func decodeMsgpack(dec *codec.Decoder, config outctx.S3Config) ([]ffi.LogEvent, error) {
	// Buffer to store events from Fluent Bit chunk.
	var logEvents []ffi.LogEvent

	// Loop through all records in Fluent Bit chunk.
	for {
		ts, record, err := decoder.GetRecord(dec)
		if err == io.EOF {
			// Chunk decoding finished. Break out of loop and send log events to output.
			break
		} else if err != nil {
			err = fmt.Errorf("error decoding data from stream: %w", err)
			return nil, err
		}

		timestamp := decodeTs(ts)
		msg, err := getMessage(record, config)
		if err != nil {
			err = fmt.Errorf("failed to get message from record: %w", err)
			return nil, err
		}

		event := ffi.LogEvent{
			LogMessage: msg,
			Timestamp:  ffi.EpochTimeMs(timestamp.UnixMilli()),
		}
		logEvents = append(logEvents, event)
	}

	return logEvents, nil
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
//   - config: Plugin configuration
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

// Uploads log events to s3.
//
// Parameters:
//   - bucket: S3 bucket
//   - bucketPrefix: Directory prefix in s3
//   - io: Chunk of compressed IR
//   - tagKey: Fluent Bit tag
//   - id: Id of output plugin
//   - uploader: AWS s3 upload manager
//
// Returns:
//   - err: Error uploading, error unescaping string
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

	fileName := fmt.Sprintf("%s_%d_%s_%s.zst", tagKey, index, timeString, id)
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

// Checks if criteria are met to upload to s3. If disk store is off, then chunk is always uploaded
// and always returns true. If disk store is on, check if Zstd store size is greater than upload
// size.
//
// Parameters:
//   - tag: Tag resources and metadata
//   - diskStore: On/off for disk store
//   - uploadSizeMb: S3 upload size in MB
//
// Returns:
//   - readyToUpload: Boolean if upload criteria met or not
//   - err: Error getting Zstd store size
func checkUploadCriteria(tag *outctx.Tag, diskStore bool, uploadSizeMb int) (bool, error) {
	if !diskStore {
		return true, nil
	}

	storeSize, err := irzstd.GetDiskStoreSize(tag.Writer.ZstdStore)
	if err != nil {
		return false, fmt.Errorf("error could not get size of Zstd store: %w", err)
	}

	UploadSize := uploadSizeMb << 20

	if storeSize >= UploadSize {
		log.Printf(
			"Zstd store size of %d for tag %s exceeded upload size %d",
			storeSize,
			tag.Key,
			UploadSize,
		)
		return true, nil
	}

	return false, nil
}

// Creates stores to hold logs prior to sending to s3. If disk store is on, creates files for both
// IR and Zstd stores. If disk store is off, there is no IR store and Zstd store is a memory
// buffer. Store creation is seperate from tag creation, since during recovery the stores
// already exist.
//
// Parameters:
//   - diskStore: On/off for disk store
//   - storeDir: Directory for disk store files
//   - tagkey: Fluent Bit tag
//
// Returns:
//   - irStore: Location to store IR
//   - ZstdStore: Location to store Zstd compressed IR
//   - err: Error creating file
func newStores(
	diskStore bool,
	storeDir string,
	tagkey string,
) (io.ReadWriter, io.ReadWriter, error) {
	var irStore io.ReadWriter
	var zstdStore io.ReadWriter

	if !diskStore {
		// Store Zstd directly in memory. No IR Store needed since IR is immediately compressed.
		var membuf bytes.Buffer
		zstdStore = &membuf
		return nil, zstdStore, nil
	}

	// Create file for IR.
	irFileName := fmt.Sprintf("%s.ir", tagkey)
	irStoreDir := filepath.Join(storeDir, IrDir)
	irFile, err := createFile(irStoreDir, irFileName)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating file: %w", err)
	}
	log.Printf("created file %s", irFile.Name())
	irStore = irFile

	// Create file for Zstd.
	zstdFileName := fmt.Sprintf("%s.zst", tagkey)
	zstdStoreDir := filepath.Join(storeDir, ZstdDir)
	zstdFile, err := createFile(zstdStoreDir, zstdFileName)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating file: %w", err)
	}
	log.Printf("created file %s", zstdFile.Name())
	zstdStore = zstdFile

	return irStore, zstdStore, nil
}

// Creates a new file.
//
// Parameters:
//   - path: Directory path
//   - file: File name
//
// Returns:
//   - f: The created file
//   - err: Could not create directory, could not create file
func createFile(path string, file string) (*os.File, error) {
	// Make directory if does not exist.
	err := os.MkdirAll(path, 0o751)
	if err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", path, err)
		return nil, err
	}

	fullFilePath := filepath.Join(path, file)

	// Try to open the file exclusively. If it already exists something has gone wrong.
	f, err := os.OpenFile(fullFilePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o751)
	if err != nil {
		// Check if the error is due to the file already existing.
		if errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("file %s already exists", fullFilePath)
		}
		return nil, fmt.Errorf("failed to create file %s: %w", fullFilePath, err)
	}
	return f, nil
}

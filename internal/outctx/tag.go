package outctx

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/y-scope/fluent-bit-clp/internal/irzstd"
)

// Tag key when tagging s3 objects with Fluent Bit tag.
const s3TagKey = "fluentBitTag"

// Creates a new tag with a new [irzstd.Writer]. If UseDiskBuffer is set, buffers are created
// on disk and are used to buffer Fluent Bit chunks. If UseDiskBuffer is off, buffer is in memory
// and chunks are not buffered.
//
// Parameters:
//   - tagKey: Fluent Bit tag
//   - size: Byte length
//
// Returns:
//   - tag: Tag resources and metadata
//   - err: Error creating new writer
func (ctx *S3Context) newTag(
	tagKey string,
	size int,
) (*Tag, error) {
	var err error
	var writer *irzstd.Writer

	if ctx.Config.UseDiskBuffer {
		writer, err = irzstd.NewDiskWriter(
			ctx.Config.TimeZone,
			size,
			tagKey,
			ctx.Config.DiskBufferPath,
		)
	} else {
		writer, err = irzstd.NewMemWriter(ctx.Config.TimeZone, size, tagKey)
	}

	if err != nil {
		return nil, err
	}

	tag := Tag{
		Key:    tagKey,
		Writer: writer,
	}

	ctx.Tags[tagKey] = &tag

	return &tag, nil
}

// Recovers tag from previous execution using existing disk buffers.
//
// Parameters:
//   - tagKey: Fluent Bit tag
//   - size: Byte length
//
// Returns:
//   - tag: Tag resources and metadata
//   - err: Error creating new writer
func (ctx *S3Context) RecoverTag(
	tagKey string,
	size int,
) (*Tag, error) {
	writer, err := irzstd.RecoverWriter(
		ctx.Config.TimeZone,
		size,
		tagKey,
		ctx.Config.DiskBufferPath,
	)
	if err != nil {
		return nil, err
	}

	tag := Tag{
		Key:    tagKey,
		Writer: writer,
	}

	ctx.Tags[tagKey] = &tag

	return &tag, nil
}

// If tag exists, get the tag. If tag does not exist yet, create new tag.
//
// Parameters:
//   - tagKey: Fluent Bit tag
//   - size: Byte length
//
// Returns:
//   - err: Could not create buffers or tag
func (ctx *S3Context) GetTag(tagKey string, size int) (*Tag, error) {
	var err error
	tag, ok := ctx.Tags[tagKey]

	if !ok {
		tag, err = ctx.newTag(tagKey, size)
		if err != nil {
			return nil, err
		}
	}

	return tag, nil
}

// Sends Zstd buffer to s3 and reset writer and buffers for future uploads. Prior to upload,
// IR buffer is flushed and IR/Zstd streams are terminated. The tag's index is incremented on
// successful upload.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
//
// Returns:
//   - err: Error creating closing streams, error uploading to s3, error resetting writer
func (tag *Tag) ToS3(config S3Config, uploader *manager.Uploader) error {
	err := tag.Writer.CloseStreams()
	if err != nil {
		return fmt.Errorf("error closing irzstd stream: %w", err)
	}

	outputLocation, err := uploadToS3(
		config.S3Bucket,
		config.S3BucketPrefix,
		tag.Writer.GetZstdOutput(),
		tag.Key,
		tag.Index,
		config.Id,
		uploader,
	)
	if err != nil {
		err = fmt.Errorf("failed to upload chunk to s3, %w", err)
		return err
	}

	tag.Index += 1

	log.Printf("chunk uploaded to %s", outputLocation)

	err = tag.Writer.Reset()
	if err != nil {
		return err
	}

	return nil
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
	body io.Reader,
	tagKey string,
	index int,
	id string,
	uploader *manager.Uploader,
) (string, error) {
	currentTime := time.Now()
	timeString := currentTime.Format(time.RFC3339)

	fileName := fmt.Sprintf("%s_%d_%s_%s.zst", tagKey, index, timeString, id)
	fullFilePath := filepath.Join(bucketPrefix, fileName)

	tag := fmt.Sprintf("%s=%s", s3TagKey, tagKey)
	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(fullFilePath),
		Body:    body,
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

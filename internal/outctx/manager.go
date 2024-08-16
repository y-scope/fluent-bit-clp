package outctx

import (
	"context"
	"fmt"
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

// Resources and metadata to process Fluent Bit events with the same tag.
type EventManager struct {
	Tag    string
	Index  int
	Writer irzstd.Writer
}

// Sends Zstd buffer to s3 and reset writer and buffers for future uploads. Prior to upload,
// IR buffer is flushed and IR/Zstd streams are terminated. The [EventManager.Index] is incremented
// on successful upload.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
//
// Returns:
//   - err: Error creating closing streams, error uploading to s3, error resetting writer
func (m *EventManager) ToS3(config S3Config, uploader *manager.Uploader) error {
	err := m.Writer.CloseStreams()
	if err != nil {
		return fmt.Errorf("error closing irzstd stream: %w", err)
	}

	outputLocation, err := uploadToS3(
		config.S3Bucket,
		config.S3BucketPrefix,
		m,
		config.Id,
		uploader,
	)
	if err != nil {
		err = fmt.Errorf("failed to upload chunk to s3, %w", err)
		return err
	}

	m.Index += 1

	log.Printf("chunk uploaded to %s", outputLocation)

	err = m.Writer.Reset()
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
//   - eventManager: Manager for Fluent Bit events with the same tag
//   - id: Id of output plugin
//   - uploader: AWS s3 upload manager
//
// Returns:
//   - err: Error uploading, error unescaping string
func uploadToS3(
	bucket string,
	bucketPrefix string,
	eventManager *EventManager,
	id string,
	uploader *manager.Uploader,
) (string, error) {
	currentTime := time.Now()
	timeString := currentTime.Format(time.RFC3339)

	fileName := fmt.Sprintf("%s_%d_%s_%s.zst", eventManager.Tag, eventManager.Index, timeString, id)
	fullFilePath := filepath.Join(bucketPrefix, fileName)

	tag := fmt.Sprintf("%s=%s", s3TagKey, eventManager.Tag)
	result, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(fullFilePath),
		Body:    eventManager.Writer.GetZstdOutput(),
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

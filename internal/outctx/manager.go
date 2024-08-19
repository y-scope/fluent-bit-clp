package outctx

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/y-scope/fluent-bit-clp/internal/irzstd"
)

// Tag key when tagging s3 objects with Fluent Bit tag.
const s3TagKey = "fluentBitTag"

// Resources and metadata to process Fluent Bit events with the same tag.
type S3EventManager struct {
	Tag            string
	Index          int
	Writer         irzstd.Writer
	Listening      bool
	Mutex          sync.Mutex
	WaitGroup      sync.WaitGroup
	UploadRequests chan bool
}

// Starts upload listener which can receive signals on UploadRequests channel. Upload timeout
// is only active if use_disk_buffer is on. If UploadRequests channel is closed, the WaitGroup
// will decrement. WaitGroup allows graceful exit of listener goroutine when Fluent Bit receives
//
//	a kill signal. Without it, Fluent Bit may abruptly kill goroutines.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) Listen(config S3Config, uploader *manager.Uploader) {
	log.Printf("Starting upload listener for event manager with tag %s", m.Tag)
	defer m.WaitGroup.Done()

	m.Listening = true
	if m.Writer.GetUseDiskBuffer() {
		m.DiskUploadListener(config, uploader)
	} else {
		m.MemoryUploadListener(config, uploader)
	}
}

// Ends listener goroutine.
func (m *S3EventManager) StopListening() {
	log.Printf("Stopping upload listener for event manager with tag %s", m.Tag)

	// Closing the channel sends terminate signal to goroutine. The WaitGroup
	// will block until it actually terminates.
	close(m.UploadRequests)
	m.WaitGroup.Wait()
	m.Listening = false
}

// Immortal listener that uploads events to s3 when receives signal on UploadRequests channel or a
// timeout is hit. Listener will sleep when inactive.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) DiskUploadListener(config S3Config, uploader *manager.Uploader) {
	for {
		select {
		case _, more := <-m.UploadRequests:
			if !more {
				return
			}
		// Timeout will reset if signal sent on UploadRequest channel
		case <-time.After(config.Timeout):
		}

		err := m.Upload(config, uploader)
		if err != nil {
			log.Printf("Error uploading to s3 for event manager with tag %s", m.Tag)
		}
	}
}

// Immortal listener that uploads events to s3 when receives signal on UploadRequests channel.
// Listener will sleep when inactive.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) MemoryUploadListener(config S3Config, uploader *manager.Uploader) {
	for {
		_, more := <-m.UploadRequests
		if !more {
			return
		}

		err := m.Upload(config, uploader)
		if err != nil {
			log.Printf("Error uploading to s3 for event manager with tag %s", m.Tag)
		}
	}
}

// Uploads to s3 after acquiring lock and validating that buffer is not empty. Mutex prevents
// write while uploading. Must check that buffer is not empty as timeout can trigger on empty
// buffer and send empty file to s3. Empty buffer check is not explicitly necessary for
// MemoryUploadListener.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) Upload(config S3Config, uploader *manager.Uploader) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	empty, err := m.Writer.CheckEmpty()
	if err != nil {
		return fmt.Errorf("failed to check if buffer is empty, %w", err)
	}

	if empty {
		log.Printf("Did not uploads events with tag %s since buffer is empty", m.Tag)
		return nil
	}

	err = m.ToS3(config, uploader)
	return err
}

// Sends Zstd buffer to s3 and reset writer and buffers for future uploads. Prior to upload, IR
// buffer is flushed and IR/Zstd streams are terminated. The [S3EventManager.Index] is incremented
// on successful upload.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
//
// Returns:
//   - err: Error creating closing streams, error uploading to s3, error resetting writer
func (m *S3EventManager) ToS3(config S3Config, uploader *manager.Uploader) error {
	err := m.Writer.CloseStreams()
	if err != nil {
		panic(fmt.Errorf("error closing irzstd stream: %w", err))
	}

	outputLocation, err := s3Request(
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
		panic(fmt.Errorf("error resetting irzstd stream: %w", err))
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
func s3Request(
	bucket string,
	bucketPrefix string,
	eventManager *S3EventManager,
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

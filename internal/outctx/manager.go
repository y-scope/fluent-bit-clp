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
	"github.com/y-scope/clp-ffi-go/ffi"

	"github.com/y-scope/fluent-bit-clp/internal/irzstd"
)

// Tag key when tagging s3 objects with Fluent Bit tag.
const s3TagKey = "fluentBitTag"

// Resources and metadata to process Fluent Bit events with the same tag.
type S3EventManager struct {
	Tag       string
	Index     int
	Writer    irzstd.Writer
	WaitGroup sync.WaitGroup
	LogEvents chan []ffi.LogEvent
	Listening bool
}

// Starts the upload listener goroutine.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) StartListening(config S3Config, uploader *manager.Uploader) {
	log.Printf("Starting upload listener for event manager with tag %s", m.Tag)
	m.Listening = true
	m.WaitGroup.Add(1)
	go m.listen(config, uploader)
}

// Ends listener goroutine.
func (m *S3EventManager) StopListening() {
	if !m.Listening {
		return
	}

	log.Printf("Stopping upload listener for event manager with tag %s", m.Tag)

	// Closing the channel sends terminate signal to goroutine. The WaitGroup
	// will block until it actually terminates.
	close(m.LogEvents)
	m.WaitGroup.Wait()
	m.Listening = false
}

// ToS3 uploads events in the buffer to s3.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
//
// Returns:
//   - err: Error closing streams, error uploading, error resetting writer
func (m *S3EventManager) ToS3(config S3Config, uploader *manager.Uploader) error {
	return m.toS3(config, uploader)
}

// Starts upload listener which receives log events on LogEvents channel, writes them to the
// IR buffer, and triggers uploads when criteria are met or on timeout. This function should
// be called as a goroutine. Function runs an immortal loop which only exits if the LogEvents
// channel is closed. When function does exit, it decrements a WaitGroup letting the event
// manager know it has exited. WaitGroup allows graceful exit of listener when Fluent Bit
// receives a kill signal. Without WaitGroup, OS may abruptly kill listen goroutine.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) listen(config S3Config, uploader *manager.Uploader) {
	defer m.WaitGroup.Done()

	timer := time.NewTimer(config.Timeout)
	defer timer.Stop()

	for {
		timer.Reset(config.Timeout)
		select {
		case logEvents, more := <-m.LogEvents:
			if !more {
				return
			}
			log.Printf("Listener with tag %s received log events", m.Tag)
			numEvents, err := m.Writer.WriteIrZstd(logEvents)
			if err != nil {
				log.Printf(
					"Wrote %d out of %d total log events for tag %s: %v",
					numEvents,
					len(logEvents),
					m.Tag,
					err,
				)
				continue
			}
			uploadCriteriaMet, err := m.checkUploadCriteriaMet(config.UploadSizeMb)
			if err != nil {
				log.Printf("error checking upload criteria for tag %s: %v", m.Tag, err)
				continue
			}
			if uploadCriteriaMet {
				m.upload(config, uploader)
			}
		case <-timer.C:
			log.Printf("Timeout surpassed for listener with tag %s", m.Tag)
			m.upload(config, uploader)
		}
	}
}

// Uploads to s3 if the buffer is non-empty. Must check that buffer is not empty as timeout can
// trigger on empty buffer. Logs instead of returning error.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) upload(config S3Config, uploader *manager.Uploader) {
	empty, err := m.Writer.Empty()
	if err != nil {
		log.Printf("failed to check if buffer is empty for tag %s: %v", m.Tag, err)
		return
	}

	if empty {
		log.Printf("Did not upload events with tag %s since buffer is empty", m.Tag)
		return
	}

	if err := m.toS3(config, uploader); err != nil {
		log.Printf("listener upload failed: %v", err)
	}
}

// Checks whether Zstd buffer size is greater than or equal to upload size.
//
// Parameters:
//   - uploadSizeMb: S3 upload size in MB
//
// Returns:
//   - uploadCriteriaMet: Boolean if upload criteria met or not
//   - err: Error getting Zstd buffer size
func (m *S3EventManager) checkUploadCriteriaMet(uploadSizeMb int) (bool, error) {
	bufferSize, err := m.Writer.GetZstdOutputSize()
	if err != nil {
		return false, fmt.Errorf("error could not get size of buffer: %w", err)
	}

	uploadSize := uploadSizeMb << 20

	if bufferSize >= uploadSize {
		log.Printf(
			"Zstd buffer size of %d for tag %s exceeded upload size %d",
			bufferSize,
			m.Tag,
			uploadSize,
		)
		return true, nil
	}

	return false, nil
}

// toS3 sends Zstd buffer to s3 and resets writer and buffers for future uploads. Prior to upload,
// IR buffer is flushed and IR/Zstd streams are terminated. The [S3EventManager.Index] is
// incremented on successful upload.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
//
// Returns:
//   - err: Error closing streams, error uploading, error resetting writer
func (m *S3EventManager) toS3(config S3Config, uploader *manager.Uploader) error {
	err := m.Writer.CloseStreams()
	if err != nil {
		return fmt.Errorf("error closing irzstd stream for tag %s: %w", m.Tag, err)
	}

	outputLocation, err := s3Request(
		config.S3Bucket,
		config.S3BucketPrefix,
		m,
		config.Id,
		uploader,
	)
	if err != nil {
		return fmt.Errorf("S3 request failed for event manager with tag %s: %w", m.Tag, err)
	}

	m.Index += 1

	log.Printf("chunk uploaded to %s", outputLocation)

	err = m.Writer.Reset()
	if err != nil {
		return fmt.Errorf("error resetting irzstd stream for tag %s: %w", m.Tag, err)
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

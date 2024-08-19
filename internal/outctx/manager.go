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
	Mutex          sync.Mutex
	WaitGroup      sync.WaitGroup
	UploadRequests chan bool
	listening      bool
}

// Ends listener goroutine.
func (m *S3EventManager) StopListening() {
	log.Printf("Stopping upload listener for event manager with tag %s", m.Tag)

	// Closing the channel sends terminate signal to goroutine. The WaitGroup
	// will block until it actually terminates.
	close(m.UploadRequests)
	m.WaitGroup.Wait()
	m.listening = false
}

// Starts upload listener which can receive signals on UploadRequests channel. This function should
// be called as a goroutine. Timeout is only triggered if use_disk_buffer is on. Function calls
// immortal functions and thus will not exit. It will only exit if the uploadRequest channel is
// closed which will allow the callee to break out of infinite loop. When function does exit, it
// decrements a WaitGroup signaling that the goroutine has exited. WaitGroup allows graceful exit
// of listener when Fluent Bit receives a kill signal. On [recovery.GracefulExit], plugin will
// wait to exit until all listeners are closed. Without WaitGroup, OS may abruptly kill goroutine.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) listen(config S3Config, uploader *manager.Uploader) {
	defer m.WaitGroup.Done()

	m.listening = true
	if m.Writer.GetUseDiskBuffer() {
		m.diskUploadListener(config, uploader)
	} else {
		m.memoryUploadListener(config, uploader)
	}
}

// Immortal listener that uploads events to s3 when receives signal on UploadRequests channel or a
// timeout is hit. Listener will sleep when inactive.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) diskUploadListener(config S3Config, uploader *manager.Uploader) {
	for {
		select {
		case _, more := <-m.UploadRequests:
			log.Printf("Listener with tag %s received upload request on channel", m.Tag)
			// Exit if channel is closed
			if !more {
				return
			}
		// Timeout will reset if signal sent on UploadRequest channel
		case <-time.After(config.Timeout):
			log.Printf("Timeout surpassed for listener with tag %s", m.Tag)
		}

		m.upload(config, uploader)
	}
}

// Immortal listener that uploads events to s3 when receives signal on UploadRequests channel.
// Listener will sleep when inactive.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) memoryUploadListener(config S3Config, uploader *manager.Uploader) {
	for {
		_, more := <-m.UploadRequests
		log.Printf("Listener with tag %s received upload request on channel", m.Tag)
		// Exit if channel is closed
		if !more {
			return
		}

		m.upload(config, uploader)
	}
}

// Uploads to s3 after acquiring lock and validating that buffer is not empty. Mutex prevents
// write while uploading. Must check that buffer is not empty as timeout can trigger on empty
// buffer and send empty file to s3. Empty buffer check is not explicitly necessary for
// MemoryUploadListener. Panics instead of returning error.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) upload(config S3Config, uploader *manager.Uploader) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	empty, err := m.Writer.CheckEmpty()
	if err != nil {
		panic(fmt.Errorf("failed to check if buffer is empty, %w", err))
	}

	if empty {
		log.Printf("Did not uploads events with tag %s since buffer is empty", m.Tag)
		return
	}

	m.toS3(config, uploader)
}

// Sends Zstd buffer to s3 and reset writer and buffers for future uploads. Prior to upload, IR
// buffer is flushed and IR/Zstd streams are terminated. The [S3EventManager.Index] is incremented
// on successful upload. Logs errors with s3 request, otherwise panics instead on error. Errors
// closing and resetting writer are difficult to recover from.
//
// Parameters:
//   - config: Plugin configuration
//   - uploader: S3 uploader manager
func (m *S3EventManager) toS3(config S3Config, uploader *manager.Uploader) {
	if !m.Writer.GetClosed() {
		err := m.Writer.CloseStreams()
		if err != nil {
			panic(fmt.Errorf("error closing irzstd stream: %w", err))
		}
	}

	outputLocation, err := s3Request(
		config.S3Bucket,
		config.S3BucketPrefix,
		m,
		config.Id,
		uploader,
	)
	if err != nil {
		log.Print(fmt.Errorf("S3 request failed for event manager with tag %s: %w", m.Tag, err))
		return
	}

	m.Index += 1

	log.Printf("chunk uploaded to %s", outputLocation)

	err = m.Writer.Reset()
	if err != nil {
		panic(fmt.Errorf("error resetting irzstd stream: %w", err))
	}
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

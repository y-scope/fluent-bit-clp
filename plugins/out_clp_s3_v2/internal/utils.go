package internal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/klauspost/compress/zstd"
	"github.com/y-scope/clp-ffi-go/ir"
)

// GetOrCreateIngestionContext returns an existing IngestionContext for the given path,
// or creates and registers a new one.
func GetOrCreateIngestionContext(pluginCtx *PluginContext, path string) (*IngestionContext, error) {
	// Return existing ingestion pluginCtx if available
	if ingestionContext, exists := pluginCtx.Ingestion[path]; exists {
		return ingestionContext, nil
	}

	tempFile, err := os.CreateTemp(os.TempDir(), "clp-irv2-*.clp.zst")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	zstdWriter, err := zstd.NewWriter(tempFile)
	if err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
		return nil, fmt.Errorf("failed to create zstd writer: %w", err)
	}

	irWriter, err := ir.NewWriter[ir.FourByteEncoding](zstdWriter)
	if err != nil {
		_ = zstdWriter.Close()
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
		return nil, fmt.Errorf("failed to create IR writer: %w", err)
	}

	flushCtx := newFlushContext(pluginCtx, path, tempFile, zstdWriter)

	ingestionContext := &IngestionContext{
		Compression: &compressionContext{
			File:       tempFile,
			ZstdWriter: zstdWriter,
			IRWriter:   irWriter,
		},
		Flush: flushCtx,
	}

	pluginCtx.Ingestion[path] = ingestionContext
	return ingestionContext, nil
}

// S3CreateClient creates an AWS S3 client with credentials and endpoint configuration.
func S3CreateClient() (*s3.Client, error) {
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "us-west-1"
	}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		return nil, fmt.Errorf("could not load aws credentials: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // Required for MinIO!
		if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	return client, nil
}

// AWS error codes used for special error handling.
const (
	invalidCredsCode  = "InvalidClientTokenId"
	bucketMissingCode = "NotFound"
)

// S3ValidateLogBucket checks if the given bucket exists and that credential works.
func S3ValidateLogBucket(s3Client *s3.Client, logBucket string) error {
	_, err := s3Client.HeadBucket(
		context.TODO(),
		&s3.HeadBucketInput{Bucket: aws.String(logBucket)},
	)
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			switch code := ae.ErrorCode(); code {
			case invalidCredsCode:
				return fmt.Errorf("aws credentials are invalid: %w", err)
			case bucketMissingCode:
				return fmt.Errorf("bucket %q could not be found: %w", logBucket, err)
			default:
				return fmt.Errorf("aws error [%s]: %w", code, err)
			}
		}
		return err
	}
	return nil
}

// S3Upload uploads the specified local file to the given S3 bucket and path.
func S3Upload(s3Client *s3.Client, bucket, localPath, remotePath string) error {
	// #nosec G304 -- localPath is trusted
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file %q: %w", localPath, err)
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			log.Printf("[warn] Failed to close file %q: %v", localPath, cerr)
		}
	}()

	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(remotePath),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to upload %s to s3://%s/%s: %w",
			localPath, bucket, remotePath, err)
	}
	log.Printf("[info] Uploaded %s to s3://%s/%s", localPath, bucket, remotePath)
	return nil
}

func newFlushContext(
	pluginCtx *PluginContext,
	path string,
	tempFile *os.File,
	zstdWriter *zstd.Encoder,
) *flushContext {
	// Timers must be stopped and drained if not used,
	// but here we assume they're managed in flushContext logic.
	return &flushContext{
		HardTimer: time.NewTimer(0),
		SoftTimer: time.NewTimer(0),
		userCallback: func() {
			if err := zstdWriter.Flush(); err != nil {
				log.Printf("[error] zstdWriter.Flush failed: %v", err)
			}
			if err := S3Upload(pluginCtx.S3.Client, pluginCtx.S3.Bucket, tempFile.Name(),
				fmt.Sprintf("%s.clp.zst", path),
			); err != nil {
				log.Printf("[error] Failed to upload to S3: %v", err)
			}
		},
	}
}

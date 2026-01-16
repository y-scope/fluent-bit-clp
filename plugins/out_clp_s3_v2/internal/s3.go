// Package internal provides the core implementation for the out_clp_s3_v2 Fluent Bit plugin.
//
// This plugin implements a continuous sync strategy for uploading CLP-compressed logs to S3.
// Unlike batch upload approaches, this plugin continuously synchronizes compressed log files
// to S3, with sync frequency tunable based on log severity levels.
//
// # Architecture
//
// The plugin uses a dual-timer flush strategy:
//   - Hard timer: Absolute deadline - logs MUST be synced by this time
//   - Soft timer: Resets on each log event - syncs after a period of inactivity
//
// Higher severity logs (ERROR, FATAL) trigger faster sync of the entire log file,
// ensuring critical logs are available for investigation quickly.
//
// # Key Components
//
//   - PluginContext: Top-level context holding S3 client and ingestion contexts
//   - IngestionContext: Per-stream context with compression and flush management
//   - FlushConfigContext: Configuration for the dual-timer flush strategy
package internal

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Default AWS region when AWS_REGION environment variable is not set.
const defaultAWSRegion = "us-west-1"

// AWS API error codes for specific error handling.
const (
	// invalidCredsCode is returned when AWS credentials are invalid or expired.
	invalidCredsCode = "InvalidClientTokenId"
	// bucketMissingCode is returned when the specified S3 bucket does not exist.
	bucketMissingCode = "NotFound"
)

// S3CreateClient creates an AWS S3 client configured for the plugin.
//
// Configuration is loaded from the default AWS credential chain:
//   - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
//   - Shared credentials file (~/.aws/credentials)
//   - IAM role for Amazon EC2 or ECS
//
// The client is configured with path-style addressing to support S3-compatible
// services like MinIO. Custom endpoints can be specified via AWS_ENDPOINT_URL.
//
// Returns an error if AWS credentials cannot be loaded.
func S3CreateClient() (*s3.Client, error) {
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = defaultAWSRegion
	}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		return nil, fmt.Errorf("could not load aws credentials: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// Enable path-style addressing for S3-compatible services (MinIO, LocalStack, etc.)
		// AWS S3 supports both virtual-hosted and path-style, but custom endpoints
		// typically require path-style: http://endpoint/bucket/key vs http://bucket.endpoint/key
		o.UsePathStyle = true
		if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	return client, nil
}

// S3ValidateLogBucket verifies that the specified bucket exists and is accessible.
//
// This function performs a HEAD request on the bucket to validate:
//   - The bucket exists
//   - The configured credentials have permission to access it
//
// Returns a descriptive error for common failure cases:
//   - Invalid credentials
//   - Bucket not found
//   - Other AWS API errors
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

// S3Upload uploads a local file to S3.
//
// Parameters:
//   - s3Client: Configured S3 client
//   - bucket: Target S3 bucket name
//   - localPath: Path to the local file to upload
//   - remotePath: S3 object key (path within the bucket)
//
// The file is uploaded using a single PutObject request. For large files,
// consider using multipart upload (not implemented in this version).
func S3Upload(s3Client *s3.Client, bucket, localPath, remotePath string) error {
	// #nosec G304 -- localPath is from trusted internal temp file creation
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

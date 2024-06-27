// Package implements a context which is accessible by output plugin. The Fluent Bit engine stores a
// pointer to each context.

package outctx

// using outctx to prevent namespace collision with [context].
import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

// AWS error codes.
const (
	invalidCredsCode  = "InvalidClientTokenId"
	bucketMissingCode = "NotFound"
)

// Holds objects accessible to plugin during flush.
type S3Context struct {
	Config   S3Config
	Uploader *manager.Uploader
}

// Creates a new context. Loads configuration from user. Loads and tests aws credentials.
//
// Parameters:
//   - plugin: Fluent Bit plugin reference
//
// Returns:
//   - S3Context: Plugin context
//   - err: User configuration load failed, aws errors
func NewS3Context(plugin unsafe.Pointer) (*S3Context, error) {
	config, err := NewS3Config(plugin)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration %w", err)
	}

	// Load the aws credentials. [awsConfig.LoadDefaultConfig] will look for credentials in a
	// specfic hierarchy.
	// https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/
	awsConfig, err := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithRegion(config.S3Region),
	)
	if err != nil {
		return nil, fmt.Errorf("could not load aws credentials %w", err)
	}

	// Allows user to assume a provided role. Fluent Bit s3 plugin provides this feature.
	// In many cases, the EC2 instance will already have permission for the s3 bucket;
	// however, if it dosen't, this option allows the plugin to assume role with bucket access.
	if config.RoleArn != "" {
		stsClient := sts.NewFromConfig(awsConfig)
		creds := stscreds.NewAssumeRoleProvider(stsClient, config.RoleArn)
		awsConfig.Credentials = aws.NewCredentialsCache(creds)
	}

	s3Client := s3.NewFromConfig(awsConfig)

	// Confirm bucket exists and test aws credentials.
	_, err = s3Client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(config.S3Bucket),
	})
	if err != nil {
		// AWS does have some error types that can be checked with [error.As] such as
		// [s3.NotFound]. However, it can be difficult to always find the appropriate type. As a
		// result, using aws [smithy-go] to handle using error codes.
		// https://aws.github.io/aws-sdk-go-v2/docs/handling-errors/#api-error-responses
		var ae smithy.APIError
		if errors.As(err, &ae) {
			switch code := ae.ErrorCode(); code {
			case invalidCredsCode:
				err = fmt.Errorf("error aws credentials are invalid: %w", err)
			case bucketMissingCode:
				err = fmt.Errorf("error bucket %s could not be found: %w", config.S3Bucket, err)
			default:
				err = fmt.Errorf("error aws %s: %w", code, err)
			}
		}
		return nil, err
	}

	uploader := manager.NewUploader(s3Client)

	ctx := S3Context{
		Config:   *config,
		Uploader: uploader,
	}

	return &ctx, nil
}

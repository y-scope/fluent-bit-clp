// Package implements a context which is accessible by output plugin and stored by fluent-bit
// engine.
package outctx

import (
	"context"
	"errors"
	"fmt"
	"log"
	"unsafe"

	//"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const (
	invalidCredsCode  = "InvalidClientTokenId"
	bucketMissingCode = "NotFound"
)

// Holds configuration and state
type S3Context struct {
	Config     S3Config
	S3Uploader *manager.Uploader
}

// Creates a new context including loading of configuration and initialization of plugin state.
//
// Parameters:
//   - plugin: fluent-bit plugin reference
//
// Returns:
//   - S3Context: plugin context
//   - err: configuration load failed
func NewS3Context(plugin unsafe.Pointer) (*S3Context, error) {
	config, err := NewS3(plugin)
	if err != nil {
		return nil, err
	}

	// Load the aws credentials. Library will look for credentials in a specfic [hierarchy].
	// [hierarchy]: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/
	awsConfig, err := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithRegion(config.S3Region),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Allows user to assume a provided role. Fluent Bit s3 plugin provides functionality.
	// In many cases, the EC2 instance will already have permission for the s3 bucket;
	// however, if it dosen't, this options allows the plugin to use a role with access.
	if config.RoleArn != "" {
		stsClient := sts.NewFromConfig(awsConfig)
		creds := stscreds.NewAssumeRoleProvider(stsClient, config.RoleArn)
		awsConfig.Credentials = aws.NewCredentialsCache(creds)
	}

	// Create an Amazon S3 service client. Older version described client as thread safe, assuming
	// v2 also thread safe.
	s3Client := s3.NewFromConfig(awsConfig)

	// Confirm bucket exists. Also tests connection to client.
	_, err = s3Client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(config.S3Bucket),
	})

	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			switch code := ae.ErrorCode(); code {
			case invalidCredsCode:
				err = fmt.Errorf("error failed to connect to aws: %w", err)
			case bucketMissingCode:
				err = fmt.Errorf("error bucket %s could not be found: %w", config.S3Bucket, err)
			}
		}
		return nil, err
	}

	// Create an uploader passing it the client
	uploader := manager.NewUploader(s3Client)

	ctx := S3Context{
		Config:     *config,
		S3Uploader: uploader,
	}

	return &ctx, nil
}
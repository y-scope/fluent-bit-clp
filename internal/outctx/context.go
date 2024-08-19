// Package implements a context which is accessible by output plugin. The Fluent Bit engine stores a
// pointer to each context.

package outctx

// using outctx to prevent namespace collision with [context].
import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	"github.com/y-scope/fluent-bit-clp/internal/irzstd"
)

// Names of disk buffering directories.
const (
	IrDir   = "ir"
	ZstdDir = "zstd"
)

// AWS error codes.
const (
	invalidCredsCode  = "InvalidClientTokenId"
	bucketMissingCode = "NotFound"
)

// Holds objects accessible to plugin during flush. Fluent Bit uses a single thread for Go output
// plugin instance so no need to consider synchronization issues. C plugins use "coroutines" which
// could cause synchronization issues for C plugins according to [docs] but "coroutines" are not
// used in Go plugins.
// [docs]: https://github.com/fluent/fluent-bit/blob/master/DEVELOPER_GUIDE.md#concurrency
type S3Context struct {
	Config        S3Config
	Uploader      *manager.Uploader
	EventManagers map[string]*S3EventManager
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
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	// Load the aws credentials. [awsConfig.LoadDefaultConfig] will look for credentials in a
	// specific hierarchy.
	// https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/
	awsCfg, err := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithRegion(config.S3Region),
	)
	if err != nil {
		return nil, fmt.Errorf("could not load aws credentials %w", err)
	}

	// Allows user to assume a provided role. Fluent Bit s3 plugin provides this feature.
	// In many cases, the EC2 instance will already have permission for the s3 bucket;
	// however, if it doesn't, this option allows the plugin to assume role with bucket access.
	if config.RoleArn != "" {
		stsClient := sts.NewFromConfig(awsCfg)
		creds := stscreds.NewAssumeRoleProvider(stsClient, config.RoleArn)
		awsCfg.Credentials = aws.NewCredentialsCache(creds)
	}

	s3Client := s3.NewFromConfig(awsCfg)

	// Confirm bucket exists and test aws credentials.
	_, err = s3Client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(config.S3Bucket),
	})
	if err != nil {
		// AWS does have some error types that can be checked with [error.As] such as
		// [s3.NotFound]. However, it can be difficult to always find the appropriate type. As a
		// result, using aws [smithy-go] to handle error codes.
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
		Config:        *config,
		Uploader:      uploader,
		EventManagers: make(map[string]*S3EventManager),
	}

	return &ctx, nil
}

// If the event manager for the tag has been initialized, get the corresponding event manager. If
// not, create new one.
//
// Parameters:
//   - tag: Fluent Bit tag
//   - size: Byte length
//
// Returns:
//   - err: Could not create buffers or tag
func (ctx *S3Context) GetEventManager(tag string, size int) (*S3EventManager, error) {
	var err error
	eventManager, ok := ctx.EventManagers[tag]

	if !ok {
		eventManager, err = ctx.newEventManager(tag, size)
		if err != nil {
			return nil, err
		}
	}

	return eventManager, nil
}

// Recovers [S3EventManager] from previous execution using existing disk buffers.
//
// Parameters:
//   - tag: Fluent Bit tag
//   - size: Byte length
//
// Returns:
//   - eventManager: Manager for Fluent Bit events with the same tag
//   - err: Error creating new writer
func (ctx *S3Context) RecoverEventManager(
	tag string,
	size int,
) (*S3EventManager, error) {
	irPath, zstdPath := ctx.GetBufferFilePaths(tag)
	writer, err := irzstd.RecoverWriter(
		ctx.Config.TimeZone,
		size,
		irPath,
		zstdPath,
	)
	if err != nil {
		return nil, err
	}

	eventManager := S3EventManager{
		Tag:            tag,
		Writer:         writer,
		UploadRequests: make(chan bool),
	}

	log.Printf("Starting upload listener for event manager with tag %s", tag)
	eventManager.WaitGroup.Add(1)
	go eventManager.listen(ctx.Config, ctx.Uploader)

	ctx.EventManagers[tag] = &eventManager

	return &eventManager, nil
}

// Creates a new [S3EventManager] with a new [irzstd.Writer]. If UseDiskBuffer is set, buffers are
// created on disk and are used to buffer Fluent Bit chunks. If UseDiskBuffer is off, buffer is
// in memory and chunks are not buffered.
//
// Parameters:
//   - tag: Fluent Bit tag
//   - size: Byte length
//
// Returns:
//   - eventManager: Manager for Fluent Bit events with the same tag
//   - err: Error creating new writer
func (ctx *S3Context) newEventManager(
	tag string,
	size int,
) (*S3EventManager, error) {
	var err error
	var writer irzstd.Writer

	if ctx.Config.UseDiskBuffer {
		irPath, zstdPath := ctx.GetBufferFilePaths(tag)
		writer, err = irzstd.NewDiskWriter(
			ctx.Config.TimeZone,
			size,
			irPath,
			zstdPath,
		)

	} else {
		writer, err = irzstd.NewMemoryWriter(ctx.Config.TimeZone, size)
	}

	if err != nil {
		return nil, err
	}

	eventManager := S3EventManager{
		Tag:            tag,
		Writer:         writer,
		UploadRequests: make(chan bool),
	}

	log.Printf("Starting upload listener for event manager with tag %s", tag)
	eventManager.WaitGroup.Add(1)
	go eventManager.listen(ctx.Config, ctx.Uploader)

	ctx.EventManagers[tag] = &eventManager

	return &eventManager, nil
}

// Retrieves paths for IR and Zstd disk buffer directories.
//
// Returns:
//   - irBufferPath: Path of IR disk buffer directory
//   - zstdBufferPath: Path of Zstd disk buffer directory
func (ctx *S3Context) GetBufferPaths() (string, string) {
	irBufferPath := filepath.Join(ctx.Config.DiskBufferPath, IrDir)
	zstdBufferPath := filepath.Join(ctx.Config.DiskBufferPath, ZstdDir)
	return irBufferPath, zstdBufferPath
}

// Retrieves paths for IR and Zstd disk buffer files.
//
// Parameters:
//   - tag: Fluent Bit tag
//
// Returns:
//   - irPath: Path to IR disk buffer file
//   - zstdPath: Path to Zstd disk buffer file
func (ctx *S3Context) GetBufferFilePaths(
	tag string,
) (string, string) {
	irFileName := fmt.Sprintf("%s.ir", tag)
	irPath := filepath.Join(ctx.Config.DiskBufferPath, IrDir, irFileName)

	zstdFileName := fmt.Sprintf("%s.zst", tag)
	zstdPath := filepath.Join(ctx.Config.DiskBufferPath, ZstdDir, zstdFileName)

	return irPath, zstdPath
}

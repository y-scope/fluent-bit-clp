package outctx

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/fluent/fluent-bit-go/output"
)

// Holds settings for S3 CLP plugin from user-defined Fluent Bit configuration file.
// The "conf" struct tags are the plugin options described to user in README, and allow user to see
// snake case "use_disk_buffer" vs. camel case "UseDiskBuffer" in validation error messages. The
// "validate" struct tags are rules to be consumed by [validator]. The functionality of each rule
// can be found in docs for [validator].
//
//nolint:revive
type S3Config struct {
	S3Region       string `conf:"s3_region"        validate:"required"`
	S3Bucket       string `conf:"s3_bucket"        validate:"required"`
	S3BucketPrefix string `conf:"s3_bucket_prefix" validate:"dirpath"`
	RoleArn        string `conf:"role_arn"         validate:"omitempty,startswith=arn:aws:iam"`
	Id             string `conf:"id"               validate:"required"`
	UseDiskBuffer  bool   `conf:"use_disk_buffer"  validate:"-"`
	DiskBufferPath string `conf:"disk_buffer_path" validate:"omitempty,dirpath"`
	UploadSizeMb   int    `conf:"upload_size_mb"   validate:"omitempty,gte=2,lt=1000"`
}

// Generates configuration struct containing user-defined settings. In addition, sets default values
// and validates user input.
//
// Parameters:
//   - plugin: Fluent Bit plugin reference
//
// Returns:
//   - S3Config: Configuration based on fluent-bit.conf
//   - err: All validation errors in config wrapped, parse bool error
func NewS3Config(plugin unsafe.Pointer) (*S3Config, error) {
	// Define default values for settings. Setting defaults before validation simplifies validation
	// configuration, and ensures that default settings are also validated.
	config := S3Config{
		// Default Id is uuid to safeguard against s3 filename namespace collision. User may use
		// multiple collectors to send logs to same s3 path. Id is appended to s3 filename.
		S3Region:       "us-east-1",
		S3BucketPrefix: "logs/",
		Id:             uuid.New().String(),
		UseDiskBuffer:  true,
		DiskBufferPath: "tmp/out_clp_s3/",
		UploadSizeMb:   16,
	}

	// Map used to loop over user inputs saving a [output.FLBPluginConfigKey] call for each key.
	// Potential to iterate over struct using reflect; however, better to avoid reflect package.
	pluginSettings := map[string]interface{}{
		"s3_region":        &config.S3Region,
		"s3_bucket":        &config.S3Bucket,
		"s3_bucket_prefix": &config.S3BucketPrefix,
		"role_arn":         &config.RoleArn,
		"id":               &config.Id,
		"use_disk_buffer":  &config.UseDiskBuffer,
		"disk_buffer_path": &config.DiskBufferPath,
		"upload_size_mb":   &config.UploadSizeMb,
	}

	for settingName, untypedField := range pluginSettings {
		// [output.FLBPluginConfigKey] retrieves values defined in fluent-bit.conf. Unfortunately,
		// retrieves all values as strings. If the option is not defined by user, it is set to "".
		userInput := output.FLBPluginConfigKey(plugin, settingName)

		// If user did not specify a value, do not overwrite default value.
		if userInput == "" {
			continue
		}

		// Type switch to type parse boolean strings into boolean type. This is necessary since
		// all values are provided as strings.
		switch configField := untypedField.(type) {
		case *string:
			*configField = userInput
		case *bool:
			// This will throw error if input is "".
			boolInput, err := strconv.ParseBool(userInput)
			if err != nil {
				return nil, fmt.Errorf("error could not parse input %v into bool", userInput)
			}
			*configField = boolInput
		case *int:
			intInput, err := strconv.Atoi(userInput)
			if err != nil {
				return nil, fmt.Errorf("error could not parse input %v into int", userInput)
			}
			*configField = intInput
		default:
			return nil, fmt.Errorf("unable to parse type %T", untypedField)
		}
	}

	validate := validator.New(validator.WithRequiredStructEnabled())

	// Sets validator to return snake case setting names to user. Used example directly from
	// [validator.RegisterTagNameFunc] and replaced "json" with "conf".
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("conf"), ",", 2)[0]
		// skip if tag key says it should be ignored
		if name == "-" {
			return ""
		}
		return name
	})

	err := validate.Struct(&config)

	// Slice holds config errors allowing function to return all errors at once instead of
	// one at a time. User can fix all errors at once.
	configErrors := []error{}

	// Refactor errors provided by [validator] so they are more readable.
	if err != nil {
		valErr := err.(validator.ValidationErrors)
		// ValidateStruct will provide an error for each field, so loop over all errors.
		for _, err := range valErr {
			err := fmt.Errorf("error validating option %s=%v, failed test %s",
				err.Field(), err.Value(), err.Tag())
			configErrors = append(configErrors, err)
		}
		// Wrap all errors into one error before returning.
		err = errors.Join(configErrors...)
		return nil, err
	}

	return &config, nil
}

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
// snake case "use_single_key" vs. camel case "SingleKey" in validation error messages. The
// "validate" struct tags are rules to be consumed by [validator]. The functionality of each rule
// can be found in docs for [validator].
type S3Config struct {
	Id              string `conf:"id"                validate:"-"`
	UseSingleKey    bool   `conf:"use_single_key"    validate:"boolean"`
	AllowMissingKey bool   `conf:"allow_missing_key" validate:"boolean"`
	SingleKey       string `conf:"time_zone"         validate:"required_if=use_single_key true"`
	TimeZone        string `conf:"time_zone"         validate:"timezone"`
	S3Bucket        string `conf:"s3_bucket"         validate:"required"`
	S3BucketPrefix  string `conf:"s3_bucket_prefix"  validate:"dirpath"`
	S3Region        string `conf:"time_zone"         validate:"required"`
	RoleArn         string `conf:"role_arn"          validate:"omitempty,startswith=arn:aws:iam"`
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
	// Define default values for optional settings. Setting defaults before validation
	// simplifies validation configuration, and ensures that default settings are also validated.
	config := S3Config{
		// Default Id is uuid to safeguard against s3 filename namespace collision. User may use
		// multiple collectors to send logs to same s3 path. Id is appended to s3 filename.
		Id:              uuid.New().String(),
		UseSingleKey:    true,
		AllowMissingKey: true,
		SingleKey:       "log",
	}

	// Map used to loop over user inputs saving a [output.FLBPluginConfigKey] call for each key.
	pluginSettings := map[string]interface{}{
		"id":                &config.Id,
		"use_single_key":    &config.UseSingleKey,
		"allow_missing_key": &config.AllowMissingKey,
		"single_key":        &config.SingleKey,
		"time_zone":         &config.TimeZone,
		"s3_bucket":         &config.S3Bucket,
		"s3_bucket_prefix":  &config.S3BucketPrefix,
		"s3_region":         &config.S3Region,
		"role_arn":          &config.RoleArn,
	}

	for settingName, untypedField := range pluginSettings {
		// [output.FLBPluginConfigKey] retrieves values defined in fluent-bit.conf. Unfortunately,
		// retrieves all values as strings. If the option is not defined by user, it is set to "".
		userInput := output.FLBPluginConfigKey(plugin, settingName)

		// If user did not specify a value, do not overwrite default value.
		if userInput != "" {
			// Type switch to type parse boolean strings into boolean type. This is neccesary since
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
			default:
				return nil, fmt.Errorf("unable to parse type %T", untypedField)
			}
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
			err := fmt.Errorf("error validating option %s=%s failed test %s",
				err.Field(), err.Value(), err.Tag())
			configErrors = append(configErrors, err)
		}
		// Wrap all errors into one error before returning.
		err = errors.Join(configErrors...)
		return nil, err
	}

	return &config, err
}
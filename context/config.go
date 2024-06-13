package context

import (
	"errors"
	"fmt"
	"log"
    "strconv"
	"unsafe"


	"github.com/fluent/fluent-bit-go/output"
)

// Holds settings for s3 clp plugin from user defined fluent-bit configuration file.
type S3Config struct {
	Id   string
	Path string
	File string
	UseSingleKey bool
	Allow_Missing_Key bool
	SingleKey string
	IREncoding string
	TimeZone string
}

// Generates configuration struct containing user-defined settings.
//
// Parameters:
//   - plugin: fluent-bit plugin reference
//
// Returns:
//   - S3Config: Configuration based on fluent-bit.conf
//   - err: All errors in config wrapped
func (s *S3Config) New(plugin unsafe.Pointer) (error) {
	// TODO: redo validation using [validator]
	// [validator]: https://pkg.go.dev/github.com/go-playground/validator/v10

	// Slice holds config errors allowing function to return all errors at once instead of
	// one at a time. User can fix all errors at once.
	configErrors := []error{}

	var err error
	s.Id, err = getValueFLBConfig(plugin, "Id")
	configErrors = append(configErrors, err)

	s.Path, err = getValueFLBConfig(plugin, "Path")
	configErrors = append(configErrors, err)

	s.File, err = getValueFLBConfig(plugin, "File")
	configErrors = append(configErrors, err)

	var UseSingleKey string
	UseSingleKey, err = getValueFLBConfig(plugin, "use_single_key")
	configErrors = append(configErrors, err)

	//type conversion to bool
	s.UseSingleKey, err = strconv.ParseBool(UseSingleKey)
	configErrors = append(configErrors, err)

	var Allow_Missing_Key string
	Allow_Missing_Key, err = getValueFLBConfig(plugin, "allow_missing_key")
	configErrors = append(configErrors, err)

	//type conversion to bool
	s.Allow_Missing_Key, err = strconv.ParseBool(Allow_Missing_Key)
	configErrors = append(configErrors, err)

	//Allow nil, so no need to check error
	s.SingleKey,_ = getValueFLBConfig(plugin, "single_key")

	s.IREncoding,_ = getValueFLBConfig(plugin, "IR_encoding")
	configErrors = append(configErrors, err)

	s.TimeZone,_ = getValueFLBConfig(plugin, "time_zone")
	configErrors = append(configErrors, err)

	// Wrap all errors into one error before returning. Automically excludes nil errors.
	err = errors.Join(configErrors...)
	return err
}

// Retrieves individuals values from fluent-bit.conf.
//
// Parameters:
//   - plugin: fluent-bit plugin reference
//   - configKey: Key from fluent-bit.conf
//
// Returns:
//   - configValue
//   - err: Error if config value is blank
func getValueFLBConfig(plugin unsafe.Pointer, configKey string) (string, error) {
	configValue := output.FLBPluginConfigKey(plugin, configKey)

	if configValue == "" {
		err := fmt.Errorf("%s is not defined in fluent-bit configuration", configKey)
		return configValue, err
	}
	log.Printf("fluent-bit config key %s set to value %s", configKey, configValue)
	return configValue, nil
}


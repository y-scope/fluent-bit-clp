// Package implements loading of fluent-bit configuration file. Allows plugin to access values
// defined in configuration file.

package config

import (
	"errors"
	"fmt"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

// Holds settings for s3 clp plugin from user defined fluent-bit configuration file.
type S3Config struct {
	Id   string
	Path string
	File string
}

// Generates configuration struct containing user-defined settings.
//
// Parameters:
//   - plugin: fluent-bit plugin reference
//
// Returns:
//   - S3Config: Configuration based on fluent-bit.conf
//   - err: All errors in config wrapped
func S3New(plugin unsafe.Pointer) (*S3Config, error) {

	// Slice holds config errors allowing function to return all errors at once instead of
	// one at a time. User can fix all errors at once.
	configErrors := []error{}

	id, errID := getValueFLBConfig(plugin, "Id")
	configErrors = append(configErrors, errID)

	path, errPath := getValueFLBConfig(plugin, "Path")
	configErrors = append(configErrors, errPath)

	file, errFile := getValueFLBConfig(plugin, "File")
	configErrors = append(configErrors, errFile)

	config := &S3Config{
		Id:   id,
		Path: path,
		File: file,
	}

	// Wrap all errors into one error before returning. Automically excludes nil errors.
	err := errors.Join(configErrors...)
	return config, err
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

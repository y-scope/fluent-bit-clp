// Package implements configuration for plugins

package config

import (
	"errors"
	"fmt"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

// Holds settings for s3 clp plugin
// from user defined fluent-bit configuration file
type S3Config struct {
	Id   string
	Path string
	File string
}

// Generates configuration struct
//
// Parameters:
//   - plugin: fluent-bit plugin reference
//
// Returns:
//   - S3Config: configuration based on fluent-bit.conf
//   - err: error wrapping all errors in config
func S3New(plugin unsafe.Pointer) (*S3Config, error) {

	// slice to hold config errors
	// allows config function to return all errors at once
	// instead of one at a time
	// so user can fix all at once
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

	// wrap all errors into one error before returning
	// automically excludes nil errors
	err := errors.Join(configErrors...)
	return config, err
}

// Retrieves individuals values from fluent-bit.conf
//
// Parameters:
//   - plugin: fluent-bit plugin reference
//   - configKey: key from fluent-bit.conf
//
// Returns:
//   - configValue
//   - err: blank value
func getValueFLBConfig(plugin unsafe.Pointer, configKey string) (string, error) {
	configValue := output.FLBPluginConfigKey(plugin, configKey)

	if configValue == "" {
		err := fmt.Errorf("%s is not defined in fluent-bit configuration", configKey)
		return configValue, err
	}
	log.Printf("fluent-bit config key %s set to value %s", configKey, configValue)
	return configValue, nil
}

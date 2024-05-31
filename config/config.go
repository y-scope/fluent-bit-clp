// Package implements configuration for plugins

package config

import (
	"fmt"
	"github.com/fluent/fluent-bit-go/output" // Import the necessary Fluent Bit package
	"github.com/y-scope/fluent-bit-clp/internal/utils"
	"log"
	"unsafe"
)

// Defines settings for s3 clp plugin
// in fluent-bit configuration file
type S3Config struct {
	Id   string
	Path string
	File string
}

// Generates configuration struct
//
// Parameters:
// 	- plugin: fluent-bit plugin reference
// Returns:
// 	- S3Config: configuration based on fluent-bit.conf
func S3New(plugin unsafe.Pointer) *S3Config {
	id, err := getValueFLBConfig(plugin, "Id")
	utils.CheckFatal(err)

	path, err := getValueFLBConfig(plugin, "Path")
	utils.CheckFatal(err)

	file, err := getValueFLBConfig(plugin, "File")
	utils.CheckFatal(err)

	config := &S3Config{
		Id:   id,
		Path: path,
		File: file,
	}
	return config
}

// Retrieves individuals values from fluent-bit.conf
//
// Parameters:
// 	- plugin: fluent-bit plugin reference
// 	- configKey: key from fluent-bit.conf
// Returns:
// 	- configValue:
//  - err: blank value
func getValueFLBConfig(plugin unsafe.Pointer, configKey string) (string, error) {
	configValue := output.FLBPluginConfigKey(plugin, configKey)

	if configValue == "" {
		err := fmt.Errorf("%s is not defined in fluent-bit configuration", configKey)
		return configValue, err
	}
	log.Printf("fluent-bit config key %s set to value %s", configKey, configValue)
	return configValue, nil
}

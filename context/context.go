// Package implements a plugin context which is by fluent-bit engine. Context contains
// 	1. Configuration
// 	2. State
// Configuration allows plugin to access values defined in configuration file
// State contains stores output plugin data generated during runtime 
package context

import (
	"unsafe"
	"log"
)

// Holds configuration and state 
type S3Context struct {
    Config  S3Config
    State   S3State
}

// Creates a new context including loading of configuration and initialization of plugin state. 
//
// Parameters:
//   - plugin: fluent-bit plugin reference
//
// Returns:
//   - S3Context: plugin context
//   - err: configuration failed
func NewS3Context(plugin unsafe.Pointer) (*S3Context, error) {
	var config S3Config
	err := config.New(plugin)
	if err != nil {
		log.Fatalf("Failed to load configuration %s", err)
	}

	state := S3State{}

	ctx := S3Context{
		Config: config,
		State:  state,
	}

	return &ctx, err
}

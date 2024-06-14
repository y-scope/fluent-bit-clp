// Package implements a context which is accessible by output plugin and stored by fluent-bit
// engine.
package context

import (
	"unsafe"
)

// Holds configuration and state
type S3Context struct {
	Config S3Config
	State  S3State
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
	var config S3Config
	err := config.New(plugin)
	if err != nil {
		return nil, err
	}

	state := S3State{}

	ctx := S3Context{
		Config: config,
		State:  state,
	}

	return &ctx, nil
}

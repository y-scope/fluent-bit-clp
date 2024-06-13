package context

import (
	"unsafe"
	"log"

	"github.com/y-scope/fluent-bit-clp/internal/constant"
	"github.com/y-scope/clp-ffi-go/ffi"
)

// Define a struct
type S3Context struct {
    Config  S3Config
    EventBuffer  []ffi.LogEvent
    State   S3State
}

func NewS3Context(plugin unsafe.Pointer) (*S3Context, error) {
	var config S3Config
	err := config.New(plugin)
	if err != nil {
		log.Fatalf("Failed to load configuration %s", err)
	}

	var eventBuf []ffi.LogEvent

	state := S3State{index: 0}

	ctx := S3Context{
		Config: config,
		EventBuffer: eventBuf,
		State:  state,
	}

	return &ctx, err
}

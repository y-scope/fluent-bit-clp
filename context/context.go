package context

import (
	"unsafe"
	"log"
)

// Define a struct
type S3Context struct {
    Config  S3Config
    State   S3State
}

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

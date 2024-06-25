// Package defines high-level callback functions required by Fluent Bit go plugin documentation.
// See article/repo fo more information [Fluent Bit go], [Fluent Bit stdout example].
//
// [Fluent Bit go]: https://docs.fluentbit.io/manual/development/golang-output-plugins
// [Fluent Bit stdout example]: https://github.com/fluent/fluent-bit-go/tree/master/examples/out_multiinstance
package main

// Note package name "main" is required by Fluent Bit which suppresses go docs. Do not remove
// export, required for use by Fluent Bit C calls.

import (
	"C"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"

	"github.com/y-scope/fluent-bit-clp/internal/config"
	"github.com/y-scope/fluent-bit-clp/plugins/out_clp_s3/flush"
)

const s3PluginName = "out_clp_s3"

// Required Fluent Bit registration callback.
//
// Parameters:
//   - def: Fluent Bit plugin definition
//
// Returns:
//   - nil
//
//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	log.Printf("[%s] Register called", s3PluginName)
	return output.FLBPluginRegister(def, s3PluginName, "Clp s3 plugin")
}

// Required Fluent Bit initialization callback.
//
// Parameters:
//   - def: Fluent Bit plugin reference
//
// Returns:
//   - code: Fluent Bit success code (OK, RETRY, ERROR)
//
//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	config, err := config.NewS3(plugin)
	if err != nil {
		log.Fatalf("Failed to load configuration %s", err)
	}

	log.Printf("[%s] Init called for id: %s", s3PluginName, config.Id)

	// Set the context for this instance so that params can be retrieved during flush.
	output.FLBPluginSetContext(plugin, &config)
	return output.FLB_OK
}

// Required Fluent Bit flush callback.
//
// Parameters:
//   - ctx: Fluent Bit plugin context
//   - data: Msgpack data
//   - length: Byte length
//   - tag: Fluent Bit tag
//
// Returns:
//   - code: Fluent Bit success code (OK, RETRY, ERROR)
//
//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	p := output.FLBPluginGetContext(ctx)
	// Type assert context back into the original type for the Go variable.
	config, ok := p.(*config.S3Config)
	if !ok {
		log.Fatal("Could not read context during flush")
	}

	log.Printf("[%s] Flush called for id: %s", s3PluginName, config.Id)

	code, err := flush.ToFile(data, int(length), C.GoString(tag), config)
	if err != nil {
		log.Printf("error flushing data: %s", err)
		// RETRY or ERROR
		return code
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Printf("[%s] Exit called for unknown instance", s3PluginName)
	return output.FLB_OK
}

// Required Fluent Bit exit callback.
//
// Parameters:
//   - ctx: Fluent Bit plugin context
//
// Returns:
//   - code: Fluent Bit success code (OK, RETRY, ERROR)
//
//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	p := output.FLBPluginGetContext(ctx)
	// Type assert context back into the original type for the Go variable.

	config, ok := p.(*config.S3Config)
	if !ok {
		log.Fatal("Could not read context during flush")
	}

	log.Printf("[%s] Exit called for id: %s", s3PluginName, config.Id)
	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Printf("[%s] Unregister called", s3PluginName)
	output.FLBPluginUnregister(def)
}

func main() {
}

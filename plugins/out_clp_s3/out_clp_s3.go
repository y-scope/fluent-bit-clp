// Package defines high-level callback functions required by fluent-bit go plugin documentation. See
// article/repo fo more information [fluent-bit go], [fluent-bit stdout example].
//
// [fluent-bit go]: https://docs.fluentbit.io/manual/development/golang-output-plugins
// [fluent-bit stdout example]: https://github.com/fluent/fluent-bit-go/tree/master/examples/out_multiinstance
// nolint:revive

// Note package name "main" is required by fluent-bit which suppresses go docs. Do not remove
// export, required for use by fluent-bit C calls.
package main

import (
	"C"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"

	"github.com/y-scope/fluent-bit-clp/context"
	"github.com/y-scope/fluent-bit-clp/internal/constant"
	"github.com/y-scope/fluent-bit-clp/plugins/out_clp_s3/flush"
)

// Required fluent-bit registration callback.
//
// Parameters:
//   - def: fluent-bit plugin definition
//
// Returns:
//   - nil
//
//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	log.Printf("[%s] Register called", constant.S3PluginName)
	return output.FLBPluginRegister(def, constant.S3PluginName, "Clp s3 plugin")
}

// Required fluent-bit initialization callback.
//
// Parameters:
//   - def: fluent-bit plugin reference
//
// Returns:
//   - code: fluent-bit success code (OK, RETRY, ERROR)
//
//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	S3Ctx, err := context.NewS3Context(plugin)
	if err != nil {
		log.Fatalf("Failed to load configuration %s", err)
	}
	config := (*S3Ctx).Config

	log.Printf("[%s] Init called for id: %s", constant.S3PluginName, config.Id)

	// Set the context for this instance so that params can be retrieved during flush. Context
	// should only be set once to avoid race condition.
	output.FLBPluginSetContext(plugin, S3Ctx)
	return output.FLB_OK
}

// Required fluent-bit flush callback.
//
// Parameters:
//   - ctx: fluent-bit plugin context
//   - data: msgpack data
//   - length: Byte length
//   - tag: fluent-bit tag
//
// Returns:
//   - code: fluent-bit success code (OK, RETRY, ERROR)
//
//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	p := output.FLBPluginGetContext(ctx)
	// Type assert context back into the original type for the Go variable.
	S3Ctx, ok := (p).(*context.S3Context)
	if !ok {
		log.Fatal("Could not read config during flush")
	}
	config := (*S3Ctx).Config

	log.Printf("[%s] Flush called for id: %s", constant.S3PluginName, config.Id)

	err, success_code := flush.File(data, int(length), C.GoString(tag), S3Ctx)
	if err != nil {
		log.Printf("error flushing data %s", err)
		// retry later
		return success_code
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Printf("[%s] Exit called for unknown instance", constant.S3PluginName)
	return output.FLB_OK
}

// Required fluent-bit exit callback.
//
// Parameters:
//   - ctx: fluent-bit plugin context
//
// Returns:
//   - code: fluent-bit success code (OK, RETRY, ERROR)
//
//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	p := output.FLBPluginGetContext(ctx)
	// Type assert context back into the original type for the Go variable.

	S3Ctx, ok := (p).(*context.S3Context)
	if !ok {
		log.Fatal("Could not read config during flush")
	}
	config := (*S3Ctx).Config

	log.Printf("[%s] Exit called for id: %s", constant.S3PluginName, config.Id)
	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Printf("[%s] Unregister called", constant.S3PluginName)
	output.FLBPluginUnregister(def)
}

func main() {
}

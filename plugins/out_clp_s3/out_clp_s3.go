// Package defines high-level callback functions required by fluent-bit go
// plugin documentation. See article/repo fo more information [fluent-bit go], [fluent-bit stdout example].
//
// nolint:revive // [fluent-bit go]: https://docs.fluentbit.io/manual/development/golang-output-plugins
// nolint:revive // [fluent-bit stdout example]: https://github.com/fluent/fluent-bit-go/tree/master/examples/out_multiinstance

// note package name "main" is required by fluent-bit which suppresses go docs
// do not remove export, required for use by fluent-bit C calls
package main

import (
	"C"
	"log"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/y-scope/fluent-bit-clp/config"
	"github.com/y-scope/fluent-bit-clp/internal/constant"
	"github.com/y-scope/fluent-bit-clp/plugins/out_clp_s3/flush"
)

// fluent-bit registration callback
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

// fluent-bit initialization callback
//
// Parameters:
//   - def: fluent-bit plugin reference
//
// Returns:
//   - code: fluent-bit success code (OK,RETRY,ERROR)
//
//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	// returns pointer to a config instance based on fluent-bit configuration
	config := config.S3New(plugin)
	log.Printf("[%s] Init called for id: %s", constant.S3PluginName, config.Id)

	// set the context for this instance so that params can be retrieved during flush
	// context should only be set once to avoid race condition
	output.FLBPluginSetContext(plugin, config)
	return output.FLB_OK
}

// fluent-bit flush callback
//
// Parameters:
//   - ctx: fluent-bit plugin context
//   - data: msgpack data
//   - length: byte length
//   - tag: fluent-bit tag
//
// Returns:
//   - code: fluent-bit success code (OK,RETRY,ERROR)
//
//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	p := output.FLBPluginGetContext(ctx)
	// Type assert context back into the original type for the Go variable
	config := (p).(*config.S3Config)
	log.Printf("[%s] Flush called for id: %s", constant.S3PluginName, config.Id)

	err := flush.File(data, int(length), C.GoString(tag), config)
	if err != nil {
		log.Printf("error flushing data %s", err)
		// retry later
		return output.FLB_RETRY
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Printf("[%s] Exit called for unknown instance", constant.S3PluginName)
	return output.FLB_OK
}

// fluent-bit exit callback
//
// Parameters:
//   - ctx: fluent-bit plugin context
//
// Returns:
//   - code: fluent-bit success code (OK,RETRY,ERROR)
//
//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	p := output.FLBPluginGetContext(ctx)
	// Type assert context back into the original type for the Go variable
	config := (p).(*config.S3Config)
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

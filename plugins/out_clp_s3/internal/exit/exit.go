// Package exit provides functions for gracefully shutting down the plugin. Exit functions are only
// called when Fluent Bit receives a kill signal, not during an abrupt crash. The plugin is given
// limited time to clean up resources before Fluent Bit terminates it.

package exit

import (
	"github.com/y-scope/fluent-bit-clp/internal/outctx"
)

// Fs gracefully exits the plugin by closing files.
//
// Parameters:
//   - ctx: Plugin context
//
// Returns:
//   - err: Error closing file
func Fs(ctx *outctx.S3Context) error {
	for _, eventManager := range ctx.EventManagers {
		err := eventManager.Writer.Close()
		if err != nil {
			return err
		}
		eventManager.Writer = nil
	}

	return nil
}

// S3 gracefully exits the plugin by flushing buffered data to S3. Makes a best-effort attempt,
// however Fluent Bit may kill the plugin before the upload completes, resulting in data loss.
//
// Parameters:
//   - ctx: Plugin context
//
// Returns:
//   - err: Error closing file
func S3(ctx *outctx.S3Context) error {
	for _, eventManager := range ctx.EventManagers {
		empty, err := eventManager.Writer.CheckEmpty()
		if err != nil {
			return err
		}
		if empty {
			continue
		}
		err = eventManager.ToS3(ctx.Config, ctx.Uploader)
		if err != nil {
			return err
		}
		err = eventManager.Writer.Close()
		if err != nil {
			return err
		}
		eventManager.Writer = nil
	}

	return nil
}
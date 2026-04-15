// Package pathregistry tracks disk buffer paths to prevent multiple output
// instances from using the same path.
package pathregistry

import (
	"fmt"
	"path/filepath"
	"sync"
)

var (
	paths   = make(map[string]bool)
	pathsMu sync.Mutex
)

// Registers a disk buffer path to prevent multiple output instances from using the same path.
//
// Parameters:
//   - path: Disk buffer path
//
// Returns:
//   - err: Error resolving path, or path already in use
func Register(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("could not resolve path: %w", err)
	}

	pathsMu.Lock()
	defer pathsMu.Unlock()

	if paths[absPath] {
		return fmt.Errorf("disk_buffer_path %s is already in use by another output instance", path)
	}
	paths[absPath] = true
	return nil
}

// Unregister removes a disk buffer path from registry.
//
// Parameters:
//   - path: Disk buffer path
func Unregister(path string) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return
	}

	pathsMu.Lock()
	defer pathsMu.Unlock()

	delete(paths, absPath)
}

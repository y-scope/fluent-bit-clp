package recovery

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mockFileInfo implements fs.FileInfo for testing
type mockFileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (m mockFileInfo) Name() string       { return m.name }
func (m mockFileInfo) Size() int64        { return m.size }
func (m mockFileInfo) Mode() fs.FileMode  { return m.mode }
func (m mockFileInfo) ModTime() time.Time { return m.modTime }
func (m mockFileInfo) IsDir() bool        { return m.isDir }
func (m mockFileInfo) Sys() any           { return nil }

func TestCheckFilesValid_MatchingFiles(t *testing.T) {
	irFiles := map[string]fs.FileInfo{
		"tag1": mockFileInfo{name: "tag1.ir"},
		"tag2": mockFileInfo{name: "tag2.ir"},
	}
	zstdFiles := map[string]fs.FileInfo{
		"tag1": mockFileInfo{name: "tag1.zst"},
		"tag2": mockFileInfo{name: "tag2.zst"},
	}

	err := checkFilesValid(irFiles, zstdFiles)
	if err != nil {
		t.Errorf("checkFilesValid() error = %v, want nil", err)
	}
}

func TestCheckFilesValid_EmptyMaps(t *testing.T) {
	irFiles := map[string]fs.FileInfo{}
	zstdFiles := map[string]fs.FileInfo{}

	err := checkFilesValid(irFiles, zstdFiles)
	if err != nil {
		t.Errorf("checkFilesValid() with empty maps error = %v, want nil", err)
	}
}

func TestCheckFilesValid_DifferentLengths(t *testing.T) {
	irFiles := map[string]fs.FileInfo{
		"tag1": mockFileInfo{name: "tag1.ir"},
		"tag2": mockFileInfo{name: "tag2.ir"},
	}
	zstdFiles := map[string]fs.FileInfo{
		"tag1": mockFileInfo{name: "tag1.zst"},
	}

	err := checkFilesValid(irFiles, zstdFiles)
	if err == nil {
		t.Error("checkFilesValid() expected error for different lengths, got nil")
	}
}

func TestCheckFilesValid_MissingZstdFile(t *testing.T) {
	irFiles := map[string]fs.FileInfo{
		"tag1": mockFileInfo{name: "tag1.ir"},
		"tag2": mockFileInfo{name: "tag2.ir"},
	}
	zstdFiles := map[string]fs.FileInfo{
		"tag1":      mockFileInfo{name: "tag1.zst"},
		"different": mockFileInfo{name: "different.zst"},
	}

	err := checkFilesValid(irFiles, zstdFiles)
	if err == nil {
		t.Error("checkFilesValid() expected error for mismatched keys, got nil")
	}
}

func TestReadDirectory_NonExistentDirectory(t *testing.T) {
	files, err := readDirectory("/nonexistent/path/that/does/not/exist")
	if err != nil {
		t.Errorf("readDirectory() error = %v, want nil for non-existent", err)
	}
	if len(files) != 0 {
		t.Errorf("readDirectory() returned %d files, want 0", len(files))
	}
}

func TestReadDirectory_EmptyDirectory(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "recovery_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	files, err := readDirectory(tmpDir)
	if err != nil {
		t.Errorf("readDirectory() error = %v, want nil", err)
	}
	if len(files) != 0 {
		t.Errorf("readDirectory() returned %d files, want 0", len(files))
	}
}

func TestReadDirectory_WithFiles(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "recovery_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	testFiles := []string{"tag1.ir", "tag2.ir", "another.ir"}
	for _, name := range testFiles {
		f, err := os.Create(filepath.Join(tmpDir, name))
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		f.Close()
	}

	files, err := readDirectory(tmpDir)
	if err != nil {
		t.Errorf("readDirectory() error = %v, want nil", err)
	}
	if len(files) != len(testFiles) {
		t.Errorf("readDirectory() returned %d files, want %d", len(files), len(testFiles))
	}

	// Verify tags (filenames without extension)
	expectedTags := map[string]bool{"tag1": true, "tag2": true, "another": true}
	for tag := range files {
		if !expectedTags[tag] {
			t.Errorf("readDirectory() unexpected tag %q", tag)
		}
	}
}

func TestReadDirectory_DuplicateTags(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "recovery_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create files with same base name but different extensions
	// This simulates a bug scenario where both tag1.ir and tag1.other exist
	testFiles := []string{"tag1.ir", "tag1.other"}
	for _, name := range testFiles {
		f, err := os.Create(filepath.Join(tmpDir, name))
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		f.Close()
	}

	_, err = readDirectory(tmpDir)
	if err == nil {
		t.Error("readDirectory() expected error for duplicate tags, got nil")
	}
}

func TestReadDirectory_IgnoresSubdirectories(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "recovery_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a regular file
	f, err := os.Create(filepath.Join(tmpDir, "tag1.ir"))
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	f.Close()

	// Create a subdirectory
	subDir := filepath.Join(tmpDir, "subdir.ir")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	_, err = readDirectory(tmpDir)
	// Should error because subdirectory is not a regular file
	if err == nil {
		t.Error("readDirectory() expected error for subdirectory, got nil")
	}
}

func TestRemoveBufferFiles(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "recovery_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	irPath := filepath.Join(tmpDir, "test.ir")
	zstdPath := filepath.Join(tmpDir, "test.zst")

	// Create files
	if f, err := os.Create(irPath); err != nil {
		t.Fatalf("Failed to create IR file: %v", err)
	} else {
		f.Close()
	}
	if f, err := os.Create(zstdPath); err != nil {
		t.Fatalf("Failed to create Zstd file: %v", err)
	} else {
		f.Close()
	}

	// Remove files
	err = removeBufferFiles(irPath, zstdPath)
	if err != nil {
		t.Errorf("removeBufferFiles() error = %v, want nil", err)
	}

	// Verify files are removed
	if _, err := os.Stat(irPath); !os.IsNotExist(err) {
		t.Error("IR file should have been removed")
	}
	if _, err := os.Stat(zstdPath); !os.IsNotExist(err) {
		t.Error("Zstd file should have been removed")
	}
}

func TestRemoveBufferFiles_MissingIRFile(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "recovery_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	irPath := filepath.Join(tmpDir, "nonexistent.ir")
	zstdPath := filepath.Join(tmpDir, "test.zst")

	// Create only zstd file
	if f, err := os.Create(zstdPath); err != nil {
		t.Fatalf("Failed to create Zstd file: %v", err)
	} else {
		f.Close()
	}

	err = removeBufferFiles(irPath, zstdPath)
	if err == nil {
		t.Error("removeBufferFiles() expected error for missing IR file, got nil")
	}
}

func TestRemoveBufferFiles_MissingZstdFile(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "recovery_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	irPath := filepath.Join(tmpDir, "test.ir")
	zstdPath := filepath.Join(tmpDir, "nonexistent.zst")

	// Create only IR file
	if f, err := os.Create(irPath); err != nil {
		t.Fatalf("Failed to create IR file: %v", err)
	} else {
		f.Close()
	}

	err = removeBufferFiles(irPath, zstdPath)
	if err == nil {
		t.Error("removeBufferFiles() expected error for missing Zstd file, got nil")
	}
}

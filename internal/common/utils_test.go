package common

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	dst := filepath.Join(dir, "dst.db")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0600))

	require.NoError(t, CopyFile(src, dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
}

func TestCopyFile_DestinationExists(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	dst := filepath.Join(dir, "dst.db")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0600))
	require.NoError(t, os.WriteFile(dst, []byte("keep"), 0600))

	require.Error(t, CopyFile(src, dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, "keep", string(got))
}

// A dangling symlink passes the os.Stat existence check, so the output file
// must be created exclusively rather than through the link.
func TestCopyFile_DestinationDanglingSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need extra privileges on Windows")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	dst := filepath.Join(dir, "dst.db")
	target := filepath.Join(dir, "target.db")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0600))
	require.NoError(t, os.Symlink(target, dst))

	require.Error(t, CopyFile(src, dst))

	_, err := os.Stat(target)
	require.True(t, os.IsNotExist(err), "CopyFile must not create the symlink target")
}

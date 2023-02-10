package failpoint

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	gofail "go.etcd.io/gofail/runtime"
)

func TestFailpoint_MapFail(t *testing.T) {
	err := gofail.Enable("mapError", `return("map somehow failed")`)
	require.NoError(t, err)
	defer func() {
		err = gofail.Disable("mapError")
		require.NoError(t, err)
	}()

	f := filepath.Join(t.TempDir(), "db")
	_, err = bolt.Open(f, 0666, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "map somehow failed")
}

// ensures when munmap fails, the flock is unlocked
func TestFailpoint_UnmapFail_DbClose(t *testing.T) {
	//unmap error on db close
	//we need to open the db first, and then enable the error.
	//otherwise the db cannot be opened.
	f := filepath.Join(t.TempDir(), "db")

	err := gofail.Enable("unmapError", `return("unmap somehow failed")`)
	require.NoError(t, err)
	_, err = bolt.Open(f, 0666, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "unmap somehow failed")
	//disable the error, and try to reopen the db
	err = gofail.Disable("unmapError")
	require.NoError(t, err)

	db, err := bolt.Open(f, 0666, &bolt.Options{Timeout: 30 * time.Second})
	require.NoError(t, err)
	err = db.Close()
	require.NoError(t, err)
}

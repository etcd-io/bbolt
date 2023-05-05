package failpoint

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
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

func TestFailpoint_mLockFail(t *testing.T) {
	err := gofail.Enable("mlockError", `return("mlock somehow failed")`)
	require.NoError(t, err)

	f := filepath.Join(t.TempDir(), "db")
	_, err = bolt.Open(f, 0666, &bolt.Options{Mlock: true})
	require.Error(t, err)
	require.ErrorContains(t, err, "mlock somehow failed")

	// It should work after disabling the failpoint.
	err = gofail.Disable("mlockError")
	require.NoError(t, err)

	_, err = bolt.Open(f, 0666, &bolt.Options{Mlock: true})
	require.NoError(t, err)
}

func TestFailpoint_mLockFail_When_remap(t *testing.T) {
	db := btesting.MustCreateDB(t)
	db.Mlock = true

	err := gofail.Enable("mlockError", `return("mlock somehow failed in allocate")`)
	require.NoError(t, err)

	err = db.Fill([]byte("data"), 1, 10000,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 100) },
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "mlock somehow failed in allocate")

	// It should work after disabling the failpoint.
	err = gofail.Disable("mlockError")
	require.NoError(t, err)
	db.MustClose()
	db.MustReopen()

	err = db.Fill([]byte("data"), 1, 10000,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 100) },
	)

	require.NoError(t, err)
}

func TestFailpoint_ResizeFileFail(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := gofail.Enable("resizeFileError", `return("resizeFile somehow failed")`)
	require.NoError(t, err)

	err = db.Fill([]byte("data"), 1, 10000,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 100) },
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "resizeFile somehow failed")

	// It should work after disabling the failpoint.
	err = gofail.Disable("resizeFileError")
	require.NoError(t, err)
	db.MustClose()
	db.MustReopen()

	err = db.Fill([]byte("data"), 1, 10000,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 100) },
	)

	require.NoError(t, err)
}

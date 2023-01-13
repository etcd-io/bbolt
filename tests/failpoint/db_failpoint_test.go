package failpoint

import (
	"path/filepath"
	"testing"

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

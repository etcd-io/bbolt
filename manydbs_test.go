package bbolt

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func createDb(t *testing.T) (*DB, func()) {
	// First, create a temporary directory to be used for the duration of
	// this test.
	tempDirName, err := os.MkdirTemp("", "bboltmemtest")
	require.NoErrorf(t, err, "error creating temp dir: %v", err)
	path := filepath.Join(tempDirName, "testdb.db")

	bdb, err := Open(path, 0600, nil)
	require.NoErrorf(t, err, "error creating bbolt db: %v", err)

	cleanup := func() {
		bdb.Close()
		os.RemoveAll(tempDirName)
	}

	return bdb, cleanup
}

func createAndPutKeys(t *testing.T) {
	t.Parallel()

	db, cleanup := createDb(t)
	defer cleanup()

	bucketName := []byte("bucket")

	for i := 0; i < 100; i++ {
		err := db.Update(func(tx *Tx) error {
			nodes, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return err
			}

			var key [16]byte
			_, rerr := rand.Read(key[:])
			if rerr != nil {
				return rerr
			}
			if err := nodes.Put(key[:], nil); err != nil {
				return err
			}

			return nil
		})
		require.NoError(t, err)
	}
}

func TestManyDBs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("%d", i), createAndPutKeys)
	}
}

package bbolt

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

// TestTx_recursivelyCheckBucketPages ensures that checking all buckets in a
// single pass over their pages reaches exactly the same pages as checking
// them in multiple passes does, and that the freelist is reconstructed from
// the pages that aren't.
func TestTx_recursivelyCheckBucketPages(t *testing.T) {
	// A page size below the one of the OS results in pages that don't
	// start at an OS page boundary.
	for _, pageSize := range []int{0, 1024} {
		t.Run(fmt.Sprintf("pageSize=%d", pageSize), func(t *testing.T) {
			testRecursivelyCheckBucketPages(t, pageSize)
		})
	}
}

func testRecursivelyCheckBucketPages(t *testing.T, pageSize int) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, 0600, &Options{PageSize: pageSize})
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(1))
	randBytes := func(n int) []byte {
		b := make([]byte, n)
		_, _ = rng.Read(b)
		return b
	}

	// Create buckets that span multiple levels of branch pages, with
	// values that need overflow pages, with nested buckets that have
	// pages of their own and with nested buckets that are inline.
	for round := 0; round < 10; round++ {
		err = db.Update(func(tx *Tx) error {
			for i := 0; i < 6; i++ {
				top, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("top-%d", i)))
				if err != nil {
					return err
				}
				for j := 0; j < 300; j++ {
					if err := top.Put(randBytes(16), randBytes(100)); err != nil {
						return err
					}
				}

				nested, err := top.CreateBucketIfNotExists([]byte(fmt.Sprintf("nested-%d", round%4)))
				if err != nil {
					return err
				}
				if err := nested.Put(randBytes(8), randBytes(5*4096)); err != nil {
					return err
				}

				inline, err := nested.CreateBucketIfNotExists([]byte("inline"))
				if err != nil {
					return err
				}
				if err := inline.Put([]byte("key"), []byte("value")); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
	}

	// Free a good number of pages.
	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 6; i += 2 {
			if err := tx.DeleteBucket([]byte(fmt.Sprintf("top-%d", i))); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	check := func(fn func(tx *Tx, reachable map[common.Pgid]*common.Page, ch chan error)) map[common.Pgid]*common.Page {
		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, tx.Rollback())
		}()

		reachable := make(map[common.Pgid]*common.Page)
		ch := make(chan error)
		go func() {
			defer close(ch)
			fn(tx, reachable, ch)
		}()
		for err := range ch {
			require.NoError(t, err)
		}

		return reachable
	}

	freed := make(map[common.Pgid]bool)
	multiPass := check(func(tx *Tx, reachable map[common.Pgid]*common.Page, ch chan error) {
		tx.recursivelyCheckBucket(tx.root, reachable, freed, HexKVStringer(), ch)
	})
	singlePass := check(func(tx *Tx, reachable map[common.Pgid]*common.Page, ch chan error) {
		tx.recursivelyCheckBucketPages(tx.root.RootPage(), reachable, freed, HexKVStringer(), ch)
	})
	require.NotEmpty(t, multiPass)
	require.Equal(t, multiPass, singlePass)

	// The freelist reconstructed when opening a database without a synced
	// freelist consists of all pages that aren't reachable.
	var expectedFree []common.Pgid
	for id := common.Pgid(2); id < db.meta().Pgid(); id++ {
		if _, ok := multiPass[id]; !ok {
			expectedFree = append(expectedFree, id)
		}
	}
	require.NotEmpty(t, expectedFree)
	require.Equal(t, expectedFree, db.freepages())
	require.NoError(t, db.Close())
}

// TestTx_recursivelyCheckBucketPages_Writable ensures that buckets are not
// checked by their pages in a writable transaction, whose changes are not in
// the pages yet.
func TestTx_recursivelyCheckBucketPages_Writable(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), 0600, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	err = db.Update(func(tx *Tx) error {
		require.PanicsWithValue(t,
			"assertion failed: buckets can't be checked by their pages in a writable transaction",
			func() {
				tx.recursivelyCheckBucketPages(
					tx.root.RootPage(), make(map[common.Pgid]*common.Page),
					make(map[common.Pgid]bool), HexKVStringer(), make(chan error),
				)
			},
		)
		return nil
	})
	require.NoError(t, err)
}

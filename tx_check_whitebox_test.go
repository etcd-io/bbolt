package bbolt

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

// TestTx_recursivelyCheckBucket ensures that checking all buckets reaches
// every page they consist of, in a read-only transaction as well as in a
// writable one, and that the freelist is reconstructed from the pages that
// aren't reached.
func TestTx_recursivelyCheckBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, 0600, nil)
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

	check := func(writable bool) map[common.Pgid]bool {
		tx, err := db.Begin(writable)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, tx.Rollback())
		}()

		reachable := make(map[common.Pgid]*common.Page)
		ch := make(chan error)
		go func() {
			defer close(ch)
			tx.recursivelyCheckBucket(tx.root, reachable, make(map[common.Pgid]bool), HexKVStringer(), ch)
		}()
		for err := range ch {
			require.NoError(t, err)
		}

		ids := make(map[common.Pgid]bool)
		for id := range reachable {
			ids[id] = true
		}
		return ids
	}

	// Collect the pages of all buckets without the help of the check.
	expected := make(map[common.Pgid]bool)
	var collect func(b *Bucket) error
	collect = func(b *Bucket) error {
		if b.RootPage() != 0 {
			b.tx.forEachPage(b.RootPage(), func(p *common.Page, _ int, _ []common.Pgid) {
				for i := common.Pgid(0); i <= common.Pgid(p.Overflow()); i++ {
					expected[p.Id()+i] = true
				}
			})
		}
		return b.ForEachBucket(func(k []byte) error {
			return collect(b.Bucket(k))
		})
	}
	require.NoError(t, db.View(func(tx *Tx) error {
		return collect(tx.root)
	}))
	require.NotEmpty(t, expected)

	require.Equal(t, expected, check(false))
	require.Equal(t, expected, check(true))

	// The freelist reconstructed when opening a database without a synced
	// freelist consists of all pages that aren't reachable.
	var expectedFree []common.Pgid
	for id := common.Pgid(2); id < db.meta().Pgid(); id++ {
		if !expected[id] {
			expectedFree = append(expectedFree, id)
		}
	}
	require.NotEmpty(t, expectedFree)
	require.Equal(t, expectedFree, db.freepages())
	require.NoError(t, db.Close())
}

// TestTx_Check_Writable ensures that a writable transaction is checked by its
// in-memory state, which the pages don't reflect before it is committed.
func TestTx_Check_Writable(t *testing.T) {
	testCases := []struct {
		name   string
		change func(tx *Tx) error
	}{
		{
			name: "delete a bucket",
			change: func(tx *Tx) error {
				return tx.Bucket([]byte("a")).DeleteBucket([]byte("nested"))
			},
		},
		{
			name: "recreate a bucket",
			change: func(tx *Tx) error {
				if err := tx.Bucket([]byte("a")).DeleteBucket([]byte("nested")); err != nil {
					return err
				}
				_, err := tx.Bucket([]byte("a")).CreateBucket([]byte("nested"))
				return err
			},
		},
		{
			name: "move a bucket",
			change: func(tx *Tx) error {
				return tx.MoveBucket([]byte("nested"), tx.Bucket([]byte("a")), tx.Bucket([]byte("b")))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "db"), 0600, nil)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, db.Close())
			}()

			// None of the buckets must be inline.
			err = db.Update(func(tx *Tx) error {
				fill := func(b *Bucket, err error) error {
					if err != nil {
						return err
					}
					for i := 0; i < 1000; i++ {
						if err := b.Put([]byte(fmt.Sprintf("key-%04d", i)), make([]byte, 100)); err != nil {
							return err
						}
					}
					return nil
				}
				for _, name := range []string{"a", "b"} {
					if err := fill(tx.CreateBucket([]byte(name))); err != nil {
						return err
					}
				}
				return fill(tx.Bucket([]byte("a")).CreateBucket([]byte("nested")))
			})
			require.NoError(t, err)

			err = db.Update(func(tx *Tx) error {
				if err := tc.change(tx); err != nil {
					return err
				}
				for err := range tx.Check() {
					require.NoError(t, err)
				}
				return nil
			})
			require.NoError(t, err)

			err = db.View(func(tx *Tx) error {
				for err := range tx.Check() {
					require.NoError(t, err)
				}
				return nil
			})
			require.NoError(t, err)
		})
	}
}

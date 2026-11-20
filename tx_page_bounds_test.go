package bbolt

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt/internal/common"
)

// TestTxPageBoundsCheck is a regression test for
// https://github.com/etcd-io/bbolt/issues/1318: page ids read from the file
// are used to index straight into the mmap without a bounds check, so a
// corrupt id either panics with an index-out-of-range error or, worse,
// faults with an unrecoverable SIGSEGV. Tx.page now rejects ids at or above
// the high-water mark with a clear, recoverable panic.
//
// BoundaryIdsOnFreshDB pins the exact boundary: the last valid id
// (hwm-1) must still resolve, while hwm itself and anything above it must
// panic, so the guard has to be ">=" and not ">". AllocatedPageInWriteTx
// covers the dirty-page path: a freshly allocated and written page must
// be served back from the tx's dirty pages, not from the mmap (whose
// copy is not written yet).
func TestTxPageBoundsCheck(t *testing.T) {
	openCopy := func(t *testing.T, fixture string) (*DB, string) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("testdata", "issue1318", fixture))
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), "db")
		require.NoError(t, os.WriteFile(path, data, 0600))
		db, err := Open(path, 0600, &Options{PreLoadFreelist: true})
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		return db, path
	}

	openFresh := func(t *testing.T) *DB {
		t.Helper()
		db, err := Open(filepath.Join(t.TempDir(), "db"), 0600, nil)
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		return db
	}

	// mustPanicAboveHighWaterMark asserts the panic value is exactly the
	// guard's message for the given id and high-water mark, so a wrong id
	// or hwm in the message (or a panic from a different check) fails.
	mustPanicAboveHighWaterMark := func(t *testing.T, id, hwm uint64, fn func()) {
		t.Helper()
		want := fmt.Sprintf("pgid (%d) above high water mark (%d)", id, hwm)
		require.PanicsWithValue(t, want, fn)
	}

	t.Run("WalkWithOutOfRangeBranchElement", func(t *testing.T) {
		db, _ := openCopy(t, "bbolt_db.go_1055.db")
		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()
		mustPanicAboveHighWaterMark(t, 137438953473, 4, func() {
			_ = tx.ForEach(func(name []byte, b *Bucket) error {
				c := b.Cursor()
				for k, _ := c.First(); k != nil; k, _ = c.Next() {
				}
				return nil
			})
		})
	})

	t.Run("PutIntoInlineBucketWithBogusPageId", func(t *testing.T) {
		db, _ := openCopy(t, "bbolt_node.go_363.db")
		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()
		mustPanicAboveHighWaterMark(t, 268435456, 6, func() {
			b := tx.Bucket([]byte("auth"))
			require.NotNil(t, b)
			require.NoError(t, b.Put([]byte("repro"), []byte("v")))
			// The panic surfaces during Commit, when the spill frees the
			// page the inline bucket's bogus id points at.
			require.NoError(t, tx.Commit())
		})
	})

	t.Run("BoundaryIdsOnFreshDB", func(t *testing.T) {
		db := openFresh(t)
		tx, err := db.Begin(false)
		require.NoError(t, err)
		defer tx.Rollback()
		hwm := uint64(tx.meta.Pgid())
		require.Greater(t, hwm, uint64(0))
		t.Logf("high-water mark on a fresh DB: %d", hwm)

		// The last valid page id resolves without a panic.
		require.NotPanics(t, func() { _ = tx.page(tx.meta.Pgid() - 1) })

		// id == hwm is NOT a valid page: with ">" instead of ">=" this
		// would slip past the guard and fault in Page.FastCheck.
		mustPanicAboveHighWaterMark(t, hwm, hwm, func() { _ = tx.page(tx.meta.Pgid()) })
		mustPanicAboveHighWaterMark(t, hwm+100, hwm, func() { _ = tx.page(tx.meta.Pgid() + 100) })
	})

	t.Run("AllocatedPageInWriteTx", func(t *testing.T) {
		db := openFresh(t)
		tx, err := db.Begin(true)
		require.NoError(t, err)
		defer tx.Rollback()
		hwmBefore := tx.meta.Pgid()
		p, err := tx.allocate(1)
		require.NoError(t, err)
		// allocate() takes the page at the old high-water mark and moves
		// the mark past it, so the allocated id ends up just below the
		// new hwm. The page still has to come back from the tx's dirty
		// pages: the mmap copy is not written yet, so serving it from
		// there would fail Page.FastCheck on a zeroed page.
		//
		// Note this does not pin the guard's position relative to the
		// lookup: db.allocate advances the high-water mark immediately,
		// so the allocated id is below the new hwm and the guard would
		// not fire for it in either position. What it pins is the
		// dirty-page return path itself.
		require.Equal(t, hwmBefore, p.Id())
		require.Equal(t, hwmBefore+1, tx.meta.Pgid())
		// A freshly allocated page has no type flags until it is
		// written (node.write sets them), and FastCheck rejects a
		// flagless page, so mark it as a leaf page first, as a written
		// dirty page would be.
		p.SetFlags(common.LeafPageFlag)
		var got *common.Page
		require.NotPanics(t, func() { got = tx.page(p.Id()) })
		require.Same(t, p, got)
	})
}

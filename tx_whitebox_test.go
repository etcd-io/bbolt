package bbolt

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
	"go.etcd.io/bbolt/internal/surgeon"
)

// TestTx_forEachPage_CycleTerminates corrupts a db so that a branch page's
// child list loops back on itself, then verifies that tx.forEachPage
// terminates instead of recursing until the goroutine stack overflows.
// See issue #701 for the real-world corruption pattern.
func TestTx_forEachPage_CycleTerminates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		for i := 0; i < 500; i++ {
			if err := b.Put([]byte(fmt.Sprintf("%04d", i)), make([]byte, 100)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	// Pick a branch ancestor and one of its leaf descendants; overwriting the
	// leaf with a copy of the branch leaves the leaf as a branch whose child
	// list still references the leaf's own pgid, forming a cycle.
	xray := surgeon.NewXRay(path)
	paths, err := xray.FindPathsToKey([]byte("0001"))
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	p0 := paths[0]
	require.GreaterOrEqual(t, len(p0), 2, "need at least one branch above the leaf")
	ancestor := p0[len(p0)-2]
	leaf := p0[len(p0)-1]
	require.NoError(t, surgeon.CopyPage(path, ancestor, leaf))

	ancestorPage, _, err := guts_cli.ReadPage(path, uint64(ancestor))
	require.NoError(t, err)
	require.True(t, ancestorPage.IsBranchPage())
	var hasLeafAsChild bool
	for i := uint16(0); i < ancestorPage.Count(); i++ {
		if ancestorPage.BranchPageElement(i).Pgid() == common.Pgid(leaf) {
			hasLeafAsChild = true
			break
		}
	}
	require.True(t, hasLeafAsChild, "expected ancestor to reference the leaf directly")

	db, err = Open(path, 0600, nil)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.View(func(tx *Tx) error {
		rootPage := tx.Bucket([]byte("data")).RootPage()
		var count int
		leafVisits := 0
		tx.forEachPage(rootPage, func(p *common.Page, _ int, _ []common.Pgid) {
			count++
			if p.Id() == common.Pgid(leaf) {
				leafVisits++
			}
		})
		require.Less(t, count, 10_000, "forEachPage walked %d pages; cycle detection likely missing", count)
		require.GreaterOrEqual(t, leafVisits, 2, "expected fn to fire on the cycle back-edge so duplicate-reference diagnostics still run")
		return nil
	}))
}

package bbolt_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/guts_cli"
)

// TestTx_Check_SingleElementBranchPage verifies that Tx.Check flags a
// branch page whose element count was corrupted to 1. The writer never
// produces branch pages with fewer than two elements, and rebalancing
// a child of such a page panics on the assertion that a parent must
// have at least two children, so Check must surface the corruption
// itself instead of staying silent (page-scoped check) or only
// reporting the orphaned subtree pages as unreachable (full check).
func TestTx_Check_SingleElementBranchPage(t *testing.T) {
	bucketName := []byte("data")
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})

	// Fill enough key/value pairs for a three-level tree, so the bucket
	// has branch pages below its root page.
	err := db.Fill(bucketName, 1, 8000,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%08d", k)) },
		func(tx int, k int) []byte { return make([]byte, 100) },
	)
	require.NoError(t, err)

	// Find a non-root branch page.
	rootPageId := mustGetBucketRootPage(t, db.DB, bucketName)
	rootPage, _, err := guts_cli.ReadPage(db.Path(), uint64(rootPageId))
	require.NoError(t, err)
	require.True(t, rootPage.IsBranchPage())

	var victimPageId uint64
	for _, elem := range rootPage.BranchPageElements() {
		child, _, err := guts_cli.ReadPage(db.Path(), uint64(elem.Pgid()))
		require.NoError(t, err)
		if child.IsBranchPage() && child.Count() > 1 {
			victimPageId = uint64(elem.Pgid())
			break
		}
	}
	require.NotZero(t, victimPageId, "expected a non-root branch page")

	collect := func(options ...bbolt.CheckOption) []error {
		var errs []error
		vErr := db.View(func(tx *bbolt.Tx) error {
			for cErr := range tx.Check(options...) {
				errs = append(errs, cErr)
			}
			return nil
		})
		require.NoError(t, vErr)
		return errs
	}

	// The healthy database passes both checks.
	require.Empty(t, collect())
	require.Empty(t, collect(bbolt.WithPageId(victimPageId)))

	// Corrupt the branch page: shrink its element count to 1.
	victim, victimBuf, err := guts_cli.ReadPage(db.Path(), victimPageId)
	require.NoError(t, err)
	victim.SetCount(1)
	require.NoError(t, guts_cli.WritePage(db.Path(), victimBuf))

	// A page-scoped check on the corrupted page must report exactly the
	// single-element branch page (before the fix it reported nothing).
	errs := collect(bbolt.WithPageId(victimPageId))
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), fmt.Sprintf("page %d:", victimPageId))
	require.Contains(t, errs[0].Error(), "fewer than 2 elements")

	// A full check must identify the corrupted branch page as well.
	found := false
	for _, cErr := range collect() {
		if strings.Contains(cErr.Error(), "fewer than 2 elements") &&
			strings.Contains(cErr.Error(), fmt.Sprintf("page %d:", victimPageId)) {
			found = true
		}
	}
	require.True(t, found, "full check should flag the single-element branch page")

	// Manually close the db, otherwise the PostTestCleanup will
	// check the db again and accordingly fail the test.
	db.MustClose()
}

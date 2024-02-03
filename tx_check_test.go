package bbolt_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func TestTx_Check_CorruptPage(t *testing.T) {
	t.Log("Creating db file.")
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})

	// Each page can hold roughly 20 key/values pair, so 100 such
	// key/value pairs will consume about 5 leaf pages.
	err := db.Fill([]byte("data"), 1, 100,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 100) },
	)
	require.NoError(t, err)

	t.Log("Corrupting random leaf page.")
	victimPageId, validPageIds := corruptRandomLeafPage(t, db.DB)

	t.Log("Running consistency check.")
	vErr := db.View(func(tx *bbolt.Tx) error {
		var cErrs []error

		t.Log("Check corrupted page.")
		errChan := tx.Check(bbolt.WithPageId(uint(victimPageId)))
		for cErr := range errChan {
			cErrs = append(cErrs, cErr)
		}
		require.Greater(t, len(cErrs), 0)

		t.Log("Check valid pages.")
		cErrs = cErrs[:0]
		for _, pgId := range validPageIds {
			errChan = tx.Check(bbolt.WithPageId(uint(pgId)))
			for cErr := range errChan {
				cErrs = append(cErrs, cErr)
			}
			require.Equal(t, 0, len(cErrs))
		}
		return nil
	})
	require.NoError(t, vErr)
	t.Log("All check passed")

	// Manually close the db, otherwise the PostTestCleanup will
	// check the db again and accordingly fail the test.
	db.MustClose()
}

// corruptRandomLeafPage corrupts one random leaf page.
func corruptRandomLeafPage(t testing.TB, db *bbolt.DB) (victimPageId common.Pgid, validPageIds []common.Pgid) {
	victimPageId, validPageIds = pickupRandomLeafPage(t, db)
	victimPage, victimBuf, err := guts_cli.ReadPage(db.Path(), uint64(victimPageId))
	require.NoError(t, err)
	require.True(t, victimPage.IsLeafPage())
	require.True(t, victimPage.Count() > 1)

	// intentionally make the second key < the first key.
	element := victimPage.LeafPageElement(1)
	key := element.Key()
	key[0] = 0

	// Write the corrupt page to db file.
	err = guts_cli.WritePage(db.Path(), victimBuf)
	require.NoError(t, err)
	return victimPageId, validPageIds
}

// pickupRandomLeafPage picks up a random leaf page.
func pickupRandomLeafPage(t testing.TB, db *bbolt.DB) (victimPageId common.Pgid, validPageIds []common.Pgid) {
	// Read DB's RootPage, which should be a leaf page.
	rootPageId, _, err := guts_cli.GetRootPage(db.Path())
	require.NoError(t, err)
	rootPage, _, err := guts_cli.ReadPage(db.Path(), uint64(rootPageId))
	require.NoError(t, err)
	require.True(t, rootPage.IsLeafPage())

	// The leaf page contains only one item, namely the bucket
	require.Equal(t, uint16(1), rootPage.Count())
	lpe := rootPage.LeafPageElement(uint16(0))
	require.True(t, lpe.IsBucketEntry())

	// The bucket should be pointing to a branch page
	bucketRootPageId := lpe.Bucket().RootPage()
	bucketRootPage, _, err := guts_cli.ReadPage(db.Path(), uint64(bucketRootPageId))
	require.NoError(t, err)
	require.True(t, bucketRootPage.IsBranchPage())

	// Retrieve all the leaf pages included in the branch page, and pick up random one from them.
	var bucketPageIds []common.Pgid
	for _, bpe := range bucketRootPage.BranchPageElements() {
		bucketPageIds = append(bucketPageIds, bpe.Pgid())
	}
	randomIdx := rand.Intn(len(bucketPageIds))
	victimPageId = bucketPageIds[randomIdx]
	validPageIds = append(bucketPageIds[:randomIdx], bucketPageIds[randomIdx+1:]...)
	return victimPageId, validPageIds
}

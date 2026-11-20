package bbolt_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func TestTx_Check_CorruptPage(t *testing.T) {
	bucketName := []byte("data")

	t.Log("Creating db file.")
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})

	// Each page can hold roughly 20 key/values pair, so 100 such
	// key/value pairs will consume about 5 leaf pages.
	err := db.Fill(bucketName, 1, 100,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 100) },
	)
	require.NoError(t, err)

	t.Log("Corrupting a random leaf page.")
	victimPageId, validPageIds := corruptRandomLeafPageInBucket(t, db.DB, bucketName)

	t.Log("Running consistency check.")
	vErr := db.View(func(tx *bbolt.Tx) error {
		var cErrs []error

		t.Log("Check corrupted page.")
		errChan := tx.Check(bbolt.WithPageId(uint64(victimPageId)))
		for cErr := range errChan {
			cErrs = append(cErrs, cErr)
		}
		require.Greater(t, len(cErrs), 0)

		t.Log("Check valid pages.")
		cErrs = cErrs[:0]
		for _, pgId := range validPageIds {
			errChan = tx.Check(bbolt.WithPageId(uint64(pgId)))
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

func TestTx_Check_WithNestBucket(t *testing.T) {
	parentBucketName := []byte("parentBucket")

	t.Log("Creating db file.")
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})

	err := db.Update(func(tx *bbolt.Tx) error {
		pb, bErr := tx.CreateBucket(parentBucketName)
		if bErr != nil {
			return bErr
		}

		t.Log("put some key/values under the parent bucket directly")
		for i := 0; i < 10; i++ {
			k, v := fmt.Sprintf("%04d", i), fmt.Sprintf("value_%4d", i)
			if pErr := pb.Put([]byte(k), []byte(v)); pErr != nil {
				return pErr
			}
		}

		t.Log("create a nested bucket and put some key/values under the nested bucket")
		cb, bErr := pb.CreateBucket([]byte("nestedBucket"))
		if bErr != nil {
			return bErr
		}

		for i := 0; i < 2000; i++ {
			k, v := fmt.Sprintf("%04d", i), fmt.Sprintf("value_%4d", i)
			if pErr := cb.Put([]byte(k), []byte(v)); pErr != nil {
				return pErr
			}
		}

		return nil
	})
	require.NoError(t, err)

	// Get the bucket's root page.
	bucketRootPageId := mustGetBucketRootPage(t, db.DB, parentBucketName)

	t.Logf("Running consistency check starting from pageId: %d", bucketRootPageId)
	vErr := db.View(func(tx *bbolt.Tx) error {
		var cErrs []error

		errChan := tx.Check(bbolt.WithPageId(uint64(bucketRootPageId)))
		for cErr := range errChan {
			cErrs = append(cErrs, cErr)
		}
		require.Equal(t, 0, len(cErrs))

		return nil
	})
	require.NoError(t, vErr)
	t.Log("All check passed")

	// Manually close the db, otherwise the PostTestCleanup will
	// check the db again and accordingly fail the test.
	db.MustClose()
}

func TestTx_Check_Panic(t *testing.T) {
	bucketName := []byte("data")
	t.Log("Creating db file.")
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})

	// Each page can hold roughly 20 key/values pair, so 100 such
	// key/value pairs will consume about 5 leaf pages.
	err := db.Fill(bucketName, 1, 100,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 100) },
	)
	require.NoError(t, err)

	corruptRootPage(t, db.DB, bucketName)

	path := db.Path()

	require.NoError(t, db.Close())

	db = btesting.MustOpenDBWithOption(t, path, &bbolt.Options{PageSize: 4096})

	vErr := db.View(func(tx *bbolt.Tx) error {
		errChan := tx.Check()
		for cErr := range errChan {
			fmt.Println("cErr", cErr)
			return cErr
		}
		return nil
	})
	require.Error(t, vErr)
	require.ErrorContains(t, vErr, "has unexpected type/flags: 0")

	// Manually close the db, otherwise the PostTestCleanup will
	// check the db again and accordingly fail the test.
	db.MustClose()
}

// corruptRandomLeafPage corrupts one random leaf page.
func corruptRandomLeafPageInBucket(t testing.TB, db *bbolt.DB, bucketName []byte) (victimPageId common.Pgid, validPageIds []common.Pgid) {
	bucketRootPageId := mustGetBucketRootPage(t, db, bucketName)
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

func corruptRootPage(t testing.TB, db *bbolt.DB, bucketName []byte) {
	bucketRootPageId := mustGetBucketRootPage(t, db, bucketName)
	bucketRootPage, bucketRootPageBuf, err := guts_cli.ReadPage(db.Path(), uint64(bucketRootPageId))
	require.NoError(t, err)
	require.True(t, bucketRootPage.IsBranchPage())

	bucketRootPage.SetFlags(0)

	err = guts_cli.WritePage(db.Path(), bucketRootPageBuf)
	require.NoError(t, err)
}

// mustGetBucketRootPage returns the root page for the provided bucket.
func mustGetBucketRootPage(t testing.TB, db *bbolt.DB, bucketName []byte) common.Pgid {
	var rootPageId common.Pgid
	_ = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		require.NotNil(t, b)
		rootPageId = b.RootPage()
		return nil
	})

	return rootPageId
}

// TestTx_Check_OverflowPastHighWaterMark is a regression test for
// https://github.com/etcd-io/bbolt/issues/1319 (last row): a page whose
// declared overflow pushes its span past meta.pgid passed Tx.Check, because
// only the span's first page was bounds-checked and the unreachable-unfreed
// sweep only examines pages below the high-water mark. The tail page of such
// a span is also the next page the allocator hands out, so the file later
// dies with "page N already freed" on a live open.
func TestTx_Check_OverflowPastHighWaterMark(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "issue1319", "overflow_past_hwm.db"))
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "db")
	require.NoError(t, os.WriteFile(path, data, 0600))

	db, err := bbolt.Open(path, 0600, &bbolt.Options{PreLoadFreelist: true})
	require.NoError(t, err)
	defer db.Close()

	var errs []error
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		for err := range tx.Check() {
			errs = append(errs, err)
		}
		return nil
	}))
	require.NotEmpty(t, errs, "Tx.Check should flag a page whose overflow span passes the high-water mark")
	found := false
	for _, err := range errs {
		if strings.Contains(err.Error(), "overflow past high water mark") {
			found = true
		}
	}
	require.True(t, found, "expected an 'overflow past high water mark' error, got: %v", errs)
}

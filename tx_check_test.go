package bbolt_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func TestTx_Check_CorruptPage(t *testing.T) {
	bucketKey := "testBucket"

	t.Log("Creating db file.")
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: pageSize})
	defer func() {
		require.NoError(t, db.Close())
	}()

	uErr := db.Update(func(tx *bbolt.Tx) error {
		t.Logf("Creating bucket '%v'.", bucketKey)
		b, bErr := tx.CreateBucketIfNotExists([]byte(bucketKey))
		require.NoError(t, bErr)
		t.Logf("Generating random data in bucket '%v'.", bucketKey)
		generateSampleDataInBucket(t, b, pageSize, 3)
		return nil
	})
	require.NoError(t, uErr)

	t.Logf("Corrupting random leaf page in bucket '%v'.", bucketKey)
	victimPageId, validPageIds := corruptLeafPage(t, db.DB)

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
}

// corruptLeafPage write an invalid leafPageElement into the victim page.
func corruptLeafPage(t testing.TB, db *bbolt.DB) (victimPageId common.Pgid, validPageIds []common.Pgid) {
	t.Helper()
	victimPageId, validPageIds = findVictimPageId(t, db)
	victimPage, victimBuf, err := guts_cli.ReadPage(db.Path(), uint64(victimPageId))
	require.NoError(t, err)
	require.True(t, victimPage.IsLeafPage())
	require.True(t, victimPage.Count() > 0)
	// Dumping random bytes in victim page for corruption.
	copy(victimBuf[32:], generateCorruptionBytes(t))
	// Write the corrupt page to db file.
	err = guts_cli.WritePage(db.Path(), victimBuf)
	require.NoError(t, err)
	return victimPageId, validPageIds
}

// findVictimPageId finds all the leaf pages of a bucket and picks a random leaf page as a victim to be corrupted.
func findVictimPageId(t testing.TB, db *bbolt.DB) (victimPageId common.Pgid, validPageIds []common.Pgid) {
	t.Helper()
	// Read DB's RootPage.
	rootPageId, _, err := guts_cli.GetRootPage(db.Path())
	require.NoError(t, err)
	rootPage, _, err := guts_cli.ReadPage(db.Path(), uint64(rootPageId))
	require.NoError(t, err)
	require.True(t, rootPage.IsLeafPage())
	require.Equal(t, 1, len(rootPage.LeafPageElements()))
	// Find Bucket's RootPage.
	lpe := rootPage.LeafPageElement(uint16(0))
	require.Equal(t, uint32(common.BranchPageFlag), lpe.Flags())
	k := lpe.Key()
	require.Equal(t, "testBucket", string(k))
	bucketRootPageId := lpe.Bucket().RootPage()
	// Read Bucket's RootPage.
	bucketRootPage, _, err := guts_cli.ReadPage(db.Path(), uint64(bucketRootPageId))
	require.NoError(t, err)
	require.Equal(t, uint16(common.BranchPageFlag), bucketRootPage.Flags())
	// Retrieve Bucket's PageIds
	var bucketPageIds []common.Pgid
	for _, bpe := range bucketRootPage.BranchPageElements() {
		bucketPageIds = append(bucketPageIds, bpe.Pgid())
	}
	randomIdx := rand.Intn(len(bucketPageIds))
	victimPageId = bucketPageIds[randomIdx]
	validPageIds = append(bucketPageIds[:randomIdx], bucketPageIds[randomIdx+1:]...)
	return victimPageId, validPageIds
}

// generateSampleDataInBucket fill in sample data into given bucket to create the given
// number of leafPages. To control the number of leafPages, sample data are generated in order.
func generateSampleDataInBucket(t testing.TB, bk *bbolt.Bucket, pageSize int, lPages int) {
	t.Helper()
	maxBytesInPage := int(bk.FillPercent * float64(pageSize))
	currentKey := 1
	currentVal := 100
	for i := 0; i < lPages; i++ {
		currentSize := common.PageHeaderSize
		for {
			err := bk.Put([]byte(fmt.Sprintf("key_%d", currentKey)), []byte(fmt.Sprintf("val_%d", currentVal)))
			require.NoError(t, err)
			currentSize += common.LeafPageElementSize + unsafe.Sizeof(currentKey) + unsafe.Sizeof(currentVal)
			if int(currentSize) >= maxBytesInPage {
				break
			}
			currentKey++
			currentVal++
		}
	}
}

// generateCorruptionBytes returns random bytes to corrupt a page.
// It inserts a page element which violates the btree key order if no panic is expected.
func generateCorruptionBytes(t testing.TB) []byte {
	t.Helper()
	invalidLPE := common.NewLeafPageElement(0, 0, 0, 0)
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, invalidLPE)
	require.NoError(t, err)
	return buf.Bytes()
}

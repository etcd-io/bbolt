package main_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	. "go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func TestSurgery_RevertMetaPage(t *testing.T) {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize})
	srcPath := db.Path()

	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	srcFile, err := os.Open(srcPath)
	require.NoError(t, err)
	defer srcFile.Close()

	// Read both meta0 and meta1 from srcFile
	srcBuf0 := readPage(t, srcPath, 0, pageSize)
	srcBuf1 := readPage(t, srcPath, 1, pageSize)
	meta0Page := LoadPageMeta(srcBuf0)
	meta1Page := LoadPageMeta(srcBuf1)

	// Get the non-active meta page
	nonActiveSrcBuf := srcBuf0
	nonActiveMetaPageId := 0
	if meta0Page.Txid() > meta1Page.Txid() {
		nonActiveSrcBuf = srcBuf1
		nonActiveMetaPageId = 1
	}
	t.Logf("non active meta page id: %d", nonActiveMetaPageId)

	// revert the meta page
	dstPath := filepath.Join(t.TempDir(), "dstdb")
	m := NewMain()
	err = m.Run("surgery", "revert-meta-page", srcPath, dstPath)
	require.NoError(t, err)

	// read both meta0 and meta1 from dst file
	dstBuf0 := readPage(t, dstPath, 0, pageSize)
	dstBuf1 := readPage(t, dstPath, 1, pageSize)

	// check result. Note we should skip the page ID
	assert.Equal(t, pageDataWithoutPageId(nonActiveSrcBuf), pageDataWithoutPageId(dstBuf0))
	assert.Equal(t, pageDataWithoutPageId(nonActiveSrcBuf), pageDataWithoutPageId(dstBuf1))
}

func TestSurgery_CopyPage(t *testing.T) {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize})
	srcPath := db.Path()

	// Insert some sample data
	t.Log("Insert some sample data")
	err := db.Fill([]byte("data"), 1, 20,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 10) },
	)
	require.NoError(t, err)

	defer requireDBNoChange(t, dbData(t, srcPath), srcPath)

	// copy page 3 to page 2
	t.Log("copy page 3 to page 2")
	dstPath := filepath.Join(t.TempDir(), "dstdb")
	m := NewMain()
	err = m.Run("surgery", "copy-page", srcPath, dstPath, "3", "2")
	require.NoError(t, err)

	// The page 2 should have exactly the same data as page 3.
	t.Log("Verify result")
	srcPageId3Data := readPage(t, srcPath, 3, pageSize)
	dstPageId3Data := readPage(t, dstPath, 3, pageSize)
	dstPageId2Data := readPage(t, dstPath, 2, pageSize)

	assert.Equal(t, srcPageId3Data, dstPageId3Data)
	assert.Equal(t, pageDataWithoutPageId(srcPageId3Data), pageDataWithoutPageId(dstPageId2Data))
}

func TestSurgery_ClearPage_Branch(t *testing.T) {
	testClearElements(t, true, true)
}

func TestSurgery_ClearPage_Leaf(t *testing.T) {
	testClearElements(t, false, true)
}

func TestSurgery_ClearElements_Branch(t *testing.T) {
	testClearElements(t, true, false)
}

func TestSurgery_ClearElements_Leaf(t *testing.T) {
	testClearElements(t, false, false)
}

func testClearElements(t *testing.T, isBranch, isClear bool) {
	srcPath := populateData(t, 5, 100)
	defer requireDBNoChange(t, dbData(t, srcPath), srcPath)

	var (
		targetPageId Pgid
		keepCount    uint16
	)
	branchPageId, leafPageId := getBranchAndLeafPageIDs(t, srcPath)
	targetPageId = leafPageId
	if isBranch {
		targetPageId = branchPageId
	}

	dstPath := filepath.Join(t.TempDir(), "dstdb")
	m := NewMain()
	if isClear {
		err := m.Run("surgery", "clear-page", srcPath, dstPath, fmt.Sprintf("%d", targetPageId))
		require.NoError(t, err)
	} else {
		targetPage, _, err := guts_cli.ReadPage(srcPath, uint64(targetPageId))
		require.NoError(t, err)
		keepCount = targetPage.Count() / 2

		err = m.Run("surgery", "clear-elements", srcPath, dstPath, fmt.Sprintf("%d", targetPageId), fmt.Sprintf("%d", keepCount))
		require.NoError(t, err)
	}

	checkTestClearElementResult(t, srcPath, dstPath, targetPageId, keepCount)
}

func checkTestClearElementResult(t *testing.T, srcPath, dstPath string, pageId Pgid, keepCount uint16) {
	srcPage, _, err := guts_cli.ReadPage(srcPath, uint64(pageId))
	require.NoError(t, err)
	dstPage, _, err := guts_cli.ReadPage(dstPath, uint64(pageId))
	require.NoError(t, err)
	assert.Equal(t, keepCount, dstPage.Count())
	assert.Equal(t, uint32(0), dstPage.Overflow())

	clearCount := srcPage.Count() - keepCount
	srcFreePageCount := getFreePageCount(t, srcPath)
	dstFreePageCount := getFreePageCount(t, dstPath)
	t.Logf("srcFreePageCount = %d, dstFreePageCount = %d, clearCount = %d", srcFreePageCount, dstFreePageCount, clearCount)
	// TODO(ahrtr): current surgeon doesn't sync freelist, so this assertion always fails.
	// We should fix surgeon package.
	/*
		if srcPage.Type() == "branch" {
			assert.Equal(t, clearCount, dstFreePageCount-srcFreePageCount)
		}
	*/
}

// We intentionally created a db like below,
/*
$ bbolt page db 1
Page ID:    1
Page Type:  meta
Total Size: 4096 bytes
Overflow pages: 0
Version:    2
Page Size:  4096 bytes
Flags:      00000000
Root:       <pgid=14>
Freelist:   <pgid=7>
HWM:        <pgid=16>
Txn ID:     7
Checksum:   7b10600340cb9b95

$ bbolt page db 14
Page ID:    14
Page Type:  leaf
Total Size: 4096 bytes
Overflow pages: 0
Item Count: 1

"data": <pgid=12,seq=0>

$ bbolt page db 12
Page ID:    12
Page Type:  branch
Total Size: 4096 bytes
Overflow pages: 0
Item Count: 7

"0000": <pgid=2>
"0067": <pgid=4>
"0134": <pgid=5>
"0201": <pgid=3>
"0268": <pgid=6>
"0335": <pgid=10>
"0402": <pgid=11>
*/
func populateData(t *testing.T, numTx, numKeysPerTx int) string {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize})

	t.Logf("Populate sample data, numTx = %d, numKeysPerTx = %d", numTx, numKeysPerTx)
	err := db.Fill([]byte("data"), numTx, numKeysPerTx,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", tx*100+k)) },
		func(tx int, k int) []byte { return make([]byte, 10) },
	)
	require.NoError(t, err)

	db.MustClose()
	return db.Path()
}

func getBranchAndLeafPageIDs(t *testing.T, dbPath string) (Pgid, Pgid) {
	root, _, err := guts_cli.GetRootPage(dbPath)
	require.NoError(t, err)
	rootPage, _, err := guts_cli.ReadPage(dbPath, uint64(root))
	require.NoError(t, err)

	e1 := rootPage.LeafPageElement(0)
	b := e1.Bucket()
	branchPageId := b.RootPage()

	branchPage, _, err := guts_cli.ReadPage(dbPath, uint64(branchPageId))
	require.NoError(t, err)
	e2 := branchPage.BranchPageElement(0)
	leafPageId := e2.Pgid()

	return branchPageId, leafPageId
}

func getFreePageCount(t *testing.T, dbPath string) int {
	_, metaPageId, err := guts_cli.GetRootPage(dbPath)
	require.NoError(t, err)
	_, buf, err := guts_cli.ReadPage(dbPath, uint64(metaPageId))
	require.NoError(t, err)

	meta := LoadPageMeta(buf)
	freelistPageId := meta.Freelist()
	freelistPage, _, err := guts_cli.ReadPage(dbPath, uint64(freelistPageId))
	require.NoError(t, err)

	_, cnt := freelistPage.FreelistPageCount()
	return cnt
}

func readPage(t *testing.T, path string, pageId int, pageSize int) []byte {
	dbFile, err := os.Open(path)
	require.NoError(t, err)
	defer dbFile.Close()

	fi, err := dbFile.Stat()
	require.NoError(t, err)
	require.GreaterOrEqual(t, fi.Size(), int64((pageId+1)*pageSize))

	buf := make([]byte, pageSize)
	byteRead, err := dbFile.ReadAt(buf, int64(pageId*pageSize))
	require.NoError(t, err)
	require.Equal(t, pageSize, byteRead)

	return buf
}

func pageDataWithoutPageId(buf []byte) []byte {
	return buf[8:]
}

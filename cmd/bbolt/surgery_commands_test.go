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
	"go.etcd.io/bbolt/internal/common"
)

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

// TODO(ahrtr): add test case below for `surgery clear-page` command:
//  1. The page is a branch page. All its children should become free pages.
func TestSurgery_ClearPage(t *testing.T) {
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

	// clear page 3
	t.Log("clear page 3")
	dstPath := filepath.Join(t.TempDir(), "dstdb")
	m := NewMain()
	err = m.Run("surgery", "clear-page", srcPath, dstPath, "3")
	require.NoError(t, err)

	// The page 2 should have exactly the same data as page 3.
	t.Log("Verify result")
	dstPageId3Data := readPage(t, dstPath, 3, pageSize)

	p := common.LoadPage(dstPageId3Data)
	assert.Equal(t, uint16(0), p.Count())
	assert.Equal(t, uint32(0), p.Overflow())
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

package main_test

import (
	bolt "go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func TestSurgery_RevertMetaPage(t *testing.T) {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize})
	srcPath := db.Path()

	srcFile, err := os.Open(srcPath)
	require.NoError(t, err)
	defer srcFile.Close()

	// Read both meta0 and meta1 from srcFile
	srcBuf0, srcBuf1 := readBothMetaPages(t, srcPath, pageSize)
	meta0Page := guts_cli.LoadPageMeta(srcBuf0)
	meta1Page := guts_cli.LoadPageMeta(srcBuf1)

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
	dstBuf0, dstBuf1 := readBothMetaPages(t, dstPath, pageSize)

	// check result. Note we should skip the page ID
	assert.Equal(t, pageDataWithoutPageId(nonActiveSrcBuf), pageDataWithoutPageId(dstBuf0))
	assert.Equal(t, pageDataWithoutPageId(nonActiveSrcBuf), pageDataWithoutPageId(dstBuf1))
}

func pageDataWithoutPageId(buf []byte) []byte {
	return buf[8:]
}

func readBothMetaPages(t *testing.T, filePath string, pageSize int) ([]byte, []byte) {
	dbFile, err := os.Open(filePath)
	require.NoError(t, err)
	defer dbFile.Close()

	buf0 := make([]byte, pageSize)
	buf1 := make([]byte, pageSize)

	meta0Len, err := dbFile.ReadAt(buf0, 0)
	require.NoError(t, err)
	require.Equal(t, pageSize, meta0Len)

	meta1Len, err := dbFile.ReadAt(buf1, int64(pageSize))
	require.NoError(t, err)
	require.Equal(t, pageSize, meta1Len)

	return buf0, buf1
}

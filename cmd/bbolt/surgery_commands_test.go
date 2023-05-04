package main_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
)

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

package command_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

// TestPageItemCommand_CorruptLeafElement checks that page-item reports a corrupt
// leaf element as an error instead of crashing the process.
//
// A leaf element's key/value size is read straight from the file and used to
// slice an unsafe pointer in (*leafPageElement).Key/Value. A corrupt ksize makes
// the slice bound exceed MaxAllocSize and panics. The sibling "page" command
// already wraps its per-page printing in a recover and continues; "page-item"
// does not, so the same corrupt database (exactly the input this CLI exists to
// inspect) takes the whole process down.
func TestPageItemCommand_CorruptLeafElement(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	srcPath := db.Path()

	err := db.Update(func(tx *bolt.Tx) error {
		b, bErr := tx.CreateBucketIfNotExists([]byte("data"))
		if bErr != nil {
			return bErr
		}
		for i := 0; i < 100; i++ {
			if bErr = b.Put([]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("value_%d", i))); bErr != nil {
				return bErr
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Find a populated leaf page.
	meta := readMetaPage(t, srcPath)
	leafPageID := 0
	for i := 2; i < int(meta.Pgid()); i++ {
		p, _, rErr := guts_cli.ReadPage(srcPath, uint64(i))
		require.NoError(t, rErr)
		if p.IsLeafPage() && p.Count() > 1 {
			leafPageID = int(p.Id())
			break
		}
	}
	require.NotEqual(t, 0, leafPageID)

	// Corrupt element 0 so Key() slices past MaxAllocSize.
	p, buf, err := guts_cli.ReadPage(srcPath, uint64(leafPageID))
	require.NoError(t, err)
	require.Positive(t, p.Count())
	common.LoadPage(buf).LeafPageElement(0).SetKsize(0xFFFFFF00)
	require.NoError(t, guts_cli.WritePage(srcPath, buf))

	// page-item must return an error, not panic.
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"page-item", srcPath, fmt.Sprintf("%d", leafPageID), "0"})
	require.Error(t, rootCmd.Execute())
}

package guts_cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

// Ensure that a Page claiming more overflow pages than the file can hold is
// rejected even when the meta Page reports a high-water-mark below 3.
func TestReadPage_OverflowGuardUnderflow(t *testing.T) {
	const pageSize = 4096
	const overflowN = 50

	// Meta (Page 0), unused meta (Page 1), and the target Page 2.
	buf := make([]byte, pageSize*3)

	meta := common.LoadPageMeta(buf[0:pageSize])
	meta.SetMagic(common.Magic)
	meta.SetVersion(common.Version)
	meta.SetPageSize(pageSize)
	meta.SetPgid(2) // high-water-mark below 3

	mp := common.LoadPage(buf[0:pageSize])
	mp.SetId(0)
	mp.SetFlags(common.MetaPageFlag)

	// Claim more overflow pages than the three Page file can hold.
	p := common.LoadPage(buf[2*pageSize : 3*pageSize])
	p.SetId(2)
	p.SetFlags(common.LeafPageFlag)
	p.SetCount(0)
	p.SetOverflow(overflowN)

	path := filepath.Join(t.TempDir(), "corrupt.db")
	require.NoError(t, os.WriteFile(path, buf, 0o600))

	_, _, err := ReadPage(path, 2)
	require.ErrorIs(t, err, ErrCorrupt)
	require.ErrorContains(t, err, "Interrupting to avoid risky OOM")
}

package bbolt

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt/internal/common"
)

// TestLoadFreelistRejectsInvalidEntries is a regression test for
// https://github.com/etcd-io/bbolt/issues/1319: a corrupt freelist was
// accepted at open and only died later, mid-commit, with a panic
// ("invalid page allocation: 0", "page N already freed", or
// "freelist pgid (N) above high water mark"). Open now fails with a
// clear error instead. Fixtures are the reproducer databases attached
// to the issue.
func TestLoadFreelistRejectsInvalidEntries(t *testing.T) {
	for _, tc := range []struct {
		fixture string
		want    string
	}{
		{"bbolt_freelist.go_117.db", "freelist contains invalid page id 0"},
		{"bbolt_freelist.go_175.db", "freelist contains invalid page id 0"},
		{"bbolt_node.go_375.db", "freelist page id 8960 is at or above the high-water mark 4"},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "issue1319", tc.fixture))
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "db")
			require.NoError(t, os.WriteFile(path, data, 0o600))
			_, err = Open(path, 0o600, &Options{PreLoadFreelist: true})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

// TestValidateFreelistRejectsDuplicateEntries covers the duplicate-id
// branch of validateFreelist, which the issue fixtures do not reach:
// their sorted ids trip the invalid-id check first.
func TestValidateFreelistRejectsDuplicateEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, 0o600, nil)
	require.NoError(t, err)
	defer db.Close()

	// A fresh database has meta pages 0/1, freelist page 2 and root
	// page 3, so 2 is a plausible free page id below the high-water
	// mark; listing it twice is the corruption under test.
	db.freelist.Init(common.Pgids{2, 2})
	require.EqualError(t, db.validateFreelist(), "freelist contains duplicate page id 2")
}

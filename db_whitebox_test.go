package bbolt

import (
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

func TestOpenWithPreLoadFreelist(t *testing.T) {
	testCases := []struct {
		name                    string
		readonly                bool
		preLoadFreePage         bool
		expectedFreePagesLoaded bool
	}{
		{
			name:                    "write mode always load free pages",
			readonly:                false,
			preLoadFreePage:         false,
			expectedFreePagesLoaded: true,
		},
		{
			name:                    "readonly mode load free pages when flag set",
			readonly:                true,
			preLoadFreePage:         true,
			expectedFreePagesLoaded: true,
		},
		{
			name:                    "readonly mode doesn't load free pages when flag not set",
			readonly:                true,
			preLoadFreePage:         false,
			expectedFreePagesLoaded: false,
		},
	}

	fileName, err := prepareData(t)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(fileName, 0666, &Options{
				ReadOnly:        tc.readonly,
				PreLoadFreelist: tc.preLoadFreePage,
			})
			require.NoError(t, err)

			assert.Equal(t, tc.expectedFreePagesLoaded, db.freelist != nil)

			assert.NoError(t, db.Close())
		})
	}
}

func TestMethodPage(t *testing.T) {
	testCases := []struct {
		name            string
		readonly        bool
		preLoadFreePage bool
		expectedError   error
	}{
		{
			name:            "write mode",
			readonly:        false,
			preLoadFreePage: false,
			expectedError:   nil,
		},
		{
			name:            "readonly mode with preloading free pages",
			readonly:        true,
			preLoadFreePage: true,
			expectedError:   nil,
		},
		{
			name:            "readonly mode without preloading free pages",
			readonly:        true,
			preLoadFreePage: false,
			expectedError:   errors.ErrFreePagesNotLoaded,
		},
	}

	fileName, err := prepareData(t)
	require.NoError(t, err)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(fileName, 0666, &Options{
				ReadOnly:        tc.readonly,
				PreLoadFreelist: tc.preLoadFreePage,
			})
			require.NoError(t, err)
			defer db.Close()

			tx, err := db.Begin(!tc.readonly)
			require.NoError(t, err)

			_, err = tx.Page(0)
			require.Equal(t, tc.expectedError, err)

			if tc.readonly {
				require.NoError(t, tx.Rollback())
			} else {
				require.NoError(t, tx.Commit())
			}

			require.NoError(t, db.Close())
		})
	}
}

func prepareData(t *testing.T) (string, error) {
	fileName := filepath.Join(t.TempDir(), "db")
	db, err := Open(fileName, 0666, nil)
	if err != nil {
		return "", err
	}
	if err := db.Close(); err != nil {
		return "", err
	}

	return fileName, nil
}

func TestPrefetchRange(t *testing.T) {
	const defaultMmapSize = 1 << 20

	// The last page of the largest mmap possible. Rounding its end up to
	// the next OS page doesn't fit into an int.
	const lastPage = math.MaxInt/4096 - 1

	testCases := []struct {
		name        string
		first, last common.Pgid
		pageSize    int
		osPageSize  int
		mmapSize    int

		expectedStart, expectedEnd int
		expectedOK                 bool
	}{
		{
			name:  "single page",
			first: 3, last: 3, pageSize: 4096, osPageSize: 4096,
			expectedStart: 3 * 4096, expectedEnd: 4 * 4096, expectedOK: true,
		},
		{
			name:  "multiple pages",
			first: 3, last: 7, pageSize: 4096, osPageSize: 4096,
			expectedStart: 3 * 4096, expectedEnd: 8 * 4096, expectedOK: true,
		},
		{
			name:  "pages smaller than OS pages",
			first: 5, last: 6, pageSize: 1024, osPageSize: 4096,
			expectedStart: 4096, expectedEnd: 8192, expectedOK: true,
		},
		{
			name:  "pages smaller than OS pages across a boundary",
			first: 7, last: 8, pageSize: 1024, osPageSize: 4096,
			expectedStart: 4096, expectedEnd: 3 * 4096, expectedOK: true,
		},
		{
			name:  "pages larger than OS pages",
			first: 1, last: 1, pageSize: 16384, osPageSize: 4096,
			expectedStart: 16384, expectedEnd: 2 * 16384, expectedOK: true,
		},
		{
			name:  "last page of the mmap",
			first: 255, last: 255, pageSize: 4096, osPageSize: 16384,
			expectedStart: 252 * 4096, expectedEnd: defaultMmapSize, expectedOK: true,
		},
		{
			name:  "last page of the largest mmap",
			first: lastPage, last: lastPage, pageSize: 4096, osPageSize: 16384,
			mmapSize:      math.MaxInt,
			expectedStart: lastPage*4096 - lastPage*4096%16384, expectedEnd: math.MaxInt, expectedOK: true,
		},
		{
			name:  "beyond the mmap",
			first: 255, last: 256, pageSize: 4096, osPageSize: 4096,
		},
		{
			name:  "page id overflowing the offset",
			first: 1, last: common.Pgid(math.MaxUint64), pageSize: 4096, osPageSize: 4096,
		},
		{
			name:  "empty range",
			first: 4, last: 3, pageSize: 4096, osPageSize: 4096,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mmapSize := tc.mmapSize
			if mmapSize == 0 {
				mmapSize = defaultMmapSize
			}

			start, end, ok := prefetchRange(tc.first, tc.last, tc.pageSize, tc.osPageSize, mmapSize)
			require.Equal(t, tc.expectedOK, ok)
			require.Equal(t, tc.expectedStart, start)
			require.Equal(t, tc.expectedEnd, end)
		})
	}
}

func TestForEachOverflowRead(t *testing.T) {
	const pageSize = 4096

	type pageRange struct{ first, last common.Pgid }

	bucket := make([]byte, common.BucketHeaderSize)

	testCases := []struct {
		name     string
		flags    uint16
		overflow uint32
		count    uint16
		inodes   []struct {
			flags      uint32
			key, value []byte
		}
		expected []pageRange
	}{
		{
			name:  "leaf page without overflow",
			flags: common.LeafPageFlag,
			inodes: []struct {
				flags      uint32
				key, value []byte
			}{
				{key: []byte("a"), value: make([]byte, 100)},
				{key: []byte("b"), value: make([]byte, 100)},
			},
		},
		{
			// The second key starts on the second page and the third
			// one on the third page. The last two pages only hold the
			// value of the third key.
			name:     "leaf page with overflow",
			flags:    common.LeafPageFlag,
			overflow: 4,
			inodes: []struct {
				flags      uint32
				key, value []byte
			}{
				{key: []byte("a"), value: make([]byte, 6000)},
				{key: []byte("b"), value: make([]byte, 5000)},
				{key: []byte("c"), value: make([]byte, 9000)},
			},
			expected: []pageRange{{11, 11}, {12, 12}},
		},
		{
			// Both the second key and the bucket are on the second
			// page, which must be reported only once.
			name:     "leaf page with keys sharing an overflow page",
			flags:    common.LeafPageFlag,
			overflow: 1,
			inodes: []struct {
				flags      uint32
				key, value []byte
			}{
				{key: []byte("a"), value: make([]byte, 6000)},
				{key: []byte("b"), value: make([]byte, 100)},
				{flags: common.BucketLeafFlag, key: []byte("c"), value: bucket},
			},
			expected: []pageRange{{11, 11}},
		},
		{
			// The elements of a page with that count would take up
			// more than its two pages.
			name:     "leaf page with a corrupt count",
			flags:    common.LeafPageFlag,
			overflow: 1,
			count:    math.MaxUint16,
			inodes: []struct {
				flags      uint32
				key, value []byte
			}{
				{key: []byte("a"), value: make([]byte, 6000)},
				{key: []byte("b"), value: make([]byte, 100)},
			},
		},
		{
			name:     "branch page with overflow",
			flags:    common.BranchPageFlag,
			overflow: 2,
			inodes: []struct {
				flags      uint32
				key, value []byte
			}{
				{key: make([]byte, 5000)},
				{key: make([]byte, 5000)},
			},
			expected: []pageRange{{11, 12}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, (int(tc.overflow)+1)*pageSize)
			p := common.LoadPage(buf)
			p.SetId(10)
			p.SetFlags(tc.flags)
			p.SetCount(uint16(len(tc.inodes)))
			p.SetOverflow(tc.overflow)

			inodes := make(common.Inodes, len(tc.inodes))
			for i, in := range tc.inodes {
				inodes[i].SetFlags(in.flags)
				inodes[i].SetKey(in.key)
				inodes[i].SetValue(in.value)
			}
			common.WriteInodeToPage(inodes, p)
			if tc.count != 0 {
				p.SetCount(tc.count)
			}

			var got []pageRange
			forEachOverflowRead(p, pageSize, func(first, last common.Pgid) {
				got = append(got, pageRange{first, last})
			})
			require.Equal(t, tc.expected, got)
		})
	}
}

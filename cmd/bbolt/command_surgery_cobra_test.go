package main_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func TestSurgery_ClearPageElements_Without_Overflow(t *testing.T) {
	testCases := []struct {
		name                 string
		from                 int
		to                   int
		isBranchPage         bool
		setEndIdxAsCount     bool
		removeOnlyOneElement bool // only valid when setEndIdxAsCount == true, and startIdx = endIdx -1 in this case.
		expectError          bool
	}{
		// normal range in leaf page
		{
			name: "normal range in leaf page: [4, 8)",
			from: 4,
			to:   8,
		},
		{
			name: "normal range in leaf page: [5, -1)",
			from: 4,
			to:   -1,
		},
		{
			name: "normal range in leaf page: all",
			from: 0,
			to:   -1,
		},
		{
			name: "normal range in leaf page: [0, 7)",
			from: 0,
			to:   7,
		},
		{
			name:             "normal range in leaf page: [3, count)",
			from:             4,
			setEndIdxAsCount: true,
		},
		// normal range in branch page
		{
			name:         "normal range in branch page: [4, 8)",
			from:         4,
			to:           8,
			isBranchPage: true,
		},
		{
			name:         "normal range in branch page: [5, -1)",
			from:         4,
			to:           -1,
			isBranchPage: true,
		},
		{
			name:         "normal range in branch page: all",
			from:         0,
			to:           -1,
			isBranchPage: true,
		},
		{
			name:         "normal range in branch page: [0, 7)",
			from:         0,
			to:           7,
			isBranchPage: true,
		},
		{
			name:             "normal range in branch page: [3, count)",
			from:             4,
			isBranchPage:     true,
			setEndIdxAsCount: true,
		},
		// remove only one element
		{
			name: "one element: the first one",
			from: 0,
			to:   1,
		},
		{
			name: "one element: [6, 7)",
			from: 6,
			to:   7,
		},
		{
			name:                 "one element: the last one",
			setEndIdxAsCount:     true,
			removeOnlyOneElement: true,
		},
		// abnormal range
		{
			name:        "abnormal range: [-1, 4)",
			from:        -1,
			to:          4,
			expectError: true,
		},
		{
			name:        "abnormal range: [-2, 5)",
			from:        -1,
			to:          5,
			expectError: true,
		},
		{
			name:        "abnormal range: [3, 3)",
			from:        3,
			to:          3,
			expectError: true,
		},
		{
			name:        "abnormal range: [5, 3)",
			from:        5,
			to:          3,
			expectError: true,
		},
		{
			name:        "abnormal range: [3, -2)",
			from:        3,
			to:          -2,
			expectError: true,
		},
		{
			name:        "abnormal range: [3, 1000000)",
			from:        -1,
			to:          4,
			expectError: true,
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testSurgeryClearPageElementsWithoutOverflow(t, tc.from, tc.to, tc.isBranchPage, tc.setEndIdxAsCount, tc.removeOnlyOneElement, tc.expectError)
		})
	}
}

func testSurgeryClearPageElementsWithoutOverflow(t *testing.T, startIdx, endIdx int, isBranchPage, setEndIdxAsCount, removeOnlyOne, expectError bool) {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize})
	srcPath := db.Path()

	// Generate sample db
	t.Log("Generate some sample data")
	err := db.Fill([]byte("data"), 10, 200,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", tx*10000+k)) },
		func(tx int, k int) []byte { return make([]byte, 10) },
	)
	require.NoError(t, err)

	defer requireDBNoChange(t, dbData(t, srcPath), srcPath)

	// find a page with at least 10 elements
	var (
		pageId       uint64 = 2
		elementCount uint16 = 0
	)
	for {
		p, _, err := guts_cli.ReadPage(srcPath, pageId)
		require.NoError(t, err)

		if isBranchPage {
			if p.IsBranchPage() && p.Count() > 10 {
				elementCount = p.Count()
				break
			}
		} else {
			if p.IsLeafPage() && p.Count() > 10 {
				elementCount = p.Count()
				break
			}
		}
		pageId++
	}
	t.Logf("The original element count: %d", elementCount)

	if setEndIdxAsCount {
		t.Logf("Set the endIdx as the element count: %d", elementCount)
		endIdx = int(elementCount)
		if removeOnlyOne {
			startIdx = endIdx - 1
			t.Logf("Set the startIdx as the endIdx-1: %d", startIdx)
		}
	}

	// clear elements [startIdx, endIdx) in the page
	rootCmd := main.NewRootCommand()
	output := filepath.Join(t.TempDir(), "db")
	rootCmd.SetArgs([]string{
		"surgery", "clear-page-elements", srcPath,
		"--output", output,
		"--pageId", fmt.Sprintf("%d", pageId),
		"--from-index", fmt.Sprintf("%d", startIdx),
		"--to-index", fmt.Sprintf("%d", endIdx),
	})
	err = rootCmd.Execute()
	if expectError {
		require.Error(t, err)
		return
	}

	require.NoError(t, err)

	// check the element count again
	expectedCnt := 0
	if endIdx == -1 {
		expectedCnt = startIdx
	} else {
		expectedCnt = int(elementCount) - (endIdx - startIdx)
	}
	p, _, err := guts_cli.ReadPage(output, pageId)
	require.NoError(t, err)
	assert.Equal(t, expectedCnt, int(p.Count()))

	compareDataAfterClearingElement(t, srcPath, output, pageId, isBranchPage, startIdx, endIdx)
}

func compareDataAfterClearingElement(t *testing.T, srcPath, dstPath string, pageId uint64, isBranchPage bool, startIdx, endIdx int) {
	srcPage, _, err := guts_cli.ReadPage(srcPath, pageId)
	require.NoError(t, err)

	dstPage, _, err := guts_cli.ReadPage(dstPath, pageId)
	require.NoError(t, err)

	var dstIdx uint16
	for i := uint16(0); i < srcPage.Count(); i++ {
		// skip the cleared elements
		if dstIdx >= uint16(startIdx) && (dstIdx < uint16(endIdx) || endIdx == -1) {
			continue
		}

		if isBranchPage {
			srcElement := srcPage.BranchPageElement(i)
			dstElement := dstPage.BranchPageElement(dstIdx)

			require.Equal(t, srcElement.Key(), dstElement.Key())
			require.Equal(t, srcElement.Pgid(), dstElement.Pgid())
		} else {
			srcElement := srcPage.LeafPageElement(i)
			dstElement := dstPage.LeafPageElement(dstIdx)

			require.Equal(t, srcElement.Flags(), dstElement.Flags())
			require.Equal(t, srcElement.Key(), dstElement.Key())
			require.Equal(t, srcElement.Value(), dstElement.Value())
		}

		dstIdx++
	}
}

func TestSurgery_ClearPageElements_With_Overflow(t *testing.T) {
	testCases := []struct {
		name             string
		from             int
		to               int
		valueSizes       []int
		expectedOverflow int
	}{
		// big element
		{
			name:             "remove a big element at the end",
			valueSizes:       []int{500, 500, 500, 2600},
			from:             3,
			to:               4,
			expectedOverflow: 0,
		},
		{
			name:             "remove a big element at the begin",
			valueSizes:       []int{2600, 500, 500, 500},
			from:             0,
			to:               1,
			expectedOverflow: 0,
		},
		{
			name:             "remove a big element in the middle",
			valueSizes:       []int{500, 2600, 500, 500},
			from:             1,
			to:               2,
			expectedOverflow: 0,
		},
		// small element
		{
			name:             "remove a small element at the end",
			valueSizes:       []int{500, 500, 3100, 100},
			from:             3,
			to:               4,
			expectedOverflow: 1,
		},
		{
			name:             "remove a small element at the begin",
			valueSizes:       []int{100, 500, 3100, 500},
			from:             0,
			to:               1,
			expectedOverflow: 1,
		},
		{
			name:             "remove a small element in the middle",
			valueSizes:       []int{500, 100, 3100, 500},
			from:             1,
			to:               2,
			expectedOverflow: 1,
		},
		{
			name:             "remove a small element at the end of page with big overflow",
			valueSizes:       []int{500, 500, 4096 * 5, 100},
			from:             3,
			to:               4,
			expectedOverflow: 5,
		},
		{
			name:             "remove a small element at the begin of page with big overflow",
			valueSizes:       []int{100, 500, 4096 * 6, 500},
			from:             0,
			to:               1,
			expectedOverflow: 6,
		},
		{
			name:             "remove a small element in the middle of page with big overflow",
			valueSizes:       []int{500, 100, 4096 * 4, 500},
			from:             1,
			to:               2,
			expectedOverflow: 4,
		},
		// huge element
		{
			name:             "remove a huge element at the end",
			valueSizes:       []int{500, 500, 500, 4096 * 5},
			from:             3,
			to:               4,
			expectedOverflow: 0,
		},
		{
			name:             "remove a huge element at the begin",
			valueSizes:       []int{4096 * 5, 500, 500, 500},
			from:             0,
			to:               1,
			expectedOverflow: 0,
		},
		{
			name:             "remove a huge element in the middle",
			valueSizes:       []int{500, 4096 * 5, 500, 500},
			from:             1,
			to:               2,
			expectedOverflow: 0,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			testSurgeryClearPageElementsWithOverflow(t, tc.from, tc.to, tc.valueSizes, tc.expectedOverflow)
		})
	}
}

func testSurgeryClearPageElementsWithOverflow(t *testing.T, startIdx, endIdx int, valueSizes []int, expectedOverflow int) {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize})
	srcPath := db.Path()

	// Generate sample db
	err := db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("data"))
		for i, valueSize := range valueSizes {
			key := []byte(fmt.Sprintf("%04d", i))
			val := make([]byte, valueSize)
			if putErr := b.Put(key, val); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	require.NoError(t, err)

	defer requireDBNoChange(t, dbData(t, srcPath), srcPath)

	// find a page with overflow pages
	var (
		pageId       uint64 = 2
		elementCount uint16 = 0
	)
	for {
		p, _, err := guts_cli.ReadPage(srcPath, pageId)
		require.NoError(t, err)

		if p.Overflow() > 0 {
			elementCount = p.Count()
			break
		}
		pageId++
	}
	t.Logf("The original element count: %d", elementCount)

	// clear elements [startIdx, endIdx) in the page
	rootCmd := main.NewRootCommand()
	output := filepath.Join(t.TempDir(), "db")
	rootCmd.SetArgs([]string{
		"surgery", "clear-page-elements", srcPath,
		"--output", output,
		"--pageId", fmt.Sprintf("%d", pageId),
		"--from-index", fmt.Sprintf("%d", startIdx),
		"--to-index", fmt.Sprintf("%d", endIdx),
	})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// check the element count again
	expectedCnt := 0
	if endIdx == -1 {
		expectedCnt = startIdx
	} else {
		expectedCnt = int(elementCount) - (endIdx - startIdx)
	}
	p, _, err := guts_cli.ReadPage(output, pageId)
	require.NoError(t, err)
	assert.Equal(t, expectedCnt, int(p.Count()))

	assert.Equal(t, expectedOverflow, int(p.Overflow()))

	compareDataAfterClearingElement(t, srcPath, output, pageId, false, startIdx, endIdx)
}

func TestSurgery_Freelist_Abandon(t *testing.T) {
	pageSize := 4096
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize})
	srcPath := db.Path()

	defer requireDBNoChange(t, dbData(t, srcPath), srcPath)

	rootCmd := main.NewRootCommand()
	output := filepath.Join(t.TempDir(), "db")
	rootCmd.SetArgs([]string{
		"surgery", "freelist", "abandon", srcPath,
		"--output", output,
	})
	err := rootCmd.Execute()
	require.NoError(t, err)

	meta0 := loadMetaPage(t, output, 0)
	assert.Equal(t, common.PgidNoFreelist, meta0.Freelist())
	meta1 := loadMetaPage(t, output, 1)
	assert.Equal(t, common.PgidNoFreelist, meta1.Freelist())
}

func loadMetaPage(t *testing.T, dbPath string, pageID uint64) *common.Meta {
	_, buf, err := guts_cli.ReadPage(dbPath, 0)
	require.NoError(t, err)
	return common.LoadPageMeta(buf)
}

func TestSurgery_Freelist_Rebuild(t *testing.T) {
	testCases := []struct {
		name          string
		hasFreelist   bool
		expectedError error
	}{
		{
			name:          "normal operation",
			hasFreelist:   false,
			expectedError: nil,
		},
		{
			name:          "already has freelist",
			hasFreelist:   true,
			expectedError: main.ErrSurgeryFreelistAlreadyExist,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pageSize := 4096
			db := btesting.MustCreateDBWithOption(t, &bolt.Options{
				PageSize:       pageSize,
				NoFreelistSync: !tc.hasFreelist,
			})
			srcPath := db.Path()

			err := db.Update(func(tx *bolt.Tx) error {
				// do nothing
				return nil
			})
			require.NoError(t, err)

			defer requireDBNoChange(t, dbData(t, srcPath), srcPath)

			// Verify the freelist isn't synced in the beginning
			meta := readMetaPage(t, srcPath)
			if tc.hasFreelist {
				if meta.Freelist() <= 1 || meta.Freelist() >= meta.Pgid() {
					t.Fatalf("freelist (%d) isn't in the valid range (1, %d)", meta.Freelist(), meta.Pgid())
				}
			} else {
				require.Equal(t, common.PgidNoFreelist, meta.Freelist())
			}

			// Execute `surgery freelist rebuild` command
			rootCmd := main.NewRootCommand()
			output := filepath.Join(t.TempDir(), "db")
			rootCmd.SetArgs([]string{
				"surgery", "freelist", "rebuild", srcPath,
				"--output", output,
			})
			err = rootCmd.Execute()
			require.Equal(t, tc.expectedError, err)

			if tc.expectedError == nil {
				// Verify the freelist has already been rebuilt.
				meta = readMetaPage(t, output)
				if meta.Freelist() <= 1 || meta.Freelist() >= meta.Pgid() {
					t.Fatalf("freelist (%d) isn't in the valid range (1, %d)", meta.Freelist(), meta.Pgid())
				}
			}
		})
	}
}

func readMetaPage(t *testing.T, path string) *common.Meta {
	_, activeMetaPageId, err := guts_cli.GetRootPage(path)
	require.NoError(t, err)
	_, buf, err := guts_cli.ReadPage(path, uint64(activeMetaPageId))
	require.NoError(t, err)
	return common.LoadPageMeta(buf)
}

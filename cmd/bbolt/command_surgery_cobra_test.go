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
	"go.etcd.io/bbolt/internal/guts_cli"
)

func TestSurgery_ClearPageElement(t *testing.T) {
	testCases := []struct {
		name                 string
		from                 int
		to                   int
		setEndIdxAsCount     bool
		removeOnlyOneElement bool // only valid when setEndIdxAsCount == true, and startIdx = endIdx -1 in this case.
		expectError          bool
	}{
		// normal range
		{
			name: "normal range: [4, 8)",
			from: 4,
			to:   8,
		},
		{
			name: "normal range: [5, -1)",
			from: 4,
			to:   -1,
		},
		{
			name: "normal range: all",
			from: 0,
			to:   -1,
		},
		{
			name: "normal range: [0, 7)",
			from: 0,
			to:   7,
		},
		{
			name:             "normal range: [3, count)",
			from:             4,
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
			testSurgeryClearPageElement(t, tc.from, tc.to, tc.setEndIdxAsCount, tc.removeOnlyOneElement, tc.expectError)
		})
	}
}

func testSurgeryClearPageElement(t *testing.T, startIdx, endIdx int, setEndIdxAsCount, removeOnlyOne, expectError bool) {
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

		if p.IsLeafPage() && p.Count() > 10 {
			elementCount = p.Count()
			break
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
		"--from", fmt.Sprintf("%d", startIdx),
		"--to", fmt.Sprintf("%d", endIdx),
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

	compareDataAfterClearingElement(t, srcPath, output, pageId, startIdx, endIdx)
}

func compareDataAfterClearingElement(t *testing.T, srcPath, dstPath string, pageId uint64, startIdx, endIdx int) {
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
		srcElement := srcPage.LeafPageElement(i)
		dstElement := dstPage.LeafPageElement(dstIdx)

		require.Equal(t, srcElement.Flags(), dstElement.Flags())
		require.Equal(t, srcElement.Key(), dstElement.Key())
		require.Equal(t, srcElement.Value(), dstElement.Value())

		dstIdx++
	}
}

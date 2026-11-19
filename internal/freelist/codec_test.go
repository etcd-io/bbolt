package freelist

import (
	"fmt"
	"math"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

func TestCodec_RoundTrip(t *testing.T) {
	sizes := []int{0, 1, 2, 10, 1000, math.MaxUint16 - 1, math.MaxUint16, math.MaxUint16 + 1, math.MaxUint16 * 2}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("n=%d", size), func(t *testing.T) {
			// interleave free and pending ids to exercise the merge
			free, pending, expected := common.Pgids{}, common.Pgids{}, common.Pgids{}
			for i := 0; i < size; i++ {
				id := common.Pgid(i + 2)
				if i%3 == 0 {
					pending = append(pending, id)
				} else {
					free = append(free, id)
				}
				expected = append(expected, id)
			}

			c := pgidsCodec{}
			buf := make([]byte, c.estimatedWritePageSize(size))
			p := common.LoadPage(buf)
			c.write(p, free, pending)

			require.True(t, p.IsFreelistPage())
			require.Equal(t, expected, c.read(p))
		})
	}
}

func TestCodec_EstimatedWritePageSize(t *testing.T) {
	c := pgidsCodec{}
	hdr := int(common.PageHeaderSize)
	require.Equal(t, hdr, c.estimatedWritePageSize(0))
	require.Equal(t, hdr+8, c.estimatedWritePageSize(1))
	require.Equal(t, hdr+8*(math.MaxUint16-1), c.estimatedWritePageSize(math.MaxUint16-1))
	// from 0xFFFF onwards, an extra element is used to store the count
	require.Equal(t, hdr+8*(math.MaxUint16+1), c.estimatedWritePageSize(math.MaxUint16))
}

func TestCodec_ReadSortsAndCopies(t *testing.T) {
	c := pgidsCodec{}
	buf := make([]byte, 4096)
	p := common.LoadPage(buf)
	// Write refuses unsorted input, so build an unsorted page by hand
	p.SetFlags(common.FreelistPageFlag)
	p.SetCount(2)
	raw := unsafe.Slice((*common.Pgid)(common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))), 2)
	raw[0], raw[1] = 30, 10

	ids := c.read(p)
	require.Equal(t, common.Pgids{10, 30}, ids)

	// mutating the result must not change the page
	ids[0] = 99
	require.Equal(t, []common.Pgid{30, 10}, p.FreelistPageIds())
}

func TestCodec_ReadPanicsOnNonFreelistPage(t *testing.T) {
	buf := make([]byte, 4096)
	p := common.LoadPage(buf)
	p.SetFlags(common.LeafPageFlag)
	require.Panics(t, func() {
		pgidsCodec{}.read(p)
	})
}

func TestCodec_WritePanicsOnUnsortedInput(t *testing.T) {
	c := pgidsCodec{}
	buf := make([]byte, 4096)
	p := common.LoadPage(buf)
	require.PanicsWithValue(t, "write(free): pgids not sorted", func() {
		c.write(p, common.Pgids{5, 3}, common.Pgids{})
	})
	require.PanicsWithValue(t, "write(pending): pgids not sorted", func() {
		c.write(p, common.Pgids{3, 5}, common.Pgids{9, 7})
	})
}

package freelist

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

// Ensure that a freelist can find contiguous blocks of pages.
func TestFreelistArray_allocate(t *testing.T) {
	f := NewArrayFreelist()
	ids := []common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18}
	f.Init(ids)
	id := int(f.Allocate(1, 3))
	require.Equalf(t, 3, id, "exp=3; got=%v", id)
	id = int(f.Allocate(1, 1))
	require.Equalf(t, 6, id, "exp=6; got=%v", id)
	id = int(f.Allocate(1, 3))
	require.Equalf(t, 0, id, "exp=0; got=%v", id)
	id = int(f.Allocate(1, 2))
	require.Equalf(t, 12, id, "exp=12; got=%v", id)
	id = int(f.Allocate(1, 1))
	require.Equalf(t, 7, id, "exp=7; got=%v", id)
	id = int(f.Allocate(1, 0))
	require.Equalf(t, 0, id, "exp=0; got=%v", id)
	id = int(f.Allocate(1, 0))
	require.Equalf(t, 0, id, "exp=0; got=%v", id)
	exp := common.Pgids([]common.Pgid{9, 18})
	require.Truef(t, reflect.DeepEqual(exp, f.freePageIds()), "exp=%v; got=%v", exp, f.freePageIds())

	id = int(f.Allocate(1, 1))
	require.Equalf(t, 9, id, "exp=9; got=%v", id)
	id = int(f.Allocate(1, 1))
	require.Equalf(t, 18, id, "exp=18; got=%v", id)
	id = int(f.Allocate(1, 1))
	require.Equalf(t, 0, id, "exp=0; got=%v", id)
	exp = common.Pgids([]common.Pgid{})
	require.Truef(t, reflect.DeepEqual(exp, f.freePageIds()), "exp=%v; got=%v", exp, f.freePageIds())
}

func TestInvalidArrayAllocation(t *testing.T) {
	f := NewArrayFreelist()
	// page 0 and 1 are reserved for meta pages, so they should never be free pages.
	ids := []common.Pgid{1}
	f.Init(ids)
	require.Panics(t, func() {
		f.Allocate(common.Txid(1), 1)
	})
}

func Test_Freelist_Array_Rollback(t *testing.T) {
	f := newTestArrayFreelist()

	f.Init([]common.Pgid{3, 5, 6, 7, 12, 13})

	f.Free(100, common.NewPage(20, 0, 0, 1))
	f.Allocate(100, 3)
	f.Free(100, common.NewPage(25, 0, 0, 0))
	f.Allocate(100, 2)

	require.Equal(t, map[common.Pgid]common.Txid{5: 100, 12: 100}, f.allocs)
	require.Equal(t, map[common.Txid]*txPending{100: {
		ids:     []common.Pgid{20, 21, 25},
		alloctx: []common.Txid{0, 0, 0},
	}}, f.pending)

	f.Rollback(100)

	require.Equal(t, map[common.Pgid]common.Txid{}, f.allocs)
	require.Equal(t, map[common.Txid]*txPending{}, f.pending)
}

func newTestArrayFreelist() *array {
	f := NewArrayFreelist()
	return f.(*array)
}

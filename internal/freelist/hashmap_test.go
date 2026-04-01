package freelist

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
)

func TestFreelistHashmap_init_panics(t *testing.T) {
	f := NewHashMapFreelist()
	require.Panics(t, func() {
		// init expects sorted input
		f.Init([]common.Pgid{25, 5})
	})
}

func TestFreelistHashmap_allocate(t *testing.T) {
	f := NewHashMapFreelist()

	ids := []common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18}
	f.Init(ids)

	f.Allocate(1, 3)
	if x := f.FreeCount(); x != 6 {
		t.Fatalf("exp=6; got=%v", x)
	}

	f.Allocate(1, 2)
	if x := f.FreeCount(); x != 4 {
		t.Fatalf("exp=4; got=%v", x)
	}
	f.Allocate(1, 1)
	if x := f.FreeCount(); x != 3 {
		t.Fatalf("exp=3; got=%v", x)
	}

	f.Allocate(1, 0)
	if x := f.FreeCount(); x != 3 {
		t.Fatalf("exp=3; got=%v", x)
	}
}

func TestFreelistHashmap_mergeWithExist(t *testing.T) {
	bm1 := pidSet{1: struct{}{}}

	bm2 := pidSet{5: struct{}{}}
	tests := []struct {
		name               string
		ids                common.Pgids
		startPgid, endPgid common.Pgid
		want               common.Pgids
		wantForwardmap     map[common.Pgid]uint64
		wantBackwardmap    map[common.Pgid]uint64
		wantfreemap        map[uint64]pidSet
	}{
		{
			name:            "test1",
			ids:             []common.Pgid{1, 2, 4, 5, 6},
			startPgid:       3,
			endPgid:         3,
			want:            []common.Pgid{1, 2, 3, 4, 5, 6},
			wantForwardmap:  map[common.Pgid]uint64{1: 6},
			wantBackwardmap: map[common.Pgid]uint64{6: 6},
			wantfreemap:     map[uint64]pidSet{6: bm1},
		},
		{
			name:            "test2",
			ids:             []common.Pgid{1, 2, 5, 6},
			startPgid:       3,
			endPgid:         3,
			want:            []common.Pgid{1, 2, 3, 5, 6},
			wantForwardmap:  map[common.Pgid]uint64{1: 3, 5: 2},
			wantBackwardmap: map[common.Pgid]uint64{6: 2, 3: 3},
			wantfreemap:     map[uint64]pidSet{3: bm1, 2: bm2},
		},
		{
			name:            "test3",
			ids:             []common.Pgid{1, 2},
			startPgid:       3,
			endPgid:         3,
			want:            []common.Pgid{1, 2, 3},
			wantForwardmap:  map[common.Pgid]uint64{1: 3},
			wantBackwardmap: map[common.Pgid]uint64{3: 3},
			wantfreemap:     map[uint64]pidSet{3: bm1},
		},
		{
			name:            "test4",
			ids:             []common.Pgid{2, 3},
			startPgid:       1,
			endPgid:         1,
			want:            []common.Pgid{1, 2, 3},
			wantForwardmap:  map[common.Pgid]uint64{1: 3},
			wantBackwardmap: map[common.Pgid]uint64{3: 3},
			wantfreemap:     map[uint64]pidSet{3: bm1},
		},
		{
			name:            "test5",
			ids:             []common.Pgid{1, 2, 6},
			startPgid:       3,
			endPgid:         5,
			want:            []common.Pgid{1, 2, 3, 4, 5, 6},
			wantForwardmap:  map[common.Pgid]uint64{1: 6},
			wantBackwardmap: map[common.Pgid]uint64{6: 6},
			wantfreemap:     map[uint64]pidSet{6: bm1},
		},
		{
			name:            "test6",
			ids:             []common.Pgid{1, 2, 6},
			startPgid:       3,
			endPgid:         4,
			want:            []common.Pgid{1, 2, 3, 4, 6},
			wantForwardmap:  map[common.Pgid]uint64{1: 4, 6: 1},
			wantBackwardmap: map[common.Pgid]uint64{4: 4, 6: 1},
			wantfreemap:     map[uint64]pidSet{4: bm1, 1: {6: struct{}{}}},
		},
	}
	for _, tt := range tests {
		f := newTestHashMapFreelist()
		f.Init(tt.ids)

		f.mergeWithExistingSpan(tt.startPgid, tt.endPgid)

		if got := f.freePageIds(); !reflect.DeepEqual(tt.want, got) {
			t.Fatalf("name %s; exp=%v; got=%v", tt.name, tt.want, got)
		}
		if got := f.forwardMap; !reflect.DeepEqual(tt.wantForwardmap, got) {
			t.Fatalf("name %s; exp=%v; got=%v", tt.name, tt.wantForwardmap, got)
		}
		if got := f.backwardMap; !reflect.DeepEqual(tt.wantBackwardmap, got) {
			t.Fatalf("name %s; exp=%v; got=%v", tt.name, tt.wantBackwardmap, got)
		}
		if got := f.freemaps; !reflect.DeepEqual(tt.wantfreemap, got) {
			t.Fatalf("name %s; exp=%v; got=%v", tt.name, tt.wantfreemap, got)
		}
	}
}

func TestFreelistHashmap_GetFreePageIDs(t *testing.T) {
	f := newTestHashMapFreelist()

	N := int32(100000)
	fm := make(map[common.Pgid]uint64)
	i := int32(0)
	val := int32(0)
	for i = 0; i < N; {
		val = rand.Int31n(1000)
		fm[common.Pgid(i)] = uint64(val)
		i += val
		f.freePagesCount += uint64(val)
	}

	f.forwardMap = fm
	res := f.freePageIds()

	if !sort.SliceIsSorted(res, func(i, j int) bool { return res[i] < res[j] }) {
		t.Fatalf("pgids not sorted")
	}
}

func Test_Freelist_Hashmap_Rollback(t *testing.T) {
	f := newTestHashMapFreelist()

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

func Benchmark_freelist_hashmapGetFreePageIDs(b *testing.B) {
	f := newTestHashMapFreelist()
	N := int32(100000)
	fm := make(map[common.Pgid]uint64)
	i := int32(0)
	val := int32(0)
	for i = 0; i < N; {
		val = rand.Int31n(1000)
		fm[common.Pgid(i)] = uint64(val)
		i += val
	}

	f.forwardMap = fm

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		f.freePageIds()
	}
}

func Benchmark_freelist_hashmapMergeSpans(b *testing.B) {
	type testCase struct {
		maxPgid    int
		mergeCount int
	}

	testCases := []testCase{
		{maxPgid: 1000, mergeCount: 500},
		{maxPgid: 5000, mergeCount: 2500},
		{maxPgid: 10000, mergeCount: 5000},
	}
	mergeSpanLens := []int{1, 16, 32, 64}

	for _, tc := range testCases {
		for _, mergeSpanLen := range mergeSpanLens {
			benchmarkName := fmt.Sprintf("max%d_merge%d_span%d", tc.maxPgid, tc.mergeCount, mergeSpanLen)
			initIDs, mergePgids := benchmarkInitAndMergePgids(tc.maxPgid, tc.mergeCount, mergeSpanLen)

			b.Run(benchmarkName, func(b *testing.B) {
				b.Run("nonBatch", func(b *testing.B) {
					benchmarkHashMapMergeSpansNonBatch(b, initIDs, mergePgids)
				})

				b.Run("batch", func(b *testing.B) {
					benchmarkHashMapMergeSpans(b, initIDs, mergePgids)
				})
			})
		}
	}
}

func benchmarkInitAndMergePgids(maxPgid, mergeCount, mergeSpanLen int) (common.Pgids, common.Pgids) {
	const gapSize = 100

	mergeSpanCount := (mergeCount + mergeSpanLen - 1) / mergeSpanLen
	requiredPageCount := mergeCount + mergeSpanCount - 1
	if requiredPageCount > maxPgid-1 {
		panic("invalid benchmark parameters: insufficient pgid space")
	}

	initIDs := make(common.Pgids, 0, maxPgid-mergeCount-1)
	mergePgids := make(common.Pgids, 0, mergeCount)
	next := common.Pgid(2)
	remaining := mergeCount

	for remaining > 0 {
		spanLen := mergeSpanLen
		if remaining < spanLen {
			spanLen = remaining
		}

		spanPgids := make(common.Pgids, 0, spanLen)
		for i := 0; i < spanLen; i++ {
			spanPgids = append(spanPgids, next+common.Pgid(i))
		}
		// Prepend each contiguous span so larger page IDs are merged first.
		mergePgids = append(spanPgids, mergePgids...)

		next += common.Pgid(spanLen)
		remaining -= spanLen
		if remaining == 0 {
			break
		}

		for i := 0; i < gapSize; i++ {
			initIDs = append(initIDs, next)
			next++
		}
	}

	for ; next <= common.Pgid(maxPgid); next++ {
		initIDs = append(initIDs, next)
	}

	return initIDs, mergePgids
}

func benchmarkHashMapMergeSpans(b *testing.B, initIDs, mergePgids common.Pgids) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f := newTestHashMapFreelist()
		f.Init(initIDs)

		mergeIDs := append(common.Pgids(nil), mergePgids...)

		b.StartTimer()
		f.mergeSpans(mergeIDs)
	}
}

func benchmarkHashMapMergeSpansNonBatch(b *testing.B, initIDs, mergePgids common.Pgids) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f := newTestHashMapFreelist()
		f.Init(initIDs)

		mergeIDs := append(common.Pgids(nil), mergePgids...)

		b.StartTimer()
		for _, id := range mergeIDs {
			f.mergeWithExistingSpan(id, id)
		}
	}
}

func newTestHashMapFreelist() *hashMap {
	f := NewHashMapFreelist()
	return f.(*hashMap)
}

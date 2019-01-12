package bbolt

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"unsafe"
)

// Ensure that a page is added to a transaction's freelist.
func TestFreelist_free(t *testing.T) {
	f := newFreelist()
	f.free(100, &page{id: 12})
	if !reflect.DeepEqual([]freespan{makeFreespan(12, 1)}, f.pending[100].spans) {
		t.Fatalf("exp=%v; got=%v", []pgid{12}, f.pending[100])
	}
}

// Ensure that a page and its overflow is added to a transaction's freelist.
func TestFreelist_free_overflow(t *testing.T) {
	f := newFreelist()
	f.free(100, &page{id: 12, overflow: 3})
	if exp := []freespan{makeFreespan(12, 4)}; !reflect.DeepEqual(exp, f.pending[100].spans) {
		t.Fatalf("exp=%v; got=%v", exp, f.pending[100])
	}
}

// Ensure that a transaction's free pages can be released.
func TestFreelist_release(t *testing.T) {
	f := newFreelist()
	f.free(100, &page{id: 12, overflow: 1})
	f.free(100, &page{id: 9})
	f.free(102, &page{id: 39})
	f.release(100)
	f.release(101)
	if exp := []freespan{makeFreespan(9, 1), makeFreespan(12, 2)}; !reflect.DeepEqual(exp, f.spans) {
		t.Fatalf("exp=%v; got=%v", exp, f.spans)
	}

	f.release(102)
	if exp := []freespan{makeFreespan(9, 1), makeFreespan(12, 2), makeFreespan(39, 1)}; !reflect.DeepEqual(exp, f.spans) {
		t.Fatalf("exp=%v; got=%v", exp, f.spans)
	}
}

// Ensure that releaseRange handles boundary conditions correctly
func TestFreelist_releaseRange(t *testing.T) {
	type testRange struct {
		begin, end txid
	}

	type testPage struct {
		span     freespan
		allocTxn txid
		freeTxn  txid
	}

	var releaseRangeTests = []struct {
		title         string
		pagesIn       []testPage
		releaseRanges []testRange
		wantFree      []freespan
	}{
		{
			title:         "Single pending in range",
			pagesIn:       []testPage{{span: makeFreespan(3, 1), allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{1, 300}},
			wantFree:      []freespan{makeFreespan(3, 1)},
		},
		{
			title:         "Single pending with minimum end range",
			pagesIn:       []testPage{{span: makeFreespan(3, 1), allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{1, 200}},
			wantFree:      []freespan{makeFreespan(3, 1)},
		},
		{
			title:         "Single pending outsize minimum end range",
			pagesIn:       []testPage{{span: makeFreespan(3, 1), allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{1, 199}},
			wantFree:      []freespan{},
		},
		{
			title:         "Single pending with minimum begin range",
			pagesIn:       []testPage{{span: makeFreespan(3, 1), allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{100, 300}},
			wantFree:      []freespan{makeFreespan(3, 1)},
		},
		{
			title:         "Single pending outside minimum begin range",
			pagesIn:       []testPage{{span: makeFreespan(3, 1), allocTxn: 100, freeTxn: 200}},
			releaseRanges: []testRange{{101, 300}},
			wantFree:      []freespan{},
		},
		{
			title:         "Single pending in minimum range",
			pagesIn:       []testPage{{span: makeFreespan(3, 1), allocTxn: 199, freeTxn: 200}},
			releaseRanges: []testRange{{199, 200}},
			wantFree:      []freespan{makeFreespan(3, 1)},
		},
		{
			title:         "Single pending and read transaction at 199",
			pagesIn:       []testPage{{span: makeFreespan(3, 1), allocTxn: 199, freeTxn: 200}},
			releaseRanges: []testRange{{100, 198}, {200, 300}},
			wantFree:      []freespan{},
		},
		{
			title: "Adjacent pending and read transactions at 199, 200",
			pagesIn: []testPage{
				{span: makeFreespan(3, 1), allocTxn: 199, freeTxn: 200},
				{span: makeFreespan(4, 1), allocTxn: 200, freeTxn: 201},
			},
			releaseRanges: []testRange{
				{100, 198},
				{200, 199}, // Simulate the ranges db.freePages might produce.
				{201, 300},
			},
			wantFree: []freespan{},
		},
		{
			title: "Out of order ranges",
			pagesIn: []testPage{
				{span: makeFreespan(3, 1), allocTxn: 199, freeTxn: 200},
				{span: makeFreespan(4, 1), allocTxn: 200, freeTxn: 201},
			},
			releaseRanges: []testRange{
				{201, 199},
				{201, 200},
				{200, 200},
			},
			wantFree: []freespan{},
		},
		{
			title: "Multiple pending, read transaction at 150",
			pagesIn: []testPage{
				{span: makeFreespan(3, 1), allocTxn: 100, freeTxn: 200},
				{span: makeFreespan(4, 1), allocTxn: 100, freeTxn: 125},
				{span: makeFreespan(5, 1), allocTxn: 125, freeTxn: 150},
				{span: makeFreespan(6, 1), allocTxn: 125, freeTxn: 175},
				{span: makeFreespan(7, 2), allocTxn: 150, freeTxn: 175},
				{span: makeFreespan(9, 2), allocTxn: 175, freeTxn: 200},
			},
			releaseRanges: []testRange{{50, 149}, {151, 300}},
			wantFree:      []freespan{makeFreespan(4, 1), makeFreespan(9, 2)},
		},
	}

	for _, c := range releaseRangeTests {
		f := newFreelist()

		for _, p := range c.pagesIn {
			f.spans = append(f.spans, p.span)
		}
		f.spansTomap()

		for _, p := range c.pagesIn {
			f.allocate(p.allocTxn, int(p.span.size()))
		}

		for _, p := range c.pagesIn {
			overflow := uint32(0)
			if p.span.size() != 1 {
				overflow = uint32(p.span.size() - 1)
			}
			f.free(p.freeTxn, &page{id: p.span.start(), overflow: overflow})
		}

		for _, r := range c.releaseRanges {
			f.releaseRange(r.begin, r.end)
		}

		if exp := c.wantFree; !reflect.DeepEqual(exp, f.spans) {
			t.Errorf("exp=%v; got=%v for %s", exp, f.spans, c.title)
		}
	}
}

// Ensure that a freelist can find contiguous blocks of pages.
func TestFreelist_allocate(t *testing.T) {
	f := newFreelist()
	f.spans = []freespan{makeFreespan(3, 5), makeFreespan(9, 1), makeFreespan(12, 2), makeFreespan(18, 1)}
	f.spansTomap()

	f.allocate(1, 3)
	if x := f.freePageCount(); x != 6 {
		t.Fatalf("exp=6; got=%v", x)
	}
	f.allocate(1, 1)
	if x := f.freePageCount(); x != 5 {
		t.Fatalf("exp=5; got=%v", x)
	}
	f.allocate(1, 3)
	if x := f.freePageCount(); x != 5 {
		t.Fatalf("exp=5; got=%v", x)
	}

	f.allocate(1, 2)
	if x := f.freePageCount(); x != 3 {
		t.Fatalf("exp=3; got=%v", x)
	}
	f.allocate(1, 1)
	if x := f.freePageCount(); x != 2 {
		t.Fatalf("exp=2; got=%v", x)
	}
	f.allocate(1, 0)
	if x := f.freePageCount(); x != 2 {
		t.Fatalf("exp=2; got=%v", x)
	}

	f.allocate(1, 0)
	if x := f.freePageCount(); x != 2 {
		t.Fatalf("exp=2; got=%v", x)
	}

	f.allocate(1, 1)
	if x := f.freePageCount(); x != 1 {
		t.Fatalf("exp=1; got=%v", x)
	}

	f.allocate(1, 1)
	if x := f.freePageCount(); x != 0 {
		t.Fatalf("exp=0; got=%v", x)
	}
}

// Ensure that a freelist can deserialize from a freelist page.
func TestFreelist_read(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = freelistPageFlag
	page.count = 2

	// Insert 2 page ids.
	ids := (*[3]freespan)(unsafe.Pointer(&page.ptr))
	ids[0] = makeFreespan(23, 1)
	ids[1] = makeFreespan(50, 1)

	// Deserialize page into a freelist.
	f := newFreelist()
	f.read(page)

	// Ensure that there are two page ids in the freelist.
	if exp := []freespan{makeFreespan(23, 1), makeFreespan(50, 1)}; !reflect.DeepEqual(exp, f.spans) {
		t.Fatalf("exp=%v; got=%v", exp, f.spans)
	}
}

// Ensure that a freelist can serialize into a freelist page.
func TestFreelist_write(t *testing.T) {
	// Create a freelist and write it to a page.
	var buf [4096]byte
	f := &freelist{spans: []freespan{makeFreespan(12, 1), makeFreespan(39, 1)}, pending: make(map[txid]*txPending)}
	f.pending[100] = &txPending{spans: []freespan{makeFreespan(11, 1), makeFreespan(28, 1)}}
	f.pending[101] = &txPending{spans: []freespan{makeFreespan(3, 1)}}
	p := (*page)(unsafe.Pointer(&buf[0]))
	if err := f.write(p); err != nil {
		t.Fatal(err)
	}

	// Read the page back out.
	f2 := newFreelist()
	f2.read(p)

	// Ensure that the freelist is correct.
	// All pages should be present and in reverse order.
	if exp := []freespan{makeFreespan(3, 1), makeFreespan(11, 2), makeFreespan(28, 1), makeFreespan(39, 1)}; !reflect.DeepEqual(exp, f2.spans) {
		t.Fatalf("exp=%v; got=%v", exp, f2.spans)
	}
}

func Benchmark_FreelistRelease10K(b *testing.B)    { benchmark_FreelistRelease(b, 10000) }
func Benchmark_FreelistRelease100K(b *testing.B)   { benchmark_FreelistRelease(b, 100000) }
func Benchmark_FreelistRelease1000K(b *testing.B)  { benchmark_FreelistRelease(b, 1000000) }
func Benchmark_FreelistRelease10000K(b *testing.B) { benchmark_FreelistRelease(b, 10000000) }

func benchmark_FreelistRelease(b *testing.B, size int) {
	total := randomSpans(size)
	spans := total[0 : size/2]
	pending := total[size/2:]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txp := &txPending{spans: pending}
		f := &freelist{spans: spans, pending: map[txid]*txPending{1: txp}}
		f.release(1)
	}
}

func randomSpans(n int) freespans {
	rand.Seed(42)
	freespans := make(freespans, n)
	for i := range freespans {
		freespans[i] = makeFreespan(pgid(rand.Int31()), 1)
	}
	sort.Sort(freespans)
	// filter out the dup ones
	index := 1
	for i := 1; i < len(freespans)-1; i++ {
		if freespans[i] != freespans[i-1] {
			freespans[index] = freespans[i]
			index++
		}
	}
	return freespans[:index]
}

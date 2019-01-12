package bbolt

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"unsafe"
)

// txPending holds a list of pgids and corresponding allocation txns
// that are pending to be freed.
type txPending struct {
	spans            []freespan
	alloctx          []txid // txids allocating the span
	lastReleaseBegin txid   // beginning txid of last matching releaseRange
}

func (txp txPending) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "string of txpending\n")
	for _, span := range txp.spans {
		fmt.Fprintf(&buf, "span %s\n", span)
	}
	for _, txid := range txp.alloctx {
		fmt.Fprintf(&buf, "txid %d\n", txid)
	}
	fmt.Fprintf(&buf, "lastReleaseBegin %d\n", txp.lastReleaseBegin)

	return buf.String()
}

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
type freelist struct {
	freemaps map[uint64]*bitmap  // the helper map for mapping the same size to the index in the spans
	spans    []freespan          // all free and available free page spans.
	allocs   map[pgid]txid       // mapping of txid that allocated a pgid.
	pending  map[txid]*txPending // mapping of soon-to-be free page spans by tx; each is sorted.
	cache    map[pgid]bool       // fast lookup of all free and pending page ids.
}

// newFreelist returns an empty, initialized freelist.
func newFreelist() *freelist {
	return &freelist{
		freemaps: make(map[uint64]*bitmap),
		spans:    make(freespans, 0),
		allocs:   make(map[pgid]txid),
		pending:  make(map[txid]*txPending),
		cache:    make(map[pgid]bool),
	}
}

// size returns the size of the freelist page after serialization.
func (f *freelist) size() int {
	n := f.spancount()
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		n++
	}
	return pageHeaderSize + (int(unsafe.Sizeof(freespanZero)) * n)
}

// spancount returns the number of spans in the freelist. It may overcount.
func (f *freelist) spancount() int {
	return f.freeSpanCount() + f.pendingSpanCount()
}

// freeSpanCount returns the number of free spans.
func (f *freelist) freeSpanCount() int {
	return len(f.spans)
}

// pendingSpanCount returns the number of pending spans. It may overcount.
func (f *freelist) pendingSpanCount() int {
	var n int
	// This is a floor. Some of these pending spans may be mergeable.
	for _, txp := range f.pending {
		n += len(txp.spans)
	}
	return n
}

// pagecount returns the number of pages on the freelist.
func (f *freelist) pagecount() int {
	return f.freePageCount() + f.pendingPageCount()
}

// freePageCount returns the number of free pages on the freelist.
func (f *freelist) freePageCount() int {
	var n int
	for _, span := range f.spans {
		n += int(span.size())
	}
	return n
}

// pendingPageCount returns the number of pending pages on the freelist.
func (f *freelist) pendingPageCount() int {
	var n int
	for _, txp := range f.pending {
		for _, span := range txp.spans {
			n += int(span.size())
		}
	}
	return n
}

// allpages returns an unsorted list of all free ids and all pending ids.
func (f *freelist) allpages() []pgid {
	ids := make(pgids, 0, f.pagecount())
	for _, span := range f.spans {
		ids = span.appendAll(ids)
	}
	for _, list := range f.pending {
		for _, span := range list.spans {
			ids = span.appendAll(ids)
		}
	}
	return ids
}

// copyall copies into dst a normalized, sorted list combining all free and pending spans.
// It returns the number of spans copied into dst.
func (f *freelist) copyall(dst []freespan) int {
	all := make([][]freespan, 0, len(f.pending)+1)
	all = append(all, f.spans)
	dst = dst[:0]
	for _, txp := range f.pending {
		all = append(all, txp.spans)
	}
	return len(mergenorm(dst, all))
}

// allocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
func (f *freelist) allocate(txid txid, n int) pgid {
	if n == 0 {
		return 0
	}

	// if we have a exact size match just return short path
	if bm, ok := f.freemaps[uint64(n)]; ok {
		index := bm.Get()
		if index == -1 {
			// doesn't have any thing delete the size class from the map
			delete(f.freemaps, uint64(n))
		} else {
			// remove the index
			bm.Del(index)
			// get the span
			span := f.spans[index]

			initial := span.start()
			f.spans[index] = makeFreespan(initial+pgid(n), 0)

			f.allocs[initial] = txid

			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+pgid(i))
			}
			return initial
		}
	}

	// lookup the map
	for size, bm := range f.freemaps {
		if size < uint64(n) {
			continue
		}

		index := bm.Get()
		if index == -1 {
			// doesn't have any thing delete the size class from the map
			delete(f.freemaps, size)
			continue
		}
		// remove the index
		bm.Del(index)
		// get the span
		span := f.spans[index]

		initial := span.start()
		remain := span.size() - uint64(n)
		f.spans[index] = makeFreespan(initial+pgid(n), remain)

		if remain != 0 {
			if _, ok := f.freemaps[uint64(remain)]; !ok {
				f.freemaps[uint64(remain)] = newBitmap()
			}
			f.freemaps[uint64(remain)].Add(index)
		}
		f.allocs[initial] = txid

		for i := pgid(0); i < pgid(n); i++ {
			delete(f.cache, initial+pgid(i))
		}
		return initial
	}

	return 0
}

func (f *freelist) printFreeSpans() {
	log.Println()
	for _, span := range f.spans {
		log.Printf("spandetail %d %d\n", span.start(), span.size())
	}
	log.Println()
}

func (f *freelist) printFreeMaps() {
	log.Println()
	for size, bm := range f.freemaps {
		log.Printf("mapdetail %d %v\n", size, bm.ToIndices())
	}
	log.Println()
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}
	// Free p and all its overflow pages.
	pspan := makeFreespan(p.id, uint64(p.overflow)+1)

	txp := f.pending[txid]
	if txp == nil {
		txp = &txPending{}
		f.pending[txid] = txp
	}

	var spans = f.pending[txid].spans
	n := sort.Search(len(spans), func(i int) bool { return spans[i] > pspan })

	if n == len(spans) {
		spans = append(spans, pspan)
	} else {
		u, v := pspan.append(spans[n])
		if v == 0 {
			// spans[n] and pspan were combined. Replace spans[n] with the new value.
			spans[n] = u
		} else {
			// Insert new span.
			spans = append(spans, 0)
			copy(spans[n+1:], spans[n:])
			spans[n] = u
			spans[n+1] = v
		}
	}
	f.pending[txid].spans = spans

	allocTxid, ok := f.allocs[p.id]
	if ok {
		delete(f.allocs, p.id)
	} else if (p.flags & freelistPageFlag) != 0 {
		// Freelist is always allocated by prior tx.
		allocTxid = txid - 1
	}
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		f.cache[id] = true
		txp.alloctx = append(txp.alloctx, allocTxid)
	}
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txid txid) {
	all := make([][]freespan, 0, len(f.pending)+1)
	all = append(all, f.spans)
	for tid, txp := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			all = append(all, txp.spans)
			delete(f.pending, tid)
		}
	}
	f.spans = mergenorm(nil, all)
	f.spansTomap()
}

// rollback removes the pages from a given pending tx.
func (f *freelist) rollback(txid txid) {
	// Remove pages from pending list.
	// Remove page ids from cache.
	txp := f.pending[txid]
	if txp == nil {
		return
	}
	var all [][]freespan
	var spans []freespan
	for i, span := range txp.spans {
		for i := span.start(); i < span.next(); i++ {
			delete(f.cache, i)
		}

		tx := txp.alloctx[i]
		if tx == 0 {
			continue
		}
		if tx != txid {
			// Pending free aborted; restore page back to alloc list.
			for i := span.start(); i < span.next(); i++ {
				f.allocs[i] = tx
			}
		} else {
			// Freed spans was allocated by this txn.
			spans = append(spans, span)
		}
		all = append(all, spans)
	}

	// Remove pages from pending list and mark as free if allocated by txid.
	delete(f.pending, txid)

	f.spans = mergenorm(f.spans, all)
	f.spansTomap()
}

// freed reports whether a given page is in the free list.
func (f *freelist) freed(pgid pgid) bool {
	if freespans(f.spans).contains(pgid) {
		return true
	}
	for _, s := range f.pending {
		if freespans(s.spans).contains(pgid) {
			return true
		}
	}
	return false
}

// read initializes the freelist from a freelist page.
func (f *freelist) read(p *page) {
	if (p.flags & freelistPageFlag) == 0 {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.id, p.typ()))
	}
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.

	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[0])
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.spans = nil
	} else {
		spans := ((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.spans = make([]freespan, len(spans))
		copy(f.spans, spans)

		// Make sure they're sorted.
		// TODO: eliminate? By construction, they are sorted.
		// Or instead, panic if not sorted?
		//sort.Slice(f.spans, func(i, j int) bool { return f.spans[i] < f.spans[j] })
	}
	f.spansTomap()

	f.reindex()
}

func (f *freelist) spansTomap() {
	//reindex
	f.freemaps = make(map[uint64]*bitmap)
	for i, v := range f.spans {
		size := v.size()
		if _, ok := f.freemaps[size]; !ok {
			f.freemaps[size] = newBitmap()
		}

		f.freemaps[size].Add(i)
	}

}

func (f *freelist) check() {
	for size, bm := range f.freemaps {
		indices := bm.ToIndices()

		for _, index := range indices {
			if size != f.spans[index].size() {
				panic("wrong size ....")
			}
		}
	}
}

// write writes the spans onto a freelist page. All free and pending spans are
// saved to disk since in the event of a program crash, all pending spans will
// become free.
func (f *freelist) write(p *page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements.
	// If we might overflow that number then we put the size in the first element.
	n := f.spancount()
	switch {
	case n == 0:
		p.count = 0
	case n < 0xFFFF:
		n = f.copyall(((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[:])
		p.count = uint16(n)
	default:
		p.count = 0xFFFF
		n = f.copyall(((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[1:])
		((*[maxAllocSize]freespan)(unsafe.Pointer(&p.ptr)))[0] = freespan(n)
	}

	return nil
}

// reload reads the freelist from a page and filters out pending items.
func (f *freelist) reload(p *page) {
	f.read(p)

	// Gather all pending spans into a single list.
	all := make([][]freespan, 0, len(f.pending))
	for _, txp := range f.pending {
		all = append(all, txp.spans)
	}
	pending := mergenorm(nil, all)

	// Remove all pending spans from f.spans.
	for _, rm := range pending {
		n := sort.Search(len(f.spans), func(i int) bool { return f.spans[i] > rm })
		// n is where rm would be inserted.
		// Every element to remove must be a sub-span of some span in f.spans,
		// so n cannot have a start greater than the largest start in f.spans,
		// nor have it have an equal start or greater size.
		// Therefore, n != len(f.spans).

		// If rm is a strict prefix of one of f's spans,
		// the containing span will be at n.
		// Otherwise, it'll be at n-1.
		if s := f.spans[n]; rm.start() == s.start() {
			f.spans[n] = makeFreespan(s.start()+pgid(rm.size()), uint64(s.size())-rm.size())
			continue
		}

		s := f.spans[n-1]
		if s.start() == rm.start() {
			// Exact match.
			if rm.size() != s.size() {
				panic("sort.Search misuse?")
			}
			f.spans[n-1] = makeFreespan(s.start(), 0)
			continue
		}

		if !s.contains(rm.start()) {
			panic("sort.Search misuse (part b)?")
		}

		if s.next() == rm.next() {
			// rm is a suffix of s.
			f.spans[n-1] = makeFreespan(s.start(), s.size()-rm.size())
			continue
		}

		// rm splits s into two parts.
		// TODO: this insertion business could lead to quadratic behavior!
		f.spans = append(f.spans, 0)
		copy(f.spans[n:], f.spans[n-1:])
		f.spans[n-1] = makeFreespan(s.start(), uint64(rm.start()-s.start()))
		f.spans[n] = makeFreespan(rm.next(), uint64(s.next()-rm.next()))
	}
	f.spansTomap()
}

// readIDs initializes the freelist from a given list of ids.
func (f *freelist) readIDs(ids []pgid) {
	//make sure ids are sorted
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if len(ids) == 0 {
		return
	}

	if len(ids) == 1 {
		span := makeFreespan(ids[0], uint64(1))
		f.spans = append(f.spans, span)
		f.reindex()
		return
	}

	size := 1
	initial := ids[0]

	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[i-1] {
			size++
		} else {
			span := makeFreespan(initial, uint64(size))
			f.spans = append(f.spans, span)
			//a new start
			initial = ids[i]
			size = 1
		}

	}

	if size != 0 && initial != 0 {
		span := makeFreespan(initial, uint64(size))
		f.spans = append(f.spans, span)
	}

	// Walk dst and merge the continous page.
	out := 0
	for i := 1; i < len(f.spans); i++ {
		u, v := f.spans[out].append(f.spans[i])
		if u == 0 {
			continue
		}
		if v == 0 {
			f.spans[out] = u
			continue
		}
		f.spans[out] = u
		f.spans[out+1] = v
		out++
	}
	if f.spans[out].size() != 0 {
		out++
	}
	f.spans = f.spans[:out]
	f.spansTomap()

	f.reindex()
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *freelist) reindex() {
	f.cache = make(map[pgid]bool, f.freePageCount())

	for _, span := range f.spans {
		for i := span.start(); i < span.next(); i++ {
			f.cache[i] = true
		}
	}

	for _, txp := range f.pending {
		for _, span := range txp.spans {
			for i := span.start(); i < span.next(); i++ {
				f.cache[i] = true
			}
		}
	}
}

// releaseRange moves pending pages allocated within an extent [begin,end] to the free list.
func (f *freelist) releaseRange(begin, end txid) {
	if begin > end {
		return
	}

	all := make([][]freespan, 0, len(f.pending)+1)
	for txid, txp := range f.pending {
		one := make([]freespan, 0)
		if txid < begin || txid > end {
			continue
		}
		// Don't recompute freed pages if ranges haven't updated.
		if txp.lastReleaseBegin == begin {
			continue
		}

		for i := 0; i < len(txp.spans); i++ {
			if atx := txp.alloctx[i]; atx < begin || atx > end {
				continue
			}
			one = append(one, txp.spans[i])

			// after this operation txp.spans may be not sorted
			txp.spans[i] = txp.spans[len(txp.spans)-1]
			txp.spans = txp.spans[:len(txp.spans)-1]
			txp.alloctx[i] = txp.alloctx[len(txp.alloctx)-1]
			txp.alloctx = txp.alloctx[:len(txp.alloctx)-1]
			i--
		}
		if len(one) != 0 {
			sort.Slice(one, func(i, j int) bool { return one[i] < one[j] })
			all = append(all, one)
		}

		txp.lastReleaseBegin = begin
		// make sure txp.spans is sorted
		sort.Slice(txp.spans, func(i, j int) bool { return txp.spans[i] < txp.spans[j] })

		if len(txp.spans) == 0 {
			delete(f.pending, txid)
		}
	}
	f.spans = mergenorm(f.spans, all)
	f.spansTomap()
}

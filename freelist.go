package bbolt

import (
	"fmt"
	"math"
	"sort"
	"unsafe"

	"go.etcd.io/bbolt/internal/common"
)

// txPending holds a list of pgids and corresponding allocation txns
// that are pending to be freed.
type txPending struct {
	ids              []common.Pgid
	alloctx          []common.Txid // txids allocating the ids
	lastReleaseBegin common.Txid   // beginning txid of last matching releaseRange
}

// pidSet holds the set of starting pgids which have the same span size
type pidSet map[common.Pgid]struct{}

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
type freelist struct {
	allocs         map[common.Pgid]common.Txid               // mapping of Txid that allocated a pgid.
	pending        map[common.Txid]*txPending                // mapping of soon-to-be free page ids by tx.
	cache          map[common.Pgid]struct{}                  // fast lookup of all free and pending page ids.
	freemaps       map[uint64]pidSet                         // key is the size of continuous pages(span), value is a set which contains the starting pgids of same size
	forwardMap     map[common.Pgid]uint64                    // key is start pgid, value is its span size
	backwardMap    map[common.Pgid]uint64                    // key is end pgid, value is its span size
	freePagesCount uint64                                    // count of free pages(hashmap version)
	allocate       func(txid common.Txid, n int) common.Pgid // the freelist allocate func
	free_count     func() int                                // the function which gives you free page number
	mergeSpans     func(ids common.Pgids)                    // the mergeSpan func
	getFreePageIDs func() []common.Pgid                      // get free pgids func
	readIDs        func(pgids []common.Pgid)                 // readIDs func reads list of pages and init the freelist
	freelistType   FreelistType                              // freelist type
	ids            []common.Pgid                             // all free and available free page ids.
	readonlyTXIDs  []common.Txid                             // all readonly transaction IDs.
}

// newFreelist returns an empty, initialized freelist.
func newFreelist(freelistType FreelistType) *freelist {
	f := &freelist{
		freelistType: freelistType,
		allocs:       make(map[common.Pgid]common.Txid),
		pending:      make(map[common.Txid]*txPending),
		cache:        make(map[common.Pgid]struct{}),
		freemaps:     make(map[uint64]pidSet),
		forwardMap:   make(map[common.Pgid]uint64),
		backwardMap:  make(map[common.Pgid]uint64),
	}

	if freelistType == FreelistMapType {
		f.allocate = f.hashmapAllocate
		f.free_count = f.hashmapFreeCount
		f.mergeSpans = f.hashmapMergeSpans
		f.getFreePageIDs = f.hashmapGetFreePageIDs
		f.readIDs = f.hashmapReadIDs
	} else {
		f.allocate = f.arrayAllocate
		f.free_count = f.arrayFreeCount
		f.mergeSpans = f.arrayMergeSpans
		f.getFreePageIDs = f.arrayGetFreePageIDs
		f.readIDs = f.arrayReadIDs
	}

	return f
}

// size returns the size of the page after serialization.
func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		n++
	}
	return int(common.PageHeaderSize) + (int(unsafe.Sizeof(common.Pgid(0))) * n)
}

// count returns count of pages on the freelist
func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

// pending_count returns count of pending pages
func (f *freelist) pending_count() int {
	var count int
	for _, txp := range f.pending {
		count += len(txp.ids)
	}
	return count
}

// copyall copies a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
func (f *freelist) copyall(dst []common.Pgid) {
	m := make(common.Pgids, 0, f.pending_count())
	for _, txp := range f.pending {
		m = append(m, txp.ids...)
	}
	sort.Sort(m)
	common.Mergepgids(dst, f.getFreePageIDs(), m)
}

// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
func (f *freelist) free(txid common.Txid, p *common.Page) {
	if p.Id() <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.Id()))
	}

	// Free page and all its overflow pages.
	txp := f.pending[txid]
	if txp == nil {
		txp = &txPending{}
		f.pending[txid] = txp
	}
	allocTxid, ok := f.allocs[p.Id()]
	if ok {
		delete(f.allocs, p.Id())
	} else if p.IsFreelistPage() {
		// Freelist is always allocated by prior tx.
		allocTxid = txid - 1
	}

	for id := p.Id(); id <= p.Id()+common.Pgid(p.Overflow()); id++ {
		// Verify that page is not already free.
		if _, ok := f.cache[id]; ok {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		// Add to the freelist and cache.
		txp.ids = append(txp.ids, id)
		txp.alloctx = append(txp.alloctx, allocTxid)
		f.cache[id] = struct{}{}
	}
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txid common.Txid) {
	m := make(common.Pgids, 0)
	for tid, txp := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, txp.ids...)
			delete(f.pending, tid)
		}
	}
	f.mergeSpans(m)
}

// releaseRange moves pending pages allocated within an extent [begin,end] to the free list.
func (f *freelist) releaseRange(begin, end common.Txid) {
	if begin > end {
		return
	}
	var m common.Pgids
	for tid, txp := range f.pending {
		if tid < begin || tid > end {
			continue
		}
		// Don't recompute freed pages if ranges haven't updated.
		if txp.lastReleaseBegin == begin {
			continue
		}
		for i := 0; i < len(txp.ids); i++ {
			if atx := txp.alloctx[i]; atx < begin || atx > end {
				continue
			}
			m = append(m, txp.ids[i])
			txp.ids[i] = txp.ids[len(txp.ids)-1]
			txp.ids = txp.ids[:len(txp.ids)-1]
			txp.alloctx[i] = txp.alloctx[len(txp.alloctx)-1]
			txp.alloctx = txp.alloctx[:len(txp.alloctx)-1]
			i--
		}
		txp.lastReleaseBegin = begin
		if len(txp.ids) == 0 {
			delete(f.pending, tid)
		}
	}
	f.mergeSpans(m)
}

// rollback removes the pages from a given pending tx.
func (f *freelist) rollback(txid common.Txid) {
	// Remove page ids from cache.
	txp := f.pending[txid]
	if txp == nil {
		return
	}
	var m common.Pgids
	for i, pgid := range txp.ids {
		delete(f.cache, pgid)
		tx := txp.alloctx[i]
		if tx == 0 {
			continue
		}
		if tx != txid {
			// Pending free aborted; restore page back to alloc list.
			f.allocs[pgid] = tx
		} else {
			// Freed page was allocated by this txn; OK to throw away.
			m = append(m, pgid)
		}
	}
	// Remove pages from pending list and mark as free if allocated by txid.
	delete(f.pending, txid)
	f.mergeSpans(m)
}

// freed returns whether a given page is in the free list.
func (f *freelist) freed(pgId common.Pgid) bool {
	_, ok := f.cache[pgId]
	return ok
}

// read initializes the freelist from a freelist page.
func (f *freelist) read(p *common.Page) {
	if !p.IsFreelistPage() {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.Id(), p.Typ()))
	}

	ids := p.FreelistPageIds()

	// Copy the list of page ids from the freelist.
	if len(ids) == 0 {
		f.ids = nil
	} else {
		// copy the ids, so we don't modify on the freelist page directly
		idsCopy := make([]common.Pgid, len(ids))
		copy(idsCopy, ids)
		// Make sure they're sorted.
		sort.Sort(common.Pgids(idsCopy))

		f.readIDs(idsCopy)
	}
}

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
func (f *freelist) write(p *common.Page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	p.SetFlags(common.FreelistPageFlag)

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	l := f.count()
	if l == 0 {
		p.SetCount(uint16(l))
	} else if l < 0xFFFF {
		p.SetCount(uint16(l))
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l)
		f.copyall(ids)
	} else {
		p.SetCount(0xFFFF)
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l+1)
		ids[0] = common.Pgid(l)
		f.copyall(ids[1:])
	}

	return nil
}

// reload reads the freelist from a page and filters out pending items.
func (f *freelist) reload(p *common.Page) {
	f.read(p)

	// Build a cache of only pending pages.
	pcache := make(map[common.Pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []common.Pgid
	for _, id := range f.getFreePageIDs() {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

// noSyncReload reads the freelist from Pgids and filters out pending items.
func (f *freelist) noSyncReload(Pgids []common.Pgid) {
	// Build a cache of only pending pages.
	pcache := make(map[common.Pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []common.Pgid
	for _, id := range Pgids {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *freelist) reindex() {
	ids := f.getFreePageIDs()
	f.cache = make(map[common.Pgid]struct{}, len(ids))
	for _, id := range ids {
		f.cache[id] = struct{}{}
	}
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			f.cache[pendingID] = struct{}{}
		}
	}
}

func (f *freelist) addReadonlyTXID(tid common.Txid) {
	f.readonlyTXIDs = append(f.readonlyTXIDs, tid)
}

func (f *freelist) removeReadonlyTXID(tid common.Txid) {
	for i := range f.readonlyTXIDs {
		if f.readonlyTXIDs[i] == tid {
			last := len(f.readonlyTXIDs) - 1
			f.readonlyTXIDs[i] = f.readonlyTXIDs[last]
			f.readonlyTXIDs = f.readonlyTXIDs[:last]
			break
		}
	}
}

type txIDx []common.Txid

func (t txIDx) Len() int           { return len(t) }
func (t txIDx) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t txIDx) Less(i, j int) bool { return t[i] < t[j] }

// freePages releases any pages associated with closed read-only transactions.
func (f *freelist) freePages() {
	// Free all pending pages prior to the earliest open transaction.
	sort.Sort(txIDx(f.readonlyTXIDs))
	minid := common.Txid(math.MaxUint64)
	if len(f.readonlyTXIDs) > 0 {
		minid = f.readonlyTXIDs[0]
	}
	if minid > 0 {
		f.release(minid - 1)
	}
	// Release unused txid extents.
	for _, tid := range f.readonlyTXIDs {
		f.releaseRange(minid, tid-1)
		minid = tid + 1
	}
	f.releaseRange(minid, common.Txid(math.MaxUint64))
	// Any page both allocated and freed in an extent is safe to release.
}

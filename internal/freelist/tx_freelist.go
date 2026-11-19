package freelist

import (
	"fmt"
	"math"
	"slices"
	"sort"

	"go.etcd.io/bbolt/internal/common"
)

type txPending struct {
	ids              []common.Pgid
	alloctx          []common.Txid // txids allocating the ids
	lastReleaseBegin common.Txid   // beginning txid of last matching releaseRange
}

// txFreelist tracks free and pending pages across transactions. The free pages
// themselves are stored in a backend, the on-disk format is handled by a codec.
type txFreelist struct {
	backend backend // stores the free pages.
	codec   codec   // serializes the free and pending page ids into a freelist page.

	readonlyTXIDs []common.Txid               // all readonly transaction IDs.
	allocs        map[common.Pgid]common.Txid // mapping of Txid that allocated a pgid.
	cache         map[common.Pgid]struct{}    // fast lookup of all free and pending page ids.
	pending       map[common.Txid]*txPending  // mapping of soon-to-be free page ids by tx.
}

var _ Interface = (*txFreelist)(nil)

func newTxFreelist(b backend) *txFreelist {
	return &txFreelist{
		backend: b,
		codec:   pgidsCodec{},
		pending: make(map[common.Txid]*txPending),
		allocs:  make(map[common.Pgid]common.Txid),
		cache:   make(map[common.Pgid]struct{}),
	}
}

// init replaces all free pages with the given ids and rebuilds the freed cache.
// Pending pages are kept as they are. It panics if ids are not sorted.
func (t *txFreelist) init(ids common.Pgids) {
	verifySorted(ids, "init")
	t.backend.init(ids)
	t.reindex()
}

func (t *txFreelist) Allocate(txid common.Txid, n int) common.Pgid {
	id := t.backend.allocate(n)
	if id == 0 {
		return 0
	}
	// Remove from the free cache.
	for i := common.Pgid(0); i < common.Pgid(n); i++ {
		delete(t.cache, id+i)
	}
	t.allocs[id] = txid
	return id
}

func (t *txFreelist) FreeCount() int {
	return t.backend.freeCount()
}

// freePageIds returns the sorted IDs of all free pages. The returned slice must not be modified.
func (t *txFreelist) freePageIds() common.Pgids {
	return t.backend.freePageIds()
}

// pendingByTx returns all pending pages by transaction id.
func (t *txFreelist) pendingByTx() map[common.Txid]*txPending {
	return t.pending
}

func (t *txFreelist) PendingCount() int {
	var count int
	for _, txp := range t.pending {
		count += len(txp.ids)
	}
	return count
}

func (t *txFreelist) Count() int {
	return t.FreeCount() + t.PendingCount()
}

func (t *txFreelist) Freed(pgId common.Pgid) bool {
	_, ok := t.cache[pgId]
	return ok
}

func (t *txFreelist) Free(txid common.Txid, p *common.Page) {
	if p.Id() <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.Id()))
	}

	// Free page and all its overflow pages.
	txp := t.pending[txid]
	if txp == nil {
		txp = &txPending{}
		t.pending[txid] = txp
	}
	allocTxid, ok := t.allocs[p.Id()]
	common.Verify(func() {
		if allocTxid == txid {
			panic(fmt.Sprintf("free: freed page (%d) was allocated by the same transaction (%d)", p.Id(), txid))
		}
	})
	if ok {
		delete(t.allocs, p.Id())
	}

	for id := p.Id(); id <= p.Id()+common.Pgid(p.Overflow()); id++ {
		// Verify that page is not already free.
		if _, ok := t.cache[id]; ok {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		// Add to the freelist and cache.
		txp.ids = append(txp.ids, id)
		txp.alloctx = append(txp.alloctx, allocTxid)
		t.cache[id] = struct{}{}
	}
}

func (t *txFreelist) Rollback(txid common.Txid) {
	// Remove page ids from cache.
	txp := t.pending[txid]
	if txp == nil {
		return
	}
	for i, pgid := range txp.ids {
		delete(t.cache, pgid)
		tx := txp.alloctx[i]
		if tx == 0 {
			continue
		}
		if tx != txid {
			// Pending free aborted; restore page back to alloc list.
			t.allocs[pgid] = tx
		} else {
			// A writing TXN should never free a page which was allocated by itself.
			panic(fmt.Sprintf("rollback: freed page (%d) was allocated by the same transaction (%d)", pgid, txid))
		}
	}
	// Remove pages from pending list and mark as free if allocated by txid.
	delete(t.pending, txid)

	// Remove pgids which are allocated by this txid
	for pgid, tid := range t.allocs {
		if tid == txid {
			delete(t.allocs, pgid)
		}
	}
}

func (t *txFreelist) AddReadonlyTXID(tid common.Txid) {
	t.readonlyTXIDs = append(t.readonlyTXIDs, tid)
}

func (t *txFreelist) RemoveReadonlyTXID(tid common.Txid) {
	for i := range t.readonlyTXIDs {
		if t.readonlyTXIDs[i] == tid {
			last := len(t.readonlyTXIDs) - 1
			t.readonlyTXIDs[i] = t.readonlyTXIDs[last]
			t.readonlyTXIDs = t.readonlyTXIDs[:last]
			break
		}
	}
}

type txIDx []common.Txid

func (t txIDx) Len() int           { return len(t) }
func (t txIDx) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t txIDx) Less(i, j int) bool { return t[i] < t[j] }

func (t *txFreelist) ReleasePendingPages() {
	// Free all pending pages prior to the earliest open transaction.
	sort.Sort(txIDx(t.readonlyTXIDs))
	minid := common.Txid(math.MaxUint64)
	if len(t.readonlyTXIDs) > 0 {
		minid = t.readonlyTXIDs[0]
	}
	if minid > 0 {
		t.release(minid - 1)
	}
	// Release unused txid extents.
	for _, tid := range t.readonlyTXIDs {
		t.releaseRange(minid, tid-1)
		minid = tid + 1
	}
	t.releaseRange(minid, common.Txid(math.MaxUint64))
	// Any page both allocated and freed in an extent is safe to release.
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (t *txFreelist) release(txid common.Txid) {
	m := make(common.Pgids, 0)
	for tid, txp := range t.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, txp.ids...)
			delete(t.pending, tid)
		}
	}
	t.backend.mergeSpans(m)
}

// releaseRange moves pending pages allocated within an extent [begin,end] to the free list.
func (t *txFreelist) releaseRange(begin, end common.Txid) {
	if begin > end {
		return
	}
	m := common.Pgids{}
	for tid, txp := range t.pending {
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
			delete(t.pending, tid)
		}
	}
	t.backend.mergeSpans(m)
}

// pendingPageIds returns the sorted IDs of all pending pages. Returns an empty slice if no pages are pending.
func (t *txFreelist) pendingPageIds() common.Pgids {
	m := make(common.Pgids, 0, t.PendingCount())
	for _, txp := range t.pending {
		m = append(m, txp.ids...)
	}
	sort.Sort(m)
	return m
}

func (t *txFreelist) FreeAndPendingPages() common.Pgids {
	free, pending := t.freePageIds(), t.pendingPageIds()
	// always copy, freePageIds may alias internal state
	all := make(common.Pgids, len(free)+len(pending))
	common.Mergepgids(all, free, pending)
	return all
}

func (t *txFreelist) Read(p *common.Page) {
	t.init(t.codec.read(p))
}

func (t *txFreelist) Write(p *common.Page) {
	t.codec.write(p, t.freePageIds(), t.pendingPageIds())
}

func (t *txFreelist) EstimatedWritePageSize() int {
	return t.codec.estimatedWritePageSize(t.Count())
}

func (t *txFreelist) Reload(p *common.Page) {
	t.InitFromPageIds(t.codec.read(p))
}

func (t *txFreelist) InitFromPageIds(pgIds common.Pgids) {
	// Build a cache of only pending pages.
	pcache := make(map[common.Pgid]struct{})
	for _, txp := range t.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = struct{}{}
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	a := []common.Pgid{}
	for _, id := range pgIds {
		if _, ok := pcache[id]; !ok {
			a = append(a, id)
		}
	}

	t.init(a)
}

// reindex rebuilds the free cache based on available and pending free lists.
func (t *txFreelist) reindex() {
	free := t.freePageIds()
	t.cache = make(map[common.Pgid]struct{}, len(free))
	for _, id := range free {
		t.cache[id] = struct{}{}
	}
	for _, txp := range t.pending {
		for _, pendingID := range txp.ids {
			t.cache[pendingID] = struct{}{}
		}
	}
}

// verifySorted panics if the given page ids are not sorted in ascending order.
// Implementations that rely on sorted input must call this before mutating any state.
func verifySorted(ids common.Pgids, caller string) {
	if !slices.IsSorted(ids) {
		panic(fmt.Sprintf("%s: pgids not sorted", caller))
	}
}

package freelist

import (
	"go.etcd.io/bbolt/internal/common"
)

type ReadWriter interface {
	// Read initializes this freelist with the page ids stored in the given page.
	Read(page *common.Page)

	// Write writes the freelist into the given page.
	Write(page *common.Page)

	// EstimatedWritePageSize returns the size in bytes of the freelist after serialization in Write.
	// This should never underestimate the size.
	EstimatedWritePageSize() int
}

type Interface interface {
	ReadWriter

	// InitFromPageIds initializes the free pages with the given page ids, filtering out
	// any ids that are currently pending. It panics if pgIds are not sorted.
	InitFromPageIds(pgIds common.Pgids)

	// Allocate tries to allocate the given number of contiguous pages
	// from the free list pages. It returns the starting page ID if
	// available; otherwise, it returns 0.
	Allocate(txid common.Txid, numPages int) common.Pgid

	// Count returns the number of free and pending pages.
	Count() int

	// FreeCount returns the number of free pages.
	FreeCount() int

	// PendingCount returns the number of pending pages.
	PendingCount() int

	// AddReadonlyTXID adds a given read-only transaction id for pending page tracking.
	AddReadonlyTXID(txid common.Txid)

	// RemoveReadonlyTXID removes a given read-only transaction id for pending page tracking.
	RemoveReadonlyTXID(txid common.Txid)

	// ReleasePendingPages releases any pages associated with closed read-only transactions.
	ReleasePendingPages()

	// Free releases a page and its overflow for a given transaction id.
	// If the page is already free or is one of the meta pages, then a panic will occur.
	Free(txId common.Txid, p *common.Page)

	// Freed returns whether a given page is in the free list.
	Freed(pgId common.Pgid) bool

	// Rollback removes the pages from a given pending tx.
	Rollback(txId common.Txid)

	// FreeAndPendingPages returns a sorted copy of all free and pending page ids.
	FreeAndPendingPages() common.Pgids

	// Reload reads the freelist from a page and filters out pending items.
	Reload(p *common.Page)
}

// backend stores the free pages and implements the allocation strategy.
// It knows nothing about transactions; txFreelist owns a backend and does all the
// transaction bookkeeping (pending pages, allocations, the freed cache) around it.
type backend interface {
	// init replaces all free pages with the given sorted ids.
	init(ids common.Pgids)

	// allocate removes n contiguous free pages and returns the first page id,
	// or 0 if there is no span of n contiguous free pages.
	allocate(n int) common.Pgid

	// freeCount returns the number of free pages.
	freeCount() int

	// freePageIds returns the sorted IDs of all free pages. Returns an empty slice if no free pages are available.
	// The returned slice may alias internal state and must not be modified.
	freePageIds() common.Pgids

	// mergeSpans is merging the given pages into the free pages.
	mergeSpans(ids common.Pgids)
}

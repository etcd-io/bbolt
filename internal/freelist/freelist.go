package freelist

import (
	"go.etcd.io/bbolt/internal/common"
)

type ReadWriter interface {
	// Read calls Init with the page ids stored in the given page.
	Read(f Interface, page *common.Page)

	// Write writes the freelist into the given page.
	Write(f Interface, page *common.Page)

	// EstimatedWritePageSize returns the size of the freelist after serialization in Write.
	// This should never underestimate the size.
	EstimatedWritePageSize(f Interface) int
}

type allocator interface {
	// Init initializes this freelist with the given list of pages.
	Init(ids common.Pgids)

	// FreeCount returns the number of free pages.
	FreeCount() int

	// FreePageIds returns the IDs of all free pages.
	FreePageIds() common.Pgids

	// mergeSpans is merging the given pages into the freelist
	mergeSpans(ids common.Pgids)

	// TODO(thomas): this is necessary to decouple, but leaks internals
	alloc(txid common.Txid, numPages int, allocs *map[common.Pgid]common.Txid, cache *map[common.Pgid]struct{}) common.Pgid
}

type txManager interface {
	// AddReadonlyTXID adds a given read-only transaction id for pending page tracking.
	AddReadonlyTXID(txid common.Txid)

	// RemoveReadonlyTXID removes a given read-only transaction id for pending page tracking.
	RemoveReadonlyTXID(txid common.Txid)

	// ReleasePendingPages releases any pages associated with closed read-only transactions.
	ReleasePendingPages()

	// pendingPageIds returns all pending pages by transaction id.
	pendingPageIds() map[common.Txid]*txPending

	// release moves all page ids for a transaction id (or older) to the freelist.
	release(txId common.Txid)

	// releaseRange moves pending pages allocated within an extent [begin,end] to the free list.
	releaseRange(begin, end common.Txid)
}

type Interface interface {
	allocator
	txManager

	// Allocate tries to allocate the given number of contiguous pages
	// from the free list pages. It returns the starting page ID if
	// available; otherwise, it returns 0.
	Allocate(txid common.Txid, numPages int) common.Pgid

	// Count returns the number of free and pending pages.
	Count() int

	// PendingCount returns the number of pending pages.
	PendingCount() int

	// Free releases a page and its overflow for a given transaction id.
	// If the page is already free then a panic will occur.
	Free(txId common.Txid, p *common.Page)

	// Freed returns whether a given page is in the free list.
	Freed(pgId common.Pgid) bool

	// Rollback removes the pages from a given pending tx.
	Rollback(txId common.Txid)

	// List returns a list of all free ids and all pending ids in one sorted list.
	List() common.Pgids

	// Reload reads the freelist from Pgids and filters out pending items.
	Reload(pgIds common.Pgids)
}

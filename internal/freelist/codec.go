package freelist

import (
	"fmt"
	"sort"
	"unsafe"

	"go.etcd.io/bbolt/internal/common"
)

// codec encodes and decodes page ids into and from a freelist page.
// It is decoupled from any freelist implementation and only operates on page id slices,
// which allows swapping the on-disk encoding without touching the freelist itself.
type codec interface {
	// read returns a sorted copy of the page ids stored in the given freelist page.
	// It panics if the page is not a freelist page.
	read(page *common.Page) common.Pgids

	// write writes the union of the given page id lists into the given page.
	// The two lists must not contain duplicates between each other.
	// It panics if either list is not sorted.
	write(page *common.Page, free common.Pgids, pending common.Pgids)

	// estimatedWritePageSize returns the size in bytes of a freelist page holding count
	// page ids after serialization in write. This should never underestimate the size.
	estimatedWritePageSize(count int) int
}

// pgidsCodec stores the page ids as a plain array of uint64.
type pgidsCodec struct{}

func (pgidsCodec) read(p *common.Page) common.Pgids {
	if !p.IsFreelistPage() {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.Id(), p.Typ()))
	}

	ids := p.FreelistPageIds()
	if len(ids) == 0 {
		return common.Pgids{}
	}

	// copy the ids, so we don't modify on the freelist page directly
	idsCopy := make(common.Pgids, len(ids))
	copy(idsCopy, ids)
	// Make sure they're sorted.
	sort.Sort(idsCopy)
	return idsCopy
}

func (pgidsCodec) estimatedWritePageSize(count int) int {
	n := count
	if n >= 0xFFFF {
		// The first element will be used to store the count. See write.
		n++
	}
	return int(common.PageHeaderSize) + (int(unsafe.Sizeof(common.Pgid(0))) * n)
}

func (pgidsCodec) write(p *common.Page, free common.Pgids, pending common.Pgids) {
	verifySorted(free, "write(free)")
	verifySorted(pending, "write(pending)")

	// Update the header flag.
	p.SetFlags(common.FreelistPageFlag)

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	l := len(free) + len(pending)
	if l == 0 {
		p.SetCount(uint16(l))
	} else if l < 0xFFFF {
		p.SetCount(uint16(l))
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l)
		common.Mergepgids(ids, free, pending)
	} else {
		p.SetCount(0xFFFF)
		data := common.UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		ids := unsafe.Slice((*common.Pgid)(data), l+1)
		ids[0] = common.Pgid(l)
		common.Mergepgids(ids[1:], free, pending)
	}
}

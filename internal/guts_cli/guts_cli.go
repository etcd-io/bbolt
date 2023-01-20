package guts_cli

// Low level access to pages / data-structures of the bbolt file.

// TODO(ptab): Merge with bbolt/page file that should get ported to internal.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

var (
	// ErrCorrupt is returned when a checking a data file finds errors.
	ErrCorrupt = errors.New("invalid value")
)

// PageHeaderSize represents the size of the bolt.Page header.
const PageHeaderSize = 16

// Represents a marker value to indicate that a file (Meta Page) is a Bolt DB.
const magic uint32 = 0xED0CDAED

// DO NOT EDIT. Copied from the "bolt" package.
const maxAllocSize = 0xFFFFFFF

// DO NOT EDIT. Copied from the "bolt" package.
const (
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10
)

// DO NOT EDIT. Copied from the "bolt" package.
const bucketLeafFlag = 0x01

// DO NOT EDIT. Copied from the "bolt" package.
type Pgid uint64

// DO NOT EDIT. Copied from the "bolt" package.
type txid uint64

// DO NOT EDIT. Copied from the "bolt" package.
type Meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     Bucket
	freelist Pgid
	pgid     Pgid // High Water Mark (id of next added Page if the file growths)
	txid     txid
	checksum uint64
}

func LoadPageMeta(buf []byte) *Meta {
	return (*Meta)(unsafe.Pointer(&buf[PageHeaderSize]))
}

func (m *Meta) RootBucket() *Bucket {
	return &m.root
}

func (m *Meta) Txid() uint64 {
	return uint64(m.txid)
}

func (m *Meta) Print(w io.Writer) {
	fmt.Fprintf(w, "Version:    %d\n", m.version)
	fmt.Fprintf(w, "Page Size:  %d bytes\n", m.pageSize)
	fmt.Fprintf(w, "Flags:      %08x\n", m.flags)
	fmt.Fprintf(w, "Root:       <pgid=%d>\n", m.root.root)
	fmt.Fprintf(w, "Freelist:   <pgid=%d>\n", m.freelist)
	fmt.Fprintf(w, "HWM:        <pgid=%d>\n", m.pgid)
	fmt.Fprintf(w, "Txn ID:     %d\n", m.txid)
	fmt.Fprintf(w, "Checksum:   %016x\n", m.checksum)
	fmt.Fprintf(w, "\n")
}

// DO NOT EDIT. Copied from the "bolt" package.
type Bucket struct {
	root     Pgid
	sequence uint64
}

const bucketHeaderSize = int(unsafe.Sizeof(Bucket{}))

func LoadBucket(buf []byte) *Bucket {
	return (*Bucket)(unsafe.Pointer(&buf[0]))
}

func (b *Bucket) String() string {
	return fmt.Sprintf("<pgid=%d,seq=%d>", b.root, b.sequence)
}

func (b *Bucket) RootPage() Pgid {
	return b.root
}

func (b *Bucket) InlinePage(v []byte) *Page {
	return (*Page)(unsafe.Pointer(&v[bucketHeaderSize]))
}

// DO NOT EDIT. Copied from the "bolt" package.
type Page struct {
	id       Pgid
	flags    uint16
	count    uint16
	overflow uint32
	ptr      uintptr
}

func LoadPage(buf []byte) *Page {
	return (*Page)(unsafe.Pointer(&buf[0]))
}

func (p *Page) FreelistPageCount() int {
	// Check for overflow and, if present, adjust actual element count.
	if p.count == 0xFFFF {
		return int(((*[maxAllocSize]Pgid)(unsafe.Pointer(&p.ptr)))[0])
	} else {
		return int(p.count)
	}
}

func (p *Page) FreelistPagePages() []Pgid {
	// Check for overflow and, if present, adjust starting index.
	idx := 0
	if p.count == 0xFFFF {
		idx = 1
	}
	return (*[maxAllocSize]Pgid)(unsafe.Pointer(&p.ptr))[idx:p.FreelistPageCount()]
}

func (p *Page) Overflow() uint32 {
	return p.overflow
}

func (p *Page) String() string {
	return fmt.Sprintf("ID: %d, Type: %s, count: %d, overflow: %d", p.id, p.Type(), p.count, p.overflow)
}

// DO NOT EDIT. Copied from the "bolt" package.

// TODO(ptabor): Make the page-types an enum.
func (p *Page) Type() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

func (p *Page) Count() uint16 {
	return p.count
}

func (p *Page) Id() Pgid {
	return p.id
}

// DO NOT EDIT. Copied from the "bolt" package.
func (p *Page) LeafPageElement(index uint16) *LeafPageElement {
	n := &((*[0x7FFFFFF]LeafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// DO NOT EDIT. Copied from the "bolt" package.
func (p *Page) BranchPageElement(index uint16) *BranchPageElement {
	return &((*[0x7FFFFFF]BranchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

func (p *Page) SetId(target Pgid) {
	p.id = target
}

func (p *Page) SetCount(target uint16) {
	p.count = target
}

func (p *Page) SetOverflow(target uint32) {
	p.overflow = target
}

// DO NOT EDIT. Copied from the "bolt" package.
type BranchPageElement struct {
	pos   uint32
	ksize uint32
	pgid  Pgid
}

// DO NOT EDIT. Copied from the "bolt" package.
func (n *BranchPageElement) Key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos : n.pos+n.ksize]
}

func (n *BranchPageElement) PgId() Pgid {
	return n.pgid
}

// DO NOT EDIT. Copied from the "bolt" package.
type LeafPageElement struct {
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

// DO NOT EDIT. Copied from the "bolt" package.
func (n *LeafPageElement) Key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos : n.pos+n.ksize]
}

// DO NOT EDIT. Copied from the "bolt" package.
func (n *LeafPageElement) Value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return buf[n.pos+n.ksize : n.pos+n.ksize+n.vsize]
}

func (n *LeafPageElement) IsBucketEntry() bool {
	return n.flags&uint32(bucketLeafFlag) != 0
}

func (n *LeafPageElement) Bucket() *Bucket {
	if n.IsBucketEntry() {
		return LoadBucket(n.Value())
	} else {
		return nil
	}
}

// ReadPage reads Page info & full Page data from a path.
// This is not transactionally safe.
func ReadPage(path string, pageID uint64) (*Page, []byte, error) {
	// Find Page size.
	pageSize, hwm, err := ReadPageAndHWMSize(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read Page size: %s", err)
	}

	// Open database file.
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	// Read one block into buffer.
	buf := make([]byte, pageSize)
	if n, err := f.ReadAt(buf, int64(pageID*pageSize)); err != nil {
		return nil, nil, err
	} else if n != len(buf) {
		return nil, nil, io.ErrUnexpectedEOF
	}

	// Determine total number of blocks.
	p := LoadPage(buf)
	if p.id != Pgid(pageID) {
		return nil, nil, fmt.Errorf("error: %w due to unexpected Page id: %d != %d", ErrCorrupt, p.id, pageID)
	}
	overflowN := p.overflow
	if overflowN >= uint32(hwm)-3 { // we exclude 2 Meta pages and the current Page.
		return nil, nil, fmt.Errorf("error: %w, Page claims to have %d overflow pages (>=hwm=%d). Interrupting to avoid risky OOM", ErrCorrupt, overflowN, hwm)
	}

	// Re-read entire Page (with overflow) into buffer.
	buf = make([]byte, (uint64(overflowN)+1)*pageSize)
	if n, err := f.ReadAt(buf, int64(pageID*pageSize)); err != nil {
		return nil, nil, err
	} else if n != len(buf) {
		return nil, nil, io.ErrUnexpectedEOF
	}
	p = LoadPage(buf)
	if p.id != Pgid(pageID) {
		return nil, nil, fmt.Errorf("error: %w due to unexpected Page id: %d != %d", ErrCorrupt, p.id, pageID)
	}

	return p, buf, nil
}

func WritePage(path string, pageBuf []byte) error {
	page := LoadPage(pageBuf)
	pageSize, _, err := ReadPageAndHWMSize(path)
	if err != nil {
		return err
	}
	expectedLen := pageSize * (uint64(page.Overflow()) + 1)
	if expectedLen != uint64(len(pageBuf)) {
		return fmt.Errorf("WritePage: len(buf):%d != pageSize*(overflow+1):%d", len(pageBuf), expectedLen)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteAt(pageBuf, int64(page.Id())*int64(pageSize))
	return err
}

// ReadPageAndHWMSize reads Page size and HWM (id of the last+1 Page).
// This is not transactionally safe.
func ReadPageAndHWMSize(path string) (uint64, Pgid, error) {
	// Open database file.
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	// Read 4KB chunk.
	buf := make([]byte, 4096)
	if _, err := io.ReadFull(f, buf); err != nil {
		return 0, 0, err
	}

	// Read Page size from metadata.
	m := LoadPageMeta(buf)
	if m.magic != magic {
		return 0, 0, fmt.Errorf("the Meta Page has wrong (unexpected) magic")
	}
	return uint64(m.pageSize), Pgid(m.pgid), nil
}

// GetRootPage returns the root-page (according to the most recent transaction).
func GetRootPage(path string) (root Pgid, activeMeta Pgid, err error) {
	_, buf0, err0 := ReadPage(path, 0)
	if err0 != nil {
		return 0, 0, err0
	}
	m0 := LoadPageMeta(buf0)
	_, buf1, err1 := ReadPage(path, 1)
	if err1 != nil {
		return 0, 1, err1
	}
	m1 := LoadPageMeta(buf1)
	if m0.txid < m1.txid {
		return m1.root.root, 1, nil
	} else {
		return m0.root.root, 0, nil
	}
}

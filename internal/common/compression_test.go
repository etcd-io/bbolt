package common

import (
	"testing"
	"unsafe"
)

func TestCompressDecompressInodes(t *testing.T) {
	const pageSize = 4096
	const numInodes = 30
	const valSize = 500

	// Build inodes with highly compressible data that spans multiple
	// pages uncompressed so that compression can actually reduce the
	// page count.
	inodes := make(Inodes, numInodes)
	for i := range inodes {
		inodes[i].SetKey([]byte{byte(i + 1)})
		val := make([]byte, valSize)
		for j := range val {
			val[j] = byte(i)
		}
		inodes[i].SetValue(val)
	}

	// Sanity: verify uncompressed data spans more than 1 page.
	uncompressedSize := int(PageHeaderSize) + numInodes*(int(LeafPageElementSize)+1+valSize)
	if uncompressedSize <= pageSize {
		t.Fatalf("test data should span multiple pages, only %d bytes", uncompressedSize)
	}

	// Compress the inodes.
	compressed := CompressInodes(inodes, true, pageSize)
	if compressed == nil {
		t.Fatal("expected compression to succeed for highly compressible multi-page data")
	}

	// Verify compressed buffer is page-aligned.
	if len(compressed)%pageSize != 0 {
		t.Fatalf("compressed buffer size (%d) is not page-aligned", len(compressed))
	}

	// Verify the compressed page header.
	cp := (*Page)(unsafe.Pointer(&compressed[0]))
	if !cp.IsCompressed() {
		t.Fatal("compressed page should have compressed flag set")
	}
	if !cp.IsLeafPage() {
		t.Fatal("compressed page should still be a leaf page")
	}
	if cp.Count() != numInodes {
		t.Fatalf("expected count %d, got %d", numInodes, cp.Count())
	}

	// Verify it actually reduced page count.
	compressedPages := len(compressed) / pageSize
	uncompressedPages := (uncompressedSize + pageSize - 1) / pageSize
	if compressedPages >= uncompressedPages {
		t.Fatalf("compressed pages (%d) should be fewer than uncompressed (%d)",
			compressedPages, uncompressedPages)
	}

	// Decompress the page.
	dp, _, err := DecompressPage(cp, pageSize)
	if err != nil {
		t.Fatalf("decompression failed: %v", err)
	}

	// Verify the decompressed page.
	if dp.IsCompressed() {
		t.Fatal("decompressed page should not have compressed flag")
	}
	if !dp.IsLeafPage() {
		t.Fatal("decompressed page should be a leaf page")
	}
	if dp.Count() != numInodes {
		t.Fatalf("expected count %d, got %d", numInodes, dp.Count())
	}

	// Read back the inodes and verify.
	dInodes := ReadInodeFromPage(dp)
	if len(dInodes) != numInodes {
		t.Fatalf("expected %d inodes, got %d", numInodes, len(dInodes))
	}
	for i, in := range dInodes {
		if len(in.Key()) != 1 || in.Key()[0] != byte(i+1) {
			t.Fatalf("inode %d: key mismatch", i)
		}
		if len(in.Value()) != valSize {
			t.Fatalf("inode %d: value length %d, expected %d", i, len(in.Value()), valSize)
		}
		for j, b := range in.Value() {
			if b != byte(i) {
				t.Fatalf("inode %d value byte %d: expected 0x%02x, got 0x%02x", i, j, byte(i), b)
			}
		}
	}
}

func TestCompressInodes_NoSaving(t *testing.T) {
	const pageSize = 4096

	// Build inodes with random (incompressible) data.
	// Fill a single page worth of data — compression can't reduce below 1 page.
	inodes := make(Inodes, 1)
	inodes[0].SetKey([]byte("key1"))
	val := make([]byte, pageSize/2)
	state := uint64(0xdeadbeefcafebabe)
	for i := range val {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		val[i] = byte(state)
	}
	inodes[0].SetValue(val)

	// If the data fits in 1 page uncompressed, compression can't reduce
	// the page count, so CompressInodes should return nil.
	compressed := CompressInodes(inodes, true, pageSize)
	if compressed != nil {
		t.Fatal("expected nil when compression cannot reduce page count")
	}
}

func TestCompressInodes_LargeOverflow(t *testing.T) {
	const pageSize = 4096

	// Build inodes with lots of highly compressible data that span
	// multiple pages uncompressed.
	inodes := make(Inodes, 50)
	for i := range inodes {
		inodes[i].SetKey([]byte{byte(i + 1)}) // nonzero key
		val := make([]byte, 500)
		for j := range val {
			val[j] = byte(i)
		}
		inodes[i].SetValue(val)
	}

	compressed := CompressInodes(inodes, true, pageSize)
	if compressed == nil {
		t.Fatal("expected compression to succeed for multi-page compressible data")
	}

	// The compressed buffer should be fewer pages.
	compressedPages := len(compressed) / pageSize
	uncompressedSize := int(PageHeaderSize) + 50*(int(LeafPageElementSize)+1+500)
	uncompressedPages := (uncompressedSize + pageSize - 1) / pageSize
	if compressedPages >= uncompressedPages {
		t.Fatalf("compressed pages (%d) should be less than uncompressed pages (%d)",
			compressedPages, uncompressedPages)
	}

	// Verify round-trip.
	cp := (*Page)(unsafe.Pointer(&compressed[0]))
	dp, _, err := DecompressPage(cp, pageSize)
	if err != nil {
		t.Fatalf("decompression failed: %v", err)
	}
	dInodes := ReadInodeFromPage(dp)
	if len(dInodes) != 50 {
		t.Fatalf("expected 50 inodes, got %d", len(dInodes))
	}
	for i, in := range dInodes {
		if len(in.Key()) != 1 || in.Key()[0] != byte(i+1) {
			t.Fatalf("inode %d: key mismatch", i)
		}
		if len(in.Value()) != 500 {
			t.Fatalf("inode %d: value length %d, expected 500", i, len(in.Value()))
		}
		for j, b := range in.Value() {
			if b != byte(i) {
				t.Fatalf("inode %d value byte %d: expected 0x%02x, got 0x%02x", i, j, byte(i), b)
			}
		}
	}
}

func TestPageFlags_CompressedWithType(t *testing.T) {
	p := &Page{}

	p.SetFlags(LeafPageFlag | CompressedPageFlag)
	if !p.IsLeafPage() {
		t.Fatal("should be a leaf page")
	}
	if !p.IsCompressed() {
		t.Fatal("should be compressed")
	}
	if p.IsBranchPage() {
		t.Fatal("should not be a branch page")
	}

	p.SetFlags(BranchPageFlag | CompressedPageFlag)
	if !p.IsBranchPage() {
		t.Fatal("should be a branch page")
	}
	if !p.IsCompressed() {
		t.Fatal("should be compressed")
	}
	if p.IsLeafPage() {
		t.Fatal("should not be a leaf page")
	}

	p.SetCompressed(false)
	if p.IsCompressed() {
		t.Fatal("should not be compressed after clearing")
	}
	if !p.IsBranchPage() {
		t.Fatal("should still be a branch page after clearing compressed flag")
	}
}

func TestFastCheck_CompressedPage(t *testing.T) {
	p := &Page{}
	p.SetId(42)
	p.SetFlags(LeafPageFlag | CompressedPageFlag)
	p.FastCheck(42)

	p.SetFlags(BranchPageFlag | CompressedPageFlag)
	p.FastCheck(42)
}

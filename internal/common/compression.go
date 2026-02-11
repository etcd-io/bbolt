package common

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/golang/snappy"
)

// compressedDataLenSize is the size in bytes used to store the compressed
// data length right after the page header in a compressed page. This allows
// the decompressor to know exactly how many bytes to read.
const compressedDataLenSize = 4

// CompressInodes serializes the given inodes into a temporary page buffer,
// compresses the data portion, and returns the compressed page buffer that
// can be copied directly into the allocated page. The returned buffer
// includes the page header (with the compressed flag set), the 4-byte
// compressed length, and the compressed data.
//
// If compression does not reduce the number of pages needed, nil is returned
// to signal that the node should be written uncompressed.
//
// On success, the caller should allocate ceil(len(result)/pageSize) pages
// and copy the result into the allocated page buffer.
func CompressInodes(inodes Inodes, isLeaf bool, pageSize int) []byte {
	// First, figure out how large the uncompressed page would be.
	uncompressedSize := int(PageHeaderSize)
	var elemSize uintptr
	if isLeaf {
		elemSize = LeafPageElementSize
	} else {
		elemSize = BranchPageElementSize
	}
	for i := 0; i < len(inodes); i++ {
		uncompressedSize += int(elemSize) + len(inodes[i].Key()) + len(inodes[i].Value())
	}
	uncompressedPages := (uncompressedSize + pageSize - 1) / pageSize

	// Serialize the inodes into a scratch buffer large enough for the
	// uncompressed page(s). We need a real page layout so that
	// WriteInodeToPage can use unsafe pointer arithmetic.
	scratchSize := uncompressedPages * pageSize
	scratch := make([]byte, scratchSize)
	p := (*Page)(unsafe.Pointer(&scratch[0]))
	if isLeaf {
		p.SetFlags(LeafPageFlag)
	} else {
		p.SetFlags(BranchPageFlag)
	}
	p.SetCount(uint16(len(inodes)))
	WriteInodeToPage(inodes, p)

	// Compress the data portion (everything after the page header).
	dataSize := scratchSize - int(PageHeaderSize)
	data := scratch[PageHeaderSize:]
	compressed := snappy.Encode(nil, data[:dataSize])

	// Total compressed page size: header + 4-byte length + compressed data.
	compressedTotalSize := int(PageHeaderSize) + compressedDataLenSize + len(compressed)
	compressedPages := (compressedTotalSize + pageSize - 1) / pageSize

	// Only use compression if it actually reduces the page count.
	if compressedPages >= uncompressedPages {
		return nil
	}

	// Build the final compressed page buffer, sized to compressedPages.
	buf := make([]byte, compressedPages*pageSize)
	// Copy the header (id will be set by the caller, flags and count are set).
	copy(buf, scratch[:PageHeaderSize])
	// Set the compressed flag on the output header.
	cp := (*Page)(unsafe.Pointer(&buf[0]))
	cp.SetCompressed(true)
	cp.SetOverflow(uint32(compressedPages - 1))

	// Write the compressed data length, then the compressed data.
	binary.LittleEndian.PutUint32(buf[PageHeaderSize:], uint32(len(compressed)))
	copy(buf[int(PageHeaderSize)+compressedDataLenSize:], compressed)

	return buf
}

// DecompressPage takes a compressed page (from mmap or a buffer) and returns
// a new Page backed by a heap-allocated buffer with the decompressed data.
// The returned page has the compressed flag cleared.
//
// The caller is responsible for keeping the returned buffer alive as long
// as the page (and any slices derived from it) are in use.
func DecompressPage(p *Page, pageSize int) (*Page, []byte, error) {
	allocSize := (int(p.Overflow()) + 1) * pageSize

	// Read the compressed data length from the 4 bytes after the header.
	lenBytes := UnsafeByteSlice(unsafe.Pointer(p), PageHeaderSize, 0, compressedDataLenSize)
	compressedLen := int(binary.LittleEndian.Uint32(lenBytes))

	// Sanity check.
	maxCompressed := allocSize - int(PageHeaderSize) - compressedDataLenSize
	if compressedLen <= 0 || compressedLen > maxCompressed {
		return nil, nil, fmt.Errorf("invalid compressed data length %d (max %d) on page %d", compressedLen, maxCompressed, p.Id())
	}

	// Get the compressed data.
	compressedData := UnsafeByteSlice(unsafe.Pointer(p), PageHeaderSize+uintptr(compressedDataLenSize), 0, compressedLen)

	// Decode the decompressed length for buffer sizing.
	decompressedLen, err := snappy.DecodedLen(compressedData)
	if err != nil {
		return nil, nil, fmt.Errorf("snappy DecodedLen on page %d: %w", p.Id(), err)
	}

	// Allocate a buffer large enough for the header + decompressed data.
	// The buffer must be at least as large as the decompressed page content,
	// but we don't change the overflow — it must reflect the on-disk
	// allocation size so freelist accounting remains correct.
	bufSize := int(PageHeaderSize) + decompressedLen
	buf := make([]byte, bufSize)

	// Copy the header.
	headerBytes := UnsafeByteSlice(unsafe.Pointer(p), 0, 0, int(PageHeaderSize))
	copy(buf, headerBytes)

	// Decompress the data directly into the buffer after the header.
	_, err = snappy.Decode(buf[PageHeaderSize:], compressedData)
	if err != nil {
		return nil, nil, fmt.Errorf("snappy Decode on page %d: %w", p.Id(), err)
	}

	// Clear the compressed flag. Overflow is preserved from the original
	// page header — it reflects the on-disk allocation, not the
	// decompressed data size.
	newPage := (*Page)(unsafe.Pointer(&buf[0]))
	newPage.SetCompressed(false)

	return newPage, buf, nil
}

//go:build js || wasip1

package bbolt

import (
	"fmt"
	"io"
	"time"
	"unsafe"

	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
)

// mmap memory maps a DB's data file.
func mmap(db *DB, sz int) error {
	// Check MaxSize constraint for WASM platforms
	if !db.readOnly && db.MaxSize > 0 && sz > db.MaxSize {
		// The max size only limits future writes; however, we don't block opening
		// and mapping the database if it already exceeds the limit.
		fileSize, err := db.fileSize()
		if err != nil {
			return fmt.Errorf("could not check existing db file size: %s", err)
		}

		if sz > fileSize {
			return berrors.ErrMaxSizeReached
		}
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if err := db.file.Truncate(int64(sz)); err != nil {
			return fmt.Errorf("file resize error: %s", err)
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	// Map the data file to memory.
	b := make([]byte, sz)
	if sz > 0 {
		// Read the data file.
		if _, err := db.file.ReadAt(b, 0); err != nil && err != io.EOF {
			return err
		}
	}

	// Save the original byte slice and convert to a byte array pointer.
	db.dataref = b
	db.datasz = sz
	if sz > 0 {
		db.data = (*[common.MaxMapSize]byte)(unsafe.Pointer(&b[0]))
	}

	return nil
}

// munmap unmaps a DB's data file from memory.
func munmap(db *DB) error {
	// In WASM, we just clear the references
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return nil
}

// madvise is not supported in WASM.
func madvise(b []byte, advice int) error {
	// Not implemented - no memory advice in WASM
	return nil
}

// mlock is not supported in WASM.
func mlock(db *DB, fileSize int) error {
	// Not implemented - no memory locking in WASM
	return nil
}

// munlock is not supported in WASM.
func munlock(db *DB, fileSize int) error {
	// Not implemented - no memory unlocking in WASM
	return nil
}

// flock acquires an advisory lock on a file descriptor.
func flock(db *DB, exclusive bool, timeout time.Duration) error {
	// Not implemented - no file locking in WASM
	return nil
}

// funlock releases an advisory lock on a file descriptor.
func funlock(db *DB) error {
	// Not implemented - no file unlocking in WASM
	return nil
}

// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) error {
	if db.file == nil {
		return nil
	}
	return db.file.Sync()
}

// txInit refreshes the memory buffer from the file for WASM platforms.
// This is needed because WASM doesn't have real mmap, so we need to manually
// sync the memory buffer with the file to see changes from previous transactions.
func (db *DB) txInit() error {
	// For read-only databases or initial state, skip refresh
	if db.file == nil {
		return nil
	}

	// Check if the file has grown
	fileInfo, err := db.file.Stat()
	if err != nil {
		return err
	}
	fileSize := int(fileInfo.Size())

	// If file has grown or we need to initialize, refresh memory
	if fileSize > db.datasz || db.datasz == 0 {
		// Re-mmap with the new size
		if err := mmap(db, fileSize); err != nil {
			return err
		}
	} else if db.datasz > 0 {
		// Refresh the existing buffer
		b := make([]byte, db.datasz)
		if _, err := db.file.ReadAt(b, 0); err != nil && err != io.EOF {
			return err
		}
		db.dataref = b
		db.data = (*[common.MaxMapSize]byte)(unsafe.Pointer(&b[0]))
	}

	// Update meta page pointers
	if db.pageSize > 0 && db.datasz >= db.pageSize*2 {
		db.meta0 = db.page(0).Meta()
		db.meta1 = db.page(1).Meta()
	}

	return nil
}

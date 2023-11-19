package bbolt

import (
	"fmt"
	"syscall"
)

// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) error {
	fmt.Printf("linux: fdatasync started\n")
	err := syscall.Fdatasync(int(db.file.Fd()))
	if err != nil {
		fmt.Printf("linux: fdatasync failed: %v\n", err)
	}
	return err
}

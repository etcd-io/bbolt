//go:build !windows && !plan9 && !linux && !openbsd && !js && !wasip1

package bbolt

// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) error {
	return db.file.Sync()
}

//go:build !windows && !plan9 && !openbsd
// +build !windows,!plan9,!openbsd

package bbolt

// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) error {
	return db.file.Sync()
}

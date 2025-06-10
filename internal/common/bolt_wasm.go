//go:build js || wasip1

package common

// MaxMapSize represents the largest mmap size supported by Bolt.
// Reduced for WASM due to memory constraints.
const MaxMapSize = 0x10000000 // 256MB

// MaxAllocSize is the size used when creating array pointers.
// Slightly larger than MaxMapSize to accommodate metadata overhead.
const MaxAllocSize = 0x10100000 // 257MB

func init() {
	// Override the default page size for WASM to use 4KB instead of 64KB.
	// This reduces memory usage and makes behavior more consistent with other platforms.
	// Note: Databases created with different page sizes are not compatible.
	DefaultPageSize = 4096
}

// GetPagesize returns the system page size.
// On WASM platforms, we always return 4KB as the page size.
func GetPagesize() int {
	return 4096
}

//go:build js || wasip1

package common

// MaxMapSize represents the largest mmap size supported by Bolt.
const MaxMapSize = 0x10000000

// MaxAllocSize is the size used when creating array pointers.
const MaxAllocSize = 0x10100000

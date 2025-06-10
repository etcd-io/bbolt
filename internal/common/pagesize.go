//go:build !js && !wasip1

package common

import "os"

// GetPagesize returns the system page size.
// On most platforms, this returns the OS page size.
func GetPagesize() int {
	return os.Getpagesize()
}

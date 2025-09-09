package main

import (
	"errors"
	"fmt"
	"os"
)

var (
	// ErrPathRequired is returned when the path to a Bolt database is not specified.
	ErrPathRequired = errors.New("path required")

	// ErrInvalidValue is returned when a benchmark reads an unexpected value.
	ErrInvalidValue = errors.New("invalid value")

	// ErrPageIDRequired is returned when a required page id is not specified.
	ErrPageIDRequired = errors.New("page id required")

	// ErrInvalidPageArgs is returned when Page cmd receives pageIds and all option is true.
	ErrInvalidPageArgs = errors.New("invalid args: either use '--all' or 'pageid...'")

	// ErrBucketRequired is returned when a bucket is not specified.
	ErrBucketRequired = errors.New("bucket required")

	// ErrKeyNotFound is returned when a key is not found.
	ErrKeyNotFound = errors.New("key not found")
)

func main() {
	rootCmd := NewRootCommand()
	if err := rootCmd.Execute(); err != nil {
		if rootCmd.SilenceErrors {
			fmt.Fprintln(os.Stderr, "Error:", err)
			os.Exit(1)
		} else {
			os.Exit(1)
		}
	}
}

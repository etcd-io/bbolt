package main

import (
	"errors"
	"fmt"
	"os"
)

var (
	// ErrUsage is returned when a usage message was printed and the process
	// should simply exit with an error.
	ErrUsage = errors.New("usage")

	// ErrUnknownCommand is returned when a CLI command is not specified.
	ErrUnknownCommand = errors.New("unknown command")

	// ErrPathRequired is returned when the path to a Bolt database is not specified.
	ErrPathRequired = errors.New("path required")

	// ErrFileNotFound is returned when a Bolt database does not exist.
	ErrFileNotFound = errors.New("file not found")

	// ErrInvalidValue is returned when a benchmark reads an unexpected value.
	ErrInvalidValue = errors.New("invalid value")

	// ErrNonDivisibleBatchSize is returned when the batch size can't be evenly
	// divided by the iteration count.
	ErrNonDivisibleBatchSize = errors.New("number of iterations must be divisible by the batch size")

	// ErrPageIDRequired is returned when a required page id is not specified.
	ErrPageIDRequired = errors.New("page id required")

	// ErrInvalidPageArgs is returned when Page cmd receives pageIds and all option is true.
	ErrInvalidPageArgs = errors.New("invalid args: either use '--all' or 'pageid...'")

	// ErrBucketRequired is returned when a bucket is not specified.
	ErrBucketRequired = errors.New("bucket required")

	// ErrKeyNotFound is returned when a key is not found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrNotEnoughArgs is returned with a cmd is being executed with fewer arguments.
	ErrNotEnoughArgs = errors.New("not enough arguments")
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

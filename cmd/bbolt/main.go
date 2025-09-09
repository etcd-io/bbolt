package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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
	m := NewMain()
	if err := m.Run(os.Args[1:]...); err == ErrUsage {
		os.Exit(2)
	} else if err == ErrUnknownCommand {
		cobraExecute()
	} else if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func cobraExecute() {
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

type baseCommand struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// Main represents the main program execution.
type Main struct {
	baseCommand
}

// NewMain returns a new instance of Main connect to the standard input/output.
func NewMain() *Main {
	return &Main{
		baseCommand: baseCommand{
			Stdin:  os.Stdin,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		},
	}
}

// Run executes the program.
func (m *Main) Run(args ...string) error {
	// Require a command at the beginning.
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		fmt.Fprintln(m.Stderr, m.Usage())
		return ErrUsage
	}

	// Execute command.
	switch args[0] {
	case "help":
		fmt.Fprintln(m.Stderr, m.Usage())
		return ErrUsage
	default:
		return ErrUnknownCommand
	}
}

// Usage returns the help message.
func (m *Main) Usage() string {
	return strings.TrimLeft(`
Bbolt is a tool for inspecting bbolt databases.

Usage:

	bbolt command [arguments]

The commands are:

    version     print the current version of bbolt
    bench       run synthetic benchmark against bbolt
    buckets     print a list of buckets
    check       verifies integrity of bbolt database
    compact     copies a bbolt database, compacting it in the process
    dump        print a hexadecimal dump of a single page
    get         print the value of a key in a bucket
    info        print basic info
    keys        print a list of keys in a bucket
    help        print this screen
    page        print one or more pages in human readable format
    pages       print list of pages with their types
    page-item   print the key and value of a page item.
    stats       iterate over all pages and generate usage stats
    inspect     inspect the structure of the database
    surgery     perform surgery on bbolt database

Use "bbolt [command] -h" for more information about a command.
`, "\n")
}

type PageError struct {
	ID  int
	Err error
}

func (e *PageError) Error() string {
	return fmt.Sprintf("page error: id=%d, err=%s", e.ID, e.Err)
}

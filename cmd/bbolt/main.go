package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
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
	case "get":
		return newGetCommand(m).Run(args[1:]...)
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

const FORMAT_MODES = "auto|ascii-encoded|hex|bytes|redacted"

// formatBytes converts bytes into string according to format.
// Supported formats: ascii-encoded, hex, bytes.
func formatBytes(b []byte, format string) (string, error) {
	switch format {
	case "ascii-encoded":
		return fmt.Sprintf("%q", b), nil
	case "hex":
		return fmt.Sprintf("%x", b), nil
	case "bytes":
		return string(b), nil
	case "auto":
		return bytesToAsciiOrHex(b), nil
	case "redacted":
		hash := sha256.New()
		hash.Write(b)
		return fmt.Sprintf("<redacted len:%d sha256:%x>", len(b), hash.Sum(nil)), nil
	default:
		return "", fmt.Errorf("formatBytes: unsupported format: %s", format)
	}
}

func parseBytes(str string, format string) ([]byte, error) {
	switch format {
	case "ascii-encoded":
		return []byte(str), nil
	case "hex":
		return hex.DecodeString(str)
	default:
		return nil, fmt.Errorf("parseBytes: unsupported format: %s", format)
	}
}

// writelnBytes writes the byte to the writer. Supported formats: ascii-encoded, hex, bytes, auto, redacted.
// Terminates the write with a new line symbol;
func writelnBytes(w io.Writer, b []byte, format string) error {
	str, err := formatBytes(b, format)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, str)
	return err
}

// getCommand represents the "get" command execution.
type getCommand struct {
	baseCommand
}

// newGetCommand returns a getCommand.
func newGetCommand(m *Main) *getCommand {
	c := &getCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the command.
func (cmd *getCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	var parseFormat string
	var format string
	fs.StringVar(&parseFormat, "parse-format", "ascii-encoded", "Input format. One of: ascii-encoded|hex (default: ascii-encoded)")
	fs.StringVar(&format, "format", "auto", "Output format. One of: "+FORMAT_MODES+" (default: auto)")
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path, bucket and key.
	relevantArgs := fs.Args()
	if len(relevantArgs) < 3 {
		return ErrNotEnoughArgs
	}
	path, buckets := relevantArgs[0], relevantArgs[1:len(relevantArgs)-1]
	key, err := parseBytes(relevantArgs[len(relevantArgs)-1], parseFormat)
	if err != nil {
		return err
	}
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	} else if len(buckets) == 0 {
		return ErrBucketRequired
	} else if len(key) == 0 {
		return berrors.ErrKeyRequired
	}

	// Open database.
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print value.
	return db.View(func(tx *bolt.Tx) error {
		// Find bucket.
		lastBucket, err := findLastBucket(tx, buckets)
		if err != nil {
			return err
		}

		// Find value for given key.
		val := lastBucket.Get(key)
		if val == nil {
			return fmt.Errorf("Error %w for key: %q hex: \"%x\"", ErrKeyNotFound, key, string(key))
		}

		// TODO: In this particular case, it would be better to not terminate with '\n'
		return writelnBytes(cmd.Stdout, val, format)
	})
}

// Usage returns the help message.
func (cmd *getCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt get PATH [BUCKET..] KEY

Print the value of the given key in the given (sub)bucket.

Additional options include:

	--format
		Output format. One of: `+FORMAT_MODES+` (default=auto)
	--parse-format
		Input format (of key). One of: ascii-encoded|hex (default=ascii-encoded)"
`, "\n")
}

type PageError struct {
	ID  int
	Err error
}

func (e *PageError) Error() string {
	return fmt.Sprintf("page error: id=%d, err=%s", e.ID, e.Err)
}

// isPrintable returns true if the string is valid unicode and contains only printable runes.
func isPrintable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, ch := range s {
		if !unicode.IsPrint(ch) {
			return false
		}
	}
	return true
}

func bytesToAsciiOrHex(b []byte) string {
	sb := string(b)
	if isPrintable(sb) {
		return sb
	} else {
		return hex.EncodeToString(b)
	}
}

func stringToPage(str string) (uint64, error) {
	return strconv.ParseUint(str, 10, 64)
}

// stringToPages parses a slice of strings into page ids.
func stringToPages(strs []string) ([]uint64, error) {
	var a []uint64
	for _, str := range strs {
		if len(str) == 0 {
			continue
		}
		i, err := stringToPage(str)
		if err != nil {
			return nil, err
		}
		a = append(a, i)
	}
	return a, nil
}

type cmdKvStringer struct{}

func (cmdKvStringer) KeyToString(key []byte) string {
	return bytesToAsciiOrHex(key)
}

func (cmdKvStringer) ValueToString(value []byte) string {
	return bytesToAsciiOrHex(value)
}

func CmdKvStringer() bolt.KVStringer {
	return cmdKvStringer{}
}

func findLastBucket(tx *bolt.Tx, bucketNames []string) (*bolt.Bucket, error) {
	lastbucket := tx.Bucket([]byte(bucketNames[0]))
	if lastbucket == nil {
		return nil, berrors.ErrBucketNotFound
	}
	for _, bucket := range bucketNames[1:] {
		lastbucket = lastbucket.Bucket([]byte(bucket))
		if lastbucket == nil {
			return nil, berrors.ErrBucketNotFound
		}
	}
	return lastbucket, nil
}

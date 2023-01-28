package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"go.etcd.io/bbolt/internal/guts_cli"

	bolt "go.etcd.io/bbolt"
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

	// ErrBucketRequired is returned when a bucket is not specified.
	ErrBucketRequired = errors.New("bucket required")

	// ErrBucketNotFound is returned when a bucket is not found.
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrKeyRequired is returned when a key is not specified.
	ErrKeyRequired = errors.New("key required")

	// ErrKeyNotFound is returned when a key is not found.
	ErrKeyNotFound = errors.New("key not found")
)

func main() {
	m := NewMain()
	if err := m.Run(os.Args[1:]...); err == ErrUsage {
		os.Exit(2)
	} else if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
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
	case "bench":
		return newBenchCommand(m).Run(args[1:]...)
	case "buckets":
		return newBucketsCommand(m).Run(args[1:]...)
	case "check":
		return newCheckCommand(m).Run(args[1:]...)
	case "compact":
		return newCompactCommand(m).Run(args[1:]...)
	case "dump":
		return newDumpCommand(m).Run(args[1:]...)
	case "page-item":
		return newPageItemCommand(m).Run(args[1:]...)
	case "get":
		return newGetCommand(m).Run(args[1:]...)
	case "info":
		return newInfoCommand(m).Run(args[1:]...)
	case "keys":
		return newKeysCommand(m).Run(args[1:]...)
	case "page":
		return newPageCommand(m).Run(args[1:]...)
	case "pages":
		return newPagesCommand(m).Run(args[1:]...)
	case "stats":
		return newStatsCommand(m).Run(args[1:]...)
	case "surgery":
		return newSurgeryCommand(m).Run(args[1:]...)
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
    surgery     perform surgery on bbolt database

Use "bbolt [command] -h" for more information about a command.
`, "\n")
}

// checkCommand represents the "check" command execution.
type checkCommand struct {
	baseCommand
}

// newCheckCommand returns a checkCommand.
func newCheckCommand(m *Main) *checkCommand {
	c := &checkCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the command.
func (cmd *checkCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path.
	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open database.
	db, err := bolt.Open(path, 0666, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Perform consistency check.
	return db.View(func(tx *bolt.Tx) error {
		var count int
		for err := range tx.CheckWithOptions(bolt.WithKVStringer(CmdKvStringer())) {
			fmt.Fprintln(cmd.Stdout, err)
			count++
		}

		// Print summary of errors.
		if count > 0 {
			fmt.Fprintf(cmd.Stdout, "%d errors found\n", count)
			return guts_cli.ErrCorrupt
		}

		// Notify user that database is valid.
		fmt.Fprintln(cmd.Stdout, "OK")
		return nil
	})
}

// Usage returns the help message.
func (cmd *checkCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt check PATH

Check opens a database at PATH and runs an exhaustive check to verify that
all pages are accessible or are marked as freed. It also verifies that no
pages are double referenced.

Verification errors will stream out as they are found and the process will
return after all pages have been checked.
`, "\n")
}

// infoCommand represents the "info" command execution.
type infoCommand struct {
	baseCommand
}

// newInfoCommand returns a infoCommand.
func newInfoCommand(m *Main) *infoCommand {
	c := &infoCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the command.
func (cmd *infoCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path.
	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open the database.
	db, err := bolt.Open(path, 0666, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print basic database info.
	info := db.Info()
	fmt.Fprintf(cmd.Stdout, "Page Size: %d\n", info.PageSize)

	return nil
}

// Usage returns the help message.
func (cmd *infoCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt info PATH

Info prints basic information about the Bolt database at PATH.
`, "\n")
}

// dumpCommand represents the "dump" command execution.
type dumpCommand struct {
	baseCommand
}

// newDumpCommand returns a dumpCommand.
func newDumpCommand(m *Main) *dumpCommand {
	c := &dumpCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the command.
func (cmd *dumpCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path and page id.
	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Read page ids.
	pageIDs, err := stringToPages(fs.Args()[1:])
	if err != nil {
		return err
	} else if len(pageIDs) == 0 {
		return ErrPageIDRequired
	}

	// Open database to retrieve page size.
	pageSize, _, err := guts_cli.ReadPageAndHWMSize(path)
	if err != nil {
		return err
	}

	// Open database file handler.
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Print each page listed.
	for i, pageID := range pageIDs {
		// Print a separator.
		if i > 0 {
			fmt.Fprintln(cmd.Stdout, "===============================================")
		}

		// Print page to stdout.
		if err := cmd.PrintPage(cmd.Stdout, f, pageID, uint64(pageSize)); err != nil {
			return err
		}
	}

	return nil
}

// PrintPage prints a given page as hexadecimal.
func (cmd *dumpCommand) PrintPage(w io.Writer, r io.ReaderAt, pageID uint64, pageSize uint64) error {
	const bytesPerLineN = 16

	// Read page into buffer.
	buf := make([]byte, pageSize)
	addr := pageID * uint64(pageSize)
	if n, err := r.ReadAt(buf, int64(addr)); err != nil {
		return err
	} else if uint64(n) != pageSize {
		return io.ErrUnexpectedEOF
	}

	// Write out to writer in 16-byte lines.
	var prev []byte
	var skipped bool
	for offset := uint64(0); offset < pageSize; offset += bytesPerLineN {
		// Retrieve current 16-byte line.
		line := buf[offset : offset+bytesPerLineN]
		isLastLine := (offset == (pageSize - bytesPerLineN))

		// If it's the same as the previous line then print a skip.
		if bytes.Equal(line, prev) && !isLastLine {
			if !skipped {
				fmt.Fprintf(w, "%07x *\n", addr+offset)
				skipped = true
			}
		} else {
			// Print line as hexadecimal in 2-byte groups.
			fmt.Fprintf(w, "%07x %04x %04x %04x %04x %04x %04x %04x %04x\n", addr+offset,
				line[0:2], line[2:4], line[4:6], line[6:8],
				line[8:10], line[10:12], line[12:14], line[14:16],
			)

			skipped = false
		}

		// Save the previous line.
		prev = line
	}
	fmt.Fprint(w, "\n")

	return nil
}

// Usage returns the help message.
func (cmd *dumpCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt dump PATH pageid [pageid...]

Dump prints a hexadecimal dump of one or more pages.
`, "\n")
}

// pageItemCommand represents the "page-item" command execution.
type pageItemCommand struct {
	baseCommand
}

// newPageItemCommand returns a pageItemCommand.
func newPageItemCommand(m *Main) *pageItemCommand {
	c := &pageItemCommand{}
	c.baseCommand = m.baseCommand
	return c
}

type pageItemOptions struct {
	help      bool
	keyOnly   bool
	valueOnly bool
	format    string
}

// Run executes the command.
func (cmd *pageItemCommand) Run(args ...string) error {
	// Parse flags.
	options := &pageItemOptions{}
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.BoolVar(&options.keyOnly, "key-only", false, "Print only the key")
	fs.BoolVar(&options.valueOnly, "value-only", false, "Print only the value")
	fs.StringVar(&options.format, "format", "ascii-encoded", "Output format. One of: "+FORMAT_MODES)
	fs.BoolVar(&options.help, "h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if options.help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	if options.keyOnly && options.valueOnly {
		return fmt.Errorf("The --key-only or --value-only flag may be set, but not both.")
	}

	// Require database path and page id.
	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Read page id.
	pageID, err := strconv.ParseUint(fs.Arg(1), 10, 64)
	if err != nil {
		return err
	}

	// Read item id.
	itemID, err := strconv.ParseUint(fs.Arg(2), 10, 64)
	if err != nil {
		return err
	}

	// Open database file handler.
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Retrieve page info and page size.
	_, buf, err := guts_cli.ReadPage(path, pageID)
	if err != nil {
		return err
	}

	if !options.valueOnly {
		err := cmd.PrintLeafItemKey(cmd.Stdout, buf, uint16(itemID), options.format)
		if err != nil {
			return err
		}
	}
	if !options.keyOnly {
		err := cmd.PrintLeafItemValue(cmd.Stdout, buf, uint16(itemID), options.format)
		if err != nil {
			return err
		}
	}
	return nil
}

// leafPageElement retrieves a leaf page element.
func (cmd *pageItemCommand) leafPageElement(pageBytes []byte, index uint16) (*guts_cli.LeafPageElement, error) {
	p := (*guts_cli.Page)(unsafe.Pointer(&pageBytes[0]))
	if index >= p.Count() {
		return nil, fmt.Errorf("leafPageElement: expected item index less than %d, but got %d.", p.Count(), index)
	}
	if p.Type() != "leaf" {
		return nil, fmt.Errorf("leafPageElement: expected page type of 'leaf', but got '%s'", p.Type())
	}
	return p.LeafPageElement(index), nil
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
		return fmt.Sprintf("<redacted len:%d sha256:%x>", len(b), sha256.New().Sum(b)), nil
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

// PrintLeafItemKey writes the bytes of a leaf element's key.
func (cmd *pageItemCommand) PrintLeafItemKey(w io.Writer, pageBytes []byte, index uint16, format string) error {
	e, err := cmd.leafPageElement(pageBytes, index)
	if err != nil {
		return err
	}
	return writelnBytes(w, e.Key(), format)
}

// PrintLeafItemKey writes the bytes of a leaf element's value.
func (cmd *pageItemCommand) PrintLeafItemValue(w io.Writer, pageBytes []byte, index uint16, format string) error {
	e, err := cmd.leafPageElement(pageBytes, index)
	if err != nil {
		return err
	}
	return writelnBytes(w, e.Value(), format)
}

// Usage returns the help message.
func (cmd *pageItemCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt page-item [options] PATH pageid itemid

Additional options include:

	--key-only
		Print only the key
	--value-only
		Print only the value
	--format
		Output format. One of: `+FORMAT_MODES+` (default=ascii-encoded)

page-item prints a page item key and value.
`, "\n")
}

// pagesCommand represents the "pages" command execution.
type pagesCommand struct {
	baseCommand
}

// newPagesCommand returns a pagesCommand.
func newPagesCommand(m *Main) *pagesCommand {
	c := &pagesCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the command.
func (cmd *pagesCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path.
	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open database.
	db, err := bolt.Open(path, 0666, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Write header.
	fmt.Fprintln(cmd.Stdout, "ID       TYPE       ITEMS  OVRFLW")
	fmt.Fprintln(cmd.Stdout, "======== ========== ====== ======")

	return db.View(func(tx *bolt.Tx) error {
		var id int
		for {
			p, err := tx.Page(id)
			if err != nil {
				return &PageError{ID: id, Err: err}
			} else if p == nil {
				break
			}

			// Only display count and overflow if this is a non-free page.
			var count, overflow string
			if p.Type != "free" {
				count = strconv.Itoa(p.Count)
				if p.OverflowCount > 0 {
					overflow = strconv.Itoa(p.OverflowCount)
				}
			}

			// Print table row.
			fmt.Fprintf(cmd.Stdout, "%-8d %-10s %-6s %-6s\n", p.ID, p.Type, count, overflow)

			// Move to the next non-overflow page.
			id += 1
			if p.Type != "free" {
				id += p.OverflowCount
			}
		}
		return nil
	})
}

// Usage returns the help message.
func (cmd *pagesCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt pages PATH

Pages prints a table of pages with their type (meta, leaf, branch, freelist).
Leaf and branch pages will show a key count in the "items" column while the
freelist will show the number of free pages in the "items" column.

The "overflow" column shows the number of blocks that the page spills over
into. Normally there is no overflow but large keys and values can cause
a single page to take up multiple blocks.
`, "\n")
}

// statsCommand represents the "stats" command execution.
type statsCommand struct {
	baseCommand
}

// newStatsCommand returns a statsCommand.
func newStatsCommand(m *Main) *statsCommand {
	c := &statsCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the command.
func (cmd *statsCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path.
	path, prefix := fs.Arg(0), fs.Arg(1)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open database.
	db, err := bolt.Open(path, 0666, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		var s bolt.BucketStats
		var count int
		if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if bytes.HasPrefix(name, []byte(prefix)) {
				s.Add(b.Stats())
				count += 1
			}
			return nil
		}); err != nil {
			return err
		}

		fmt.Fprintf(cmd.Stdout, "Aggregate statistics for %d buckets\n\n", count)

		fmt.Fprintln(cmd.Stdout, "Page count statistics")
		fmt.Fprintf(cmd.Stdout, "\tNumber of logical branch pages: %d\n", s.BranchPageN)
		fmt.Fprintf(cmd.Stdout, "\tNumber of physical branch overflow pages: %d\n", s.BranchOverflowN)
		fmt.Fprintf(cmd.Stdout, "\tNumber of logical leaf pages: %d\n", s.LeafPageN)
		fmt.Fprintf(cmd.Stdout, "\tNumber of physical leaf overflow pages: %d\n", s.LeafOverflowN)

		fmt.Fprintln(cmd.Stdout, "Tree statistics")
		fmt.Fprintf(cmd.Stdout, "\tNumber of keys/value pairs: %d\n", s.KeyN)
		fmt.Fprintf(cmd.Stdout, "\tNumber of levels in B+tree: %d\n", s.Depth)

		fmt.Fprintln(cmd.Stdout, "Page size utilization")
		fmt.Fprintf(cmd.Stdout, "\tBytes allocated for physical branch pages: %d\n", s.BranchAlloc)
		var percentage int
		if s.BranchAlloc != 0 {
			percentage = int(float32(s.BranchInuse) * 100.0 / float32(s.BranchAlloc))
		}
		fmt.Fprintf(cmd.Stdout, "\tBytes actually used for branch data: %d (%d%%)\n", s.BranchInuse, percentage)
		fmt.Fprintf(cmd.Stdout, "\tBytes allocated for physical leaf pages: %d\n", s.LeafAlloc)
		percentage = 0
		if s.LeafAlloc != 0 {
			percentage = int(float32(s.LeafInuse) * 100.0 / float32(s.LeafAlloc))
		}
		fmt.Fprintf(cmd.Stdout, "\tBytes actually used for leaf data: %d (%d%%)\n", s.LeafInuse, percentage)

		fmt.Fprintln(cmd.Stdout, "Bucket statistics")
		fmt.Fprintf(cmd.Stdout, "\tTotal number of buckets: %d\n", s.BucketN)
		percentage = 0
		if s.BucketN != 0 {
			percentage = int(float32(s.InlineBucketN) * 100.0 / float32(s.BucketN))
		}
		fmt.Fprintf(cmd.Stdout, "\tTotal number on inlined buckets: %d (%d%%)\n", s.InlineBucketN, percentage)
		percentage = 0
		if s.LeafInuse != 0 {
			percentage = int(float32(s.InlineBucketInuse) * 100.0 / float32(s.LeafInuse))
		}
		fmt.Fprintf(cmd.Stdout, "\tBytes used for inlined buckets: %d (%d%%)\n", s.InlineBucketInuse, percentage)

		return nil
	})
}

// Usage returns the help message.
func (cmd *statsCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt stats PATH

Stats performs an extensive search of the database to track every page
reference. It starts at the current meta page and recursively iterates
through every accessible bucket.

The following errors can be reported:

    already freed
        The page is referenced more than once in the freelist.

    unreachable unfreed
        The page is not referenced by a bucket or in the freelist.

    reachable freed
        The page is referenced by a bucket but is also in the freelist.

    out of bounds
        A page is referenced that is above the high water mark.

    multiple references
        A page is referenced by more than one other page.

    invalid type
        The page type is not "meta", "leaf", "branch", or "freelist".

No errors should occur in your database. However, if for some reason you
experience corruption, please submit a ticket to the Bolt project page:

  https://github.com/boltdb/bolt/issues
`, "\n")
}

// bucketsCommand represents the "buckets" command execution.
type bucketsCommand struct {
	baseCommand
}

// newBucketsCommand returns a bucketsCommand.
func newBucketsCommand(m *Main) *bucketsCommand {
	c := &bucketsCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the command.
func (cmd *bucketsCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path.
	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open database.
	db, err := bolt.Open(path, 0666, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print buckets.
	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			fmt.Fprintln(cmd.Stdout, string(name))
			return nil
		})
	})
}

// Usage returns the help message.
func (cmd *bucketsCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt buckets PATH

Print a list of buckets.
`, "\n")
}

// keysCommand represents the "keys" command execution.
type keysCommand struct {
	baseCommand
}

// newKeysCommand returns a keysCommand.
func newKeysCommand(m *Main) *keysCommand {
	c := &keysCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the command.
func (cmd *keysCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	optionsFormat := fs.String("format", "bytes", "Output format. One of: "+FORMAT_MODES+" (default: bytes)")
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path and bucket.
	relevantArgs := fs.Args()
	path, buckets := relevantArgs[0], relevantArgs[1:]
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	} else if len(buckets) == 0 {
		return ErrBucketRequired
	}

	// Open database.
	db, err := bolt.Open(path, 0666, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print keys.
	return db.View(func(tx *bolt.Tx) error {
		// Find bucket.
		var lastbucket *bolt.Bucket = tx.Bucket([]byte(buckets[0]))
		if lastbucket == nil {
			return ErrBucketNotFound
		}
		for _, bucket := range buckets[1:] {
			lastbucket = lastbucket.Bucket([]byte(bucket))
			if lastbucket == nil {
				return ErrBucketNotFound
			}
		}

		// Iterate over each key.
		return lastbucket.ForEach(func(key, _ []byte) error {
			return writelnBytes(cmd.Stdout, key, *optionsFormat)
		})
	})
}

// Usage returns the help message.
// TODO: Use https://pkg.go.dev/flag#FlagSet.PrintDefaults to print supported flags.
func (cmd *keysCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt keys PATH [BUCKET...]

Print a list of keys in the given (sub)bucket.
=======

Additional options include:

	--format
		Output format. One of: `+FORMAT_MODES+` (default=bytes)

Print a list of keys in the given bucket.
`, "\n")
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
	fs.StringVar(&format, "format", "bytes", "Output format. One of: "+FORMAT_MODES+" (default: bytes)")
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database path, bucket and key.
	relevantArgs := fs.Args()
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
		return ErrKeyRequired
	}

	// Open database.
	db, err := bolt.Open(path, 0666, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print value.
	return db.View(func(tx *bolt.Tx) error {
		// Find bucket.
		var lastbucket *bolt.Bucket = tx.Bucket([]byte(buckets[0]))
		if lastbucket == nil {
			return ErrBucketNotFound
		}
		for _, bucket := range buckets[1:] {
			lastbucket = lastbucket.Bucket([]byte(bucket))
			if lastbucket == nil {
				return ErrBucketNotFound
			}
		}

		// Find value for given key.
		val := lastbucket.Get(key)
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
		Output format. One of: `+FORMAT_MODES+` (default=bytes)
	--parse-format
		Input format (of key). One of: ascii-encoded|hex (default=ascii-encoded)"
`, "\n")
}

var benchBucketName = []byte("bench")

// benchCommand represents the "bench" command execution.
type benchCommand struct {
	baseCommand
}

// newBenchCommand returns a BenchCommand using the
func newBenchCommand(m *Main) *benchCommand {
	c := &benchCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the "bench" command.
func (cmd *benchCommand) Run(args ...string) error {
	// Parse CLI arguments.
	options, err := cmd.ParseFlags(args)
	if err != nil {
		return err
	}

	// Remove path if "-work" is not set. Otherwise keep path.
	if options.Work {
		fmt.Fprintf(cmd.Stdout, "work: %s\n", options.Path)
	} else {
		defer os.Remove(options.Path)
	}

	// Create database.
	db, err := bolt.Open(options.Path, 0666, nil)
	if err != nil {
		return err
	}
	db.NoSync = options.NoSync
	defer db.Close()

	// Write to the database.
	var results BenchResults
	if err := cmd.runWrites(db, options, &results); err != nil {
		return fmt.Errorf("write: %v", err)
	}

	// Read from the database.
	if err := cmd.runReads(db, options, &results); err != nil {
		return fmt.Errorf("bench: read: %s", err)
	}

	// Print results.
	fmt.Fprintf(os.Stderr, "# Write\t%v\t(%v/op)\t(%v op/sec)\n", results.WriteDuration, results.WriteOpDuration(), results.WriteOpsPerSecond())
	fmt.Fprintf(os.Stderr, "# Read\t%v\t(%v/op)\t(%v op/sec)\n", results.ReadDuration, results.ReadOpDuration(), results.ReadOpsPerSecond())
	fmt.Fprintln(os.Stderr, "")
	return nil
}

// ParseFlags parses the command line flags.
func (cmd *benchCommand) ParseFlags(args []string) (*BenchOptions, error) {
	var options BenchOptions

	// Parse flagset.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&options.ProfileMode, "profile-mode", "rw", "")
	fs.StringVar(&options.WriteMode, "write-mode", "seq", "")
	fs.StringVar(&options.ReadMode, "read-mode", "seq", "")
	fs.IntVar(&options.Iterations, "count", 1000, "")
	fs.IntVar(&options.BatchSize, "batch-size", 0, "")
	fs.IntVar(&options.KeySize, "key-size", 8, "")
	fs.IntVar(&options.ValueSize, "value-size", 32, "")
	fs.StringVar(&options.CPUProfile, "cpuprofile", "", "")
	fs.StringVar(&options.MemProfile, "memprofile", "", "")
	fs.StringVar(&options.BlockProfile, "blockprofile", "", "")
	fs.Float64Var(&options.FillPercent, "fill-percent", bolt.DefaultFillPercent, "")
	fs.BoolVar(&options.NoSync, "no-sync", false, "")
	fs.BoolVar(&options.Work, "work", false, "")
	fs.StringVar(&options.Path, "path", "", "")
	fs.SetOutput(cmd.Stderr)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Set batch size to iteration size if not set.
	// Require that batch size can be evenly divided by the iteration count.
	if options.BatchSize == 0 {
		options.BatchSize = options.Iterations
	} else if options.Iterations%options.BatchSize != 0 {
		return nil, ErrNonDivisibleBatchSize
	}

	// Generate temp path if one is not passed in.
	if options.Path == "" {
		f, err := os.CreateTemp("", "bolt-bench-")
		if err != nil {
			return nil, fmt.Errorf("temp file: %s", err)
		}
		f.Close()
		os.Remove(f.Name())
		options.Path = f.Name()
	}

	return &options, nil
}

// Writes to the database.
func (cmd *benchCommand) runWrites(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	// Start profiling for writes.
	if options.ProfileMode == "rw" || options.ProfileMode == "w" {
		cmd.startProfiling(options)
	}

	t := time.Now()

	var err error
	switch options.WriteMode {
	case "seq":
		err = cmd.runWritesSequential(db, options, results)
	case "rnd":
		err = cmd.runWritesRandom(db, options, results)
	case "seq-nest":
		err = cmd.runWritesSequentialNested(db, options, results)
	case "rnd-nest":
		err = cmd.runWritesRandomNested(db, options, results)
	default:
		return fmt.Errorf("invalid write mode: %s", options.WriteMode)
	}

	// Save time to write.
	results.WriteDuration = time.Since(t)

	// Stop profiling for writes only.
	if options.ProfileMode == "w" {
		cmd.stopProfiling()
	}

	return err
}

func (cmd *benchCommand) runWritesSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	var i = uint32(0)
	return cmd.runWritesWithSource(db, options, results, func() uint32 { i++; return i })
}

func (cmd *benchCommand) runWritesRandom(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return cmd.runWritesWithSource(db, options, results, func() uint32 { return r.Uint32() })
}

func (cmd *benchCommand) runWritesSequentialNested(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	var i = uint32(0)
	return cmd.runWritesNestedWithSource(db, options, results, func() uint32 { i++; return i })
}

func (cmd *benchCommand) runWritesRandomNested(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return cmd.runWritesNestedWithSource(db, options, results, func() uint32 { return r.Uint32() })
}

func (cmd *benchCommand) runWritesWithSource(db *bolt.DB, options *BenchOptions, results *BenchResults, keySource func() uint32) error {
	results.WriteOps = options.Iterations

	for i := 0; i < options.Iterations; i += options.BatchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = options.FillPercent

			for j := 0; j < options.BatchSize; j++ {
				key := make([]byte, options.KeySize)
				value := make([]byte, options.ValueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (cmd *benchCommand) runWritesNestedWithSource(db *bolt.DB, options *BenchOptions, results *BenchResults, keySource func() uint32) error {
	results.WriteOps = options.Iterations

	for i := 0; i < options.Iterations; i += options.BatchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			top, err := tx.CreateBucketIfNotExists(benchBucketName)
			if err != nil {
				return err
			}
			top.FillPercent = options.FillPercent

			// Create bucket key.
			name := make([]byte, options.KeySize)
			binary.BigEndian.PutUint32(name, keySource())

			// Create bucket.
			b, err := top.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
			b.FillPercent = options.FillPercent

			for j := 0; j < options.BatchSize; j++ {
				var key = make([]byte, options.KeySize)
				var value = make([]byte, options.ValueSize)

				// Generate key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert value into subbucket.
				if err := b.Put(key, value); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Reads from the database.
func (cmd *benchCommand) runReads(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	// Start profiling for reads.
	if options.ProfileMode == "r" {
		cmd.startProfiling(options)
	}

	t := time.Now()

	var err error
	switch options.ReadMode {
	case "seq":
		switch options.WriteMode {
		case "seq-nest", "rnd-nest":
			err = cmd.runReadsSequentialNested(db, options, results)
		default:
			err = cmd.runReadsSequential(db, options, results)
		}
	default:
		return fmt.Errorf("invalid read mode: %s", options.ReadMode)
	}

	// Save read time.
	results.ReadDuration = time.Since(t)

	// Stop profiling for reads.
	if options.ProfileMode == "rw" || options.ProfileMode == "r" {
		cmd.stopProfiling()
	}

	return err
}

func (cmd *benchCommand) runReadsSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			var count int

			c := tx.Bucket(benchBucketName).Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if v == nil {
					return errors.New("invalid value")
				}
				count++
			}

			if options.WriteMode == "seq" && count != options.Iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", options.Iterations, count)
			}

			results.ReadOps += count

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func (cmd *benchCommand) runReadsSequentialNested(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			var count int
			var top = tx.Bucket(benchBucketName)
			if err := top.ForEach(func(name, _ []byte) error {
				if b := top.Bucket(name); b != nil {
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						if v == nil {
							return ErrInvalidValue
						}
						count++
					}
				}
				return nil
			}); err != nil {
				return err
			}

			if options.WriteMode == "seq-nest" && count != options.Iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", options.Iterations, count)
			}

			results.ReadOps += count

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

// File handlers for the various profiles.
var cpuprofile, memprofile, blockprofile *os.File

// Starts all profiles set on the options.
func (cmd *benchCommand) startProfiling(options *BenchOptions) {
	var err error

	// Start CPU profiling.
	if options.CPUProfile != "" {
		cpuprofile, err = os.Create(options.CPUProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not create cpu profile %q: %v\n", options.CPUProfile, err)
			os.Exit(1)
		}
		err = pprof.StartCPUProfile(cpuprofile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not start cpu profile %q: %v\n", options.CPUProfile, err)
			os.Exit(1)
		}
	}

	// Start memory profiling.
	if options.MemProfile != "" {
		memprofile, err = os.Create(options.MemProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not create memory profile %q: %v\n", options.MemProfile, err)
			os.Exit(1)
		}
		runtime.MemProfileRate = 4096
	}

	// Start fatal profiling.
	if options.BlockProfile != "" {
		blockprofile, err = os.Create(options.BlockProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not create block profile %q: %v\n", options.BlockProfile, err)
			os.Exit(1)
		}
		runtime.SetBlockProfileRate(1)
	}
}

// Stops all profiles.
func (cmd *benchCommand) stopProfiling() {
	if cpuprofile != nil {
		pprof.StopCPUProfile()
		cpuprofile.Close()
		cpuprofile = nil
	}

	if memprofile != nil {
		err := pprof.Lookup("heap").WriteTo(memprofile, 0)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not write mem profile")
		}
		memprofile.Close()
		memprofile = nil
	}

	if blockprofile != nil {
		err := pprof.Lookup("block").WriteTo(blockprofile, 0)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "bench: could not write block profile")
		}
		blockprofile.Close()
		blockprofile = nil
		runtime.SetBlockProfileRate(0)
	}
}

// BenchOptions represents the set of options that can be passed to "bolt bench".
type BenchOptions struct {
	ProfileMode   string
	WriteMode     string
	ReadMode      string
	Iterations    int
	BatchSize     int
	KeySize       int
	ValueSize     int
	CPUProfile    string
	MemProfile    string
	BlockProfile  string
	StatsInterval time.Duration
	FillPercent   float64
	NoSync        bool
	Work          bool
	Path          string
}

// BenchResults represents the performance results of the benchmark.
type BenchResults struct {
	WriteOps      int
	WriteDuration time.Duration
	ReadOps       int
	ReadDuration  time.Duration
}

// Returns the duration for a single write operation.
func (r *BenchResults) WriteOpDuration() time.Duration {
	if r.WriteOps == 0 {
		return 0
	}
	return r.WriteDuration / time.Duration(r.WriteOps)
}

// Returns average number of write operations that can be performed per second.
func (r *BenchResults) WriteOpsPerSecond() int {
	var op = r.WriteOpDuration()
	if op == 0 {
		return 0
	}
	return int(time.Second) / int(op)
}

// Returns the duration for a single read operation.
func (r *BenchResults) ReadOpDuration() time.Duration {
	if r.ReadOps == 0 {
		return 0
	}
	return r.ReadDuration / time.Duration(r.ReadOps)
}

// Returns average number of read operations that can be performed per second.
func (r *BenchResults) ReadOpsPerSecond() int {
	var op = r.ReadOpDuration()
	if op == 0 {
		return 0
	}
	return int(time.Second) / int(op)
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
		i, err := stringToPage(str)
		if err != nil {
			return nil, err
		}
		a = append(a, i)
	}
	return a, nil
}

// compactCommand represents the "compact" command execution.
type compactCommand struct {
	baseCommand

	SrcPath   string
	DstPath   string
	TxMaxSize int64
}

// newCompactCommand returns a CompactCommand.
func newCompactCommand(m *Main) *compactCommand {
	c := &compactCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the command.
func (cmd *compactCommand) Run(args ...string) (err error) {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cmd.DstPath, "o", "", "")
	fs.Int64Var(&cmd.TxMaxSize, "tx-max-size", 65536, "")
	if err := fs.Parse(args); err == flag.ErrHelp {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	} else if err != nil {
		return err
	} else if cmd.DstPath == "" {
		return fmt.Errorf("output file required")
	}

	// Require database paths.
	cmd.SrcPath = fs.Arg(0)
	if cmd.SrcPath == "" {
		return ErrPathRequired
	}

	// Ensure source file exists.
	fi, err := os.Stat(cmd.SrcPath)
	if os.IsNotExist(err) {
		return ErrFileNotFound
	} else if err != nil {
		return err
	}
	initialSize := fi.Size()

	// Open source database.
	src, err := bolt.Open(cmd.SrcPath, 0444, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer src.Close()

	// Open destination database.
	dst, err := bolt.Open(cmd.DstPath, fi.Mode(), nil)
	if err != nil {
		return err
	}
	defer dst.Close()

	// Run compaction.
	if err := bolt.Compact(dst, src, cmd.TxMaxSize); err != nil {
		return err
	}

	// Report stats on new size.
	fi, err = os.Stat(cmd.DstPath)
	if err != nil {
		return err
	} else if fi.Size() == 0 {
		return fmt.Errorf("zero db size")
	}
	fmt.Fprintf(cmd.Stdout, "%d -> %d bytes (gain=%.2fx)\n", initialSize, fi.Size(), float64(initialSize)/float64(fi.Size()))

	return nil
}

// Usage returns the help message.
func (cmd *compactCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt compact [options] -o DST SRC

Compact opens a database at SRC path and walks it recursively, copying keys
as they are found from all buckets, to a newly created database at DST path.

The original database is left untouched.

Additional options include:

	-tx-max-size NUM
		Specifies the maximum size of individual transactions.
		Defaults to 64KB.
`, "\n")
}

type cmdKvStringer struct{}

func (_ cmdKvStringer) KeyToString(key []byte) string {
	return bytesToAsciiOrHex(key)
}

func (_ cmdKvStringer) ValueToString(value []byte) string {
	return bytesToAsciiOrHex(value)
}

func CmdKvStringer() bolt.KVStringer {
	return cmdKvStringer{}
}

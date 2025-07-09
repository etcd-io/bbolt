package main

import (
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
	"sync/atomic"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
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
	case "bench":
		return newBenchCommand(m).Run(args[1:]...)
	case "get":
		return newGetCommand(m).Run(args[1:]...)
	case "page":
		return newPageCommand(m).Run(args[1:]...)
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
		fmt.Fprintf(cmd.Stderr, "work: %s\n", options.Path)
	} else {
		defer os.Remove(options.Path)
	}

	// Create database.
	dbOptions := *bolt.DefaultOptions
	dbOptions.PageSize = options.PageSize
	dbOptions.InitialMmapSize = options.InitialMmapSize
	db, err := bolt.Open(options.Path, 0600, &dbOptions)
	if err != nil {
		return err
	}
	db.NoSync = options.NoSync
	defer db.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Write to the database.
	var writeResults BenchResults

	fmt.Fprintf(cmd.Stderr, "starting write benchmark.\n")
	keys, err := cmd.runWrites(db, options, &writeResults, r)
	if err != nil {
		return fmt.Errorf("write: %v", err)
	}

	if keys != nil {
		r.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})
	}

	var readResults BenchResults
	fmt.Fprintf(cmd.Stderr, "starting read benchmark.\n")
	// Read from the database.
	if err := cmd.runReads(db, options, &readResults, keys); err != nil {
		return fmt.Errorf("bench: read: %s", err)
	}

	// Print results.
	if options.GoBenchOutput {
		// below replicates the output of testing.B benchmarks, e.g. for external tooling
		benchWriteName := "BenchmarkWrite"
		benchReadName := "BenchmarkRead"
		maxLen := max(len(benchReadName), len(benchWriteName))
		printGoBenchResult(cmd.Stdout, writeResults, maxLen, benchWriteName)
		printGoBenchResult(cmd.Stdout, readResults, maxLen, benchReadName)
	} else {
		fmt.Fprintf(cmd.Stdout, "# Write\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", writeResults.CompletedOps(), writeResults.Duration(), writeResults.OpDuration(), writeResults.OpsPerSecond())
		fmt.Fprintf(cmd.Stdout, "# Read\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", readResults.CompletedOps(), readResults.Duration(), readResults.OpDuration(), readResults.OpsPerSecond())
	}
	fmt.Fprintln(cmd.Stderr, "")

	return nil
}

func printGoBenchResult(w io.Writer, r BenchResults, maxLen int, benchName string) {
	gobenchResult := testing.BenchmarkResult{}
	gobenchResult.T = r.Duration()
	gobenchResult.N = int(r.CompletedOps())
	fmt.Fprintf(w, "%-*s\t%s\n", maxLen, benchName, gobenchResult.String())
}

// ParseFlags parses the command line flags.
func (cmd *benchCommand) ParseFlags(args []string) (*BenchOptions, error) {
	var options BenchOptions

	// Parse flagset.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&options.ProfileMode, "profile-mode", "rw", "")
	fs.StringVar(&options.WriteMode, "write-mode", "seq", "")
	fs.StringVar(&options.ReadMode, "read-mode", "seq", "")
	fs.Int64Var(&options.Iterations, "count", 1000, "")
	fs.Int64Var(&options.BatchSize, "batch-size", 0, "")
	fs.IntVar(&options.KeySize, "key-size", 8, "")
	fs.IntVar(&options.ValueSize, "value-size", 32, "")
	fs.StringVar(&options.CPUProfile, "cpuprofile", "", "")
	fs.StringVar(&options.MemProfile, "memprofile", "", "")
	fs.StringVar(&options.BlockProfile, "blockprofile", "", "")
	fs.Float64Var(&options.FillPercent, "fill-percent", bolt.DefaultFillPercent, "")
	fs.BoolVar(&options.NoSync, "no-sync", false, "")
	fs.BoolVar(&options.Work, "work", false, "")
	fs.StringVar(&options.Path, "path", "", "")
	fs.BoolVar(&options.GoBenchOutput, "gobench-output", false, "")
	fs.IntVar(&options.PageSize, "page-size", common.DefaultPageSize, "Set page size in bytes.")
	fs.IntVar(&options.InitialMmapSize, "initial-mmap-size", 0, "Set initial mmap size in bytes for database file.")
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
func (cmd *benchCommand) runWrites(db *bolt.DB, options *BenchOptions, results *BenchResults, r *rand.Rand) ([]nestedKey, error) {
	// Start profiling for writes.
	if options.ProfileMode == "rw" || options.ProfileMode == "w" {
		cmd.startProfiling(options)
	}

	finishChan := make(chan interface{})
	go checkProgress(results, finishChan, cmd.Stderr)
	defer close(finishChan)

	t := time.Now()

	var keys []nestedKey
	var err error
	switch options.WriteMode {
	case "seq":
		keys, err = cmd.runWritesSequential(db, options, results)
	case "rnd":
		keys, err = cmd.runWritesRandom(db, options, results, r)
	case "seq-nest":
		keys, err = cmd.runWritesSequentialNested(db, options, results)
	case "rnd-nest":
		keys, err = cmd.runWritesRandomNested(db, options, results, r)
	default:
		return nil, fmt.Errorf("invalid write mode: %s", options.WriteMode)
	}

	// Save time to write.
	results.SetDuration(time.Since(t))

	// Stop profiling for writes only.
	if options.ProfileMode == "w" {
		cmd.stopProfiling()
	}

	return keys, err
}

func (cmd *benchCommand) runWritesSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) ([]nestedKey, error) {
	var i = uint32(0)
	return cmd.runWritesWithSource(db, options, results, func() uint32 { i++; return i })
}

func (cmd *benchCommand) runWritesRandom(db *bolt.DB, options *BenchOptions, results *BenchResults, r *rand.Rand) ([]nestedKey, error) {
	return cmd.runWritesWithSource(db, options, results, func() uint32 { return r.Uint32() })
}

func (cmd *benchCommand) runWritesSequentialNested(db *bolt.DB, options *BenchOptions, results *BenchResults) ([]nestedKey, error) {
	var i = uint32(0)
	return cmd.runWritesNestedWithSource(db, options, results, func() uint32 { i++; return i })
}

func (cmd *benchCommand) runWritesRandomNested(db *bolt.DB, options *BenchOptions, results *BenchResults, r *rand.Rand) ([]nestedKey, error) {
	return cmd.runWritesNestedWithSource(db, options, results, func() uint32 { return r.Uint32() })
}

func (cmd *benchCommand) runWritesWithSource(db *bolt.DB, options *BenchOptions, results *BenchResults, keySource func() uint32) ([]nestedKey, error) {
	var keys []nestedKey
	if options.ReadMode == "rnd" {
		keys = make([]nestedKey, 0, options.Iterations)
	}

	for i := int64(0); i < options.Iterations; i += options.BatchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = options.FillPercent

			fmt.Fprintf(cmd.Stderr, "Starting write iteration %d\n", i)
			for j := int64(0); j < options.BatchSize; j++ {
				key := make([]byte, options.KeySize)
				value := make([]byte, options.ValueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}
				if keys != nil {
					keys = append(keys, nestedKey{nil, key})
				}
				results.AddCompletedOps(1)
			}
			fmt.Fprintf(cmd.Stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (cmd *benchCommand) runWritesNestedWithSource(db *bolt.DB, options *BenchOptions, results *BenchResults, keySource func() uint32) ([]nestedKey, error) {
	var keys []nestedKey
	if options.ReadMode == "rnd" {
		keys = make([]nestedKey, 0, options.Iterations)
	}

	for i := int64(0); i < options.Iterations; i += options.BatchSize {
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

			fmt.Fprintf(cmd.Stderr, "Starting write iteration %d\n", i)
			for j := int64(0); j < options.BatchSize; j++ {
				var key = make([]byte, options.KeySize)
				var value = make([]byte, options.ValueSize)

				// Generate key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert value into subbucket.
				if err := b.Put(key, value); err != nil {
					return err
				}
				if keys != nil {
					keys = append(keys, nestedKey{name, key})
				}
				results.AddCompletedOps(1)
			}
			fmt.Fprintf(cmd.Stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// Reads from the database.
func (cmd *benchCommand) runReads(db *bolt.DB, options *BenchOptions, results *BenchResults, keys []nestedKey) error {
	// Start profiling for reads.
	if options.ProfileMode == "r" {
		cmd.startProfiling(options)
	}

	finishChan := make(chan interface{})
	go checkProgress(results, finishChan, cmd.Stderr)
	defer close(finishChan)

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
	case "rnd":
		switch options.WriteMode {
		case "seq-nest", "rnd-nest":
			err = cmd.runReadsRandomNested(db, options, keys, results)
		default:
			err = cmd.runReadsRandom(db, options, keys, results)
		}
	default:
		return fmt.Errorf("invalid read mode: %s", options.ReadMode)
	}

	// Save read time.
	results.SetDuration(time.Since(t))

	// Stop profiling for reads.
	if options.ProfileMode == "rw" || options.ProfileMode == "r" {
		cmd.stopProfiling()
	}

	return err
}

type nestedKey struct{ bucket, key []byte }

func (cmd *benchCommand) runReadsSequential(db *bolt.DB, options *BenchOptions, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			err := func() error {
				defer func() { results.AddCompletedOps(numReads) }()

				c := tx.Bucket(benchBucketName).Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					numReads++
					if v == nil {
						return ErrInvalidValue
					}
				}

				return nil
			}()

			if err != nil {
				return err
			}

			if options.WriteMode == "seq" && numReads != options.Iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", options.Iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func (cmd *benchCommand) runReadsRandom(db *bolt.DB, options *BenchOptions, keys []nestedKey, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			err := func() error {
				defer func() { results.AddCompletedOps(numReads) }()

				b := tx.Bucket(benchBucketName)
				for _, key := range keys {
					v := b.Get(key.key)
					numReads++
					if v == nil {
						return ErrInvalidValue
					}
				}

				return nil
			}()

			if err != nil {
				return err
			}

			if options.WriteMode == "seq" && numReads != options.Iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", options.Iterations, numReads)
			}

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
			numReads := int64(0)
			var top = tx.Bucket(benchBucketName)
			if err := top.ForEach(func(name, _ []byte) error {
				defer func() { results.AddCompletedOps(numReads) }()
				if b := top.Bucket(name); b != nil {
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						numReads++
						if v == nil {
							return ErrInvalidValue
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}

			if options.WriteMode == "seq-nest" && numReads != options.Iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", options.Iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func (cmd *benchCommand) runReadsRandomNested(db *bolt.DB, options *BenchOptions, nestedKeys []nestedKey, results *BenchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			err := func() error {
				defer func() { results.AddCompletedOps(numReads) }()

				var top = tx.Bucket(benchBucketName)
				for _, nestedKey := range nestedKeys {
					if b := top.Bucket(nestedKey.bucket); b != nil {
						v := b.Get(nestedKey.key)
						numReads++
						if v == nil {
							return ErrInvalidValue
						}
					}
				}

				return nil
			}()

			if err != nil {
				return err
			}

			if options.WriteMode == "seq-nest" && numReads != options.Iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", options.Iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func checkProgress(results *BenchResults, finishChan chan interface{}, stderr io.Writer) {
	ticker := time.Tick(time.Second)
	lastCompleted, lastTime := int64(0), time.Now()
	for {
		select {
		case <-finishChan:
			return
		case t := <-ticker:
			completed, taken := results.CompletedOps(), t.Sub(lastTime)
			fmt.Fprintf(stderr, "Completed %d requests, %d/s \n",
				completed, ((completed-lastCompleted)*int64(time.Second))/int64(taken),
			)
			lastCompleted, lastTime = completed, t
		}
	}
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
	ProfileMode     string
	WriteMode       string
	ReadMode        string
	Iterations      int64
	BatchSize       int64
	KeySize         int
	ValueSize       int
	CPUProfile      string
	MemProfile      string
	BlockProfile    string
	StatsInterval   time.Duration
	FillPercent     float64
	NoSync          bool
	Work            bool
	Path            string
	GoBenchOutput   bool
	PageSize        int
	InitialMmapSize int
}

// BenchResults represents the performance results of the benchmark and is thread-safe.
type BenchResults struct {
	completedOps int64
	duration     int64
}

func (r *BenchResults) AddCompletedOps(amount int64) {
	atomic.AddInt64(&r.completedOps, amount)
}

func (r *BenchResults) CompletedOps() int64 {
	return atomic.LoadInt64(&r.completedOps)
}

func (r *BenchResults) SetDuration(dur time.Duration) {
	atomic.StoreInt64(&r.duration, int64(dur))
}

func (r *BenchResults) Duration() time.Duration {
	return time.Duration(atomic.LoadInt64(&r.duration))
}

// Returns the duration for a single read/write operation.
func (r *BenchResults) OpDuration() time.Duration {
	if r.CompletedOps() == 0 {
		return 0
	}
	return r.Duration() / time.Duration(r.CompletedOps())
}

// Returns average number of read/write operations that can be performed per second.
func (r *BenchResults) OpsPerSecond() int {
	var op = r.OpDuration()
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

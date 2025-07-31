package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/common"
)

var (
	// ErrBatchNonDivisibleBatchSize is returned when the batch size can't be evenly
	// divided by the iteration count.
	ErrBatchNonDivisibleBatchSize = errors.New("the number of iterations must be divisible by the batch size")

	// ErrBatchInvalidWriteMode is returned when the write mode is other than seq, rnd, seq-nest, or rnd-nest.
	ErrBatchInvalidWriteMode = errors.New("the write mode should be one of seq, rnd, seq-nest, or rnd-nest")
)

var benchBucketName = []byte("bench")

type benchOptions struct {
	profileMode     string
	writeMode       string
	readMode        string
	iterations      int64
	batchSize       int64
	keySize         int
	valueSize       int
	cpuProfile      string
	memProfile      string
	blockProfile    string
	fillPercent     float64
	noSync          bool
	work            bool
	path            string
	goBenchOutput   bool
	pageSize        int
	initialMmapSize int
	deleteFraction  float64 // Fraction of keys of last tx to delete during writes. works only with "seq-del" write mode.
}

func (o *benchOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.profileMode, "profile-mode", "rw", "")
	fs.StringVar(&o.writeMode, "write-mode", "seq", "")
	fs.StringVar(&o.readMode, "read-mode", "seq", "")
	fs.Int64Var(&o.iterations, "count", 1000, "")
	fs.Int64Var(&o.batchSize, "batch-size", 0, "")
	fs.IntVar(&o.keySize, "key-size", 8, "")
	fs.IntVar(&o.valueSize, "value-size", 32, "")
	fs.StringVar(&o.cpuProfile, "cpuprofile", "", "")
	fs.StringVar(&o.memProfile, "memprofile", "", "")
	fs.StringVar(&o.blockProfile, "blockprofile", "", "")
	fs.Float64Var(&o.fillPercent, "fill-percent", bolt.DefaultFillPercent, "")
	fs.BoolVar(&o.noSync, "no-sync", false, "")
	fs.BoolVar(&o.work, "work", false, "")
	fs.StringVar(&o.path, "path", "", "")
	fs.BoolVar(&o.goBenchOutput, "gobench-output", false, "")
	fs.IntVar(&o.pageSize, "page-size", common.DefaultPageSize, "Set page size in bytes.")
	fs.IntVar(&o.initialMmapSize, "initial-mmap-size", 0, "Set initial mmap size in bytes for database file.")
}

// Returns an error if `bench` options are not valid.
func (o *benchOptions) Validate() error {
	// Require that batch size can be evenly divided by the iteration count if set.
	if o.batchSize > 0 && o.iterations%o.batchSize != 0 {
		return ErrBatchNonDivisibleBatchSize
	}

	switch o.writeMode {
	case "seq", "rnd", "seq-nest", "rnd-nest":
	default:
		return ErrBatchInvalidWriteMode
	}

	// Generate temp path if one is not passed in.
	if o.path == "" {
		f, err := os.CreateTemp("", "bolt-bench-")
		if err != nil {
			return fmt.Errorf("temp file: %s", err)
		}
		f.Close()
		os.Remove(f.Name())
		o.path = f.Name()
	}

	return nil
}

// Sets the `bench` option values that are dependent on other options.
func (o *benchOptions) SetOptionValues() error {
	// Generate temp path if one is not passed in.
	if o.path == "" {
		f, err := os.CreateTemp("", "bolt-bench-")
		if err != nil {
			return fmt.Errorf("error creating temp file: %s", err)
		}
		f.Close()
		os.Remove(f.Name())
		o.path = f.Name()
	}

	// Set batch size to iteration size if not set.
	if o.batchSize == 0 {
		o.batchSize = o.iterations
	}

	return nil
}

func newBenchCommand() *cobra.Command {
	var o benchOptions

	benchCmd := &cobra.Command{
		Use:   "bench",
		Short: "run synthetic benchmark against bbolt",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			if err := o.SetOptionValues(); err != nil {
				return err
			}
			return benchFunc(cmd, &o)
		},
	}

	o.AddFlags(benchCmd.Flags())

	return benchCmd
}

func benchFunc(cmd *cobra.Command, options *benchOptions) error {
	// Remove path if "-work" is not set. Otherwise keep path.
	if options.work {
		fmt.Fprintf(cmd.ErrOrStderr(), "work: %s\n", options.path)
	} else {
		defer os.Remove(options.path)
	}

	// Create database.
	dbOptions := *bolt.DefaultOptions
	dbOptions.PageSize = options.pageSize
	dbOptions.InitialMmapSize = options.initialMmapSize
	db, err := bolt.Open(options.path, 0600, &dbOptions)
	if err != nil {
		return err
	}
	db.NoSync = options.noSync
	defer db.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	var writeResults benchResults

	fmt.Fprintf(cmd.ErrOrStderr(), "starting write benchmark.\n")
	keys, err := runWrites(cmd, db, options, &writeResults, r)
	if err != nil {
		return fmt.Errorf("write: %v", err)
	}

	if keys != nil {
		r.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})
	}

	var readResults benchResults
	fmt.Fprintf(cmd.ErrOrStderr(), "starting read benchmark.\n")
	// Read from the database.
	if err := runReads(cmd, db, options, &readResults, keys); err != nil {
		return fmt.Errorf("bench: read: %s", err)
	}

	// Print results.
	if options.goBenchOutput {
		// below replicates the output of testing.B benchmarks, e.g. for external tooling
		benchWriteName := "BenchmarkWrite"
		benchReadName := "BenchmarkRead"
		maxLen := max(len(benchReadName), len(benchWriteName))
		printGoBenchResult(cmd.OutOrStdout(), writeResults, maxLen, benchWriteName)
		printGoBenchResult(cmd.OutOrStdout(), readResults, maxLen, benchReadName)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "# Write\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", writeResults.getCompletedOps(), writeResults.getDuration(), writeResults.opDuration(), writeResults.opsPerSecond())
		fmt.Fprintf(cmd.OutOrStdout(), "# Read\t%v(ops)\t%v\t(%v/op)\t(%v op/sec)\n", readResults.getCompletedOps(), readResults.getDuration(), readResults.opDuration(), readResults.opsPerSecond())
	}
	fmt.Fprintln(cmd.OutOrStdout(), "")

	return nil
}

func runWrites(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults, r *rand.Rand) ([]nestedKey, error) {
	// Start profiling for writes.
	if options.profileMode == "rw" || options.profileMode == "w" {
		startProfiling(cmd, options)
	}

	finishChan := make(chan interface{})
	go checkProgress(results, finishChan, cmd.ErrOrStderr())
	defer close(finishChan)

	t := time.Now()

	var keys []nestedKey
	var err error
	switch options.writeMode {
	case "seq":
		keys, err = runWritesSequential(cmd, db, options, results)
	case "rnd":
		keys, err = runWritesRandom(cmd, db, options, results, r)
	case "seq-nest":
		keys, err = runWritesSequentialNested(cmd, db, options, results)
	case "rnd-nest":
		keys, err = runWritesRandomNested(cmd, db, options, results, r)
	case "seq-del":
		options.deleteFraction = 0.1
		keys, err = runWritesSequentialAndDelete(cmd, db, options, results)
	default:
		return nil, fmt.Errorf("invalid write mode: %s", options.writeMode)
	}

	// Save time to write.
	results.setDuration(time.Since(t))

	// Stop profiling for writes only.
	if options.profileMode == "w" {
		stopProfiling(cmd)
	}

	return keys, err
}

func runWritesSequential(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults) ([]nestedKey, error) {
	var i = uint32(0)
	return runWritesWithSource(cmd, db, options, results, func() uint32 { i++; return i })
}

func runWritesSequentialAndDelete(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults) ([]nestedKey, error) {
	var i = uint32(0)
	return runWritesDeletesWithSource(cmd, db, options, results, func() uint32 { i++; return i })
}

func runWritesRandom(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults, r *rand.Rand) ([]nestedKey, error) {
	return runWritesWithSource(cmd, db, options, results, func() uint32 { return r.Uint32() })
}

func runWritesSequentialNested(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults) ([]nestedKey, error) {
	var i = uint32(0)
	return runWritesNestedWithSource(cmd, db, options, results, func() uint32 { i++; return i })
}

func runWritesRandomNested(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults, r *rand.Rand) ([]nestedKey, error) {
	return runWritesNestedWithSource(cmd, db, options, results, func() uint32 { return r.Uint32() })
}

func runWritesWithSource(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults, keySource func() uint32) ([]nestedKey, error) {
	var keys []nestedKey
	if options.readMode == "rnd" {
		keys = make([]nestedKey, 0, options.iterations)
	}

	for i := int64(0); i < options.iterations; i += options.batchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = options.fillPercent

			fmt.Fprintf(cmd.ErrOrStderr(), "Starting write iteration %d\n", i)
			for j := int64(0); j < options.batchSize; j++ {
				key := make([]byte, options.keySize)
				value := make([]byte, options.valueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}
				if keys != nil {
					keys = append(keys, nestedKey{nil, key})
				}
				results.addCompletedOps(1)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func runWritesDeletesWithSource(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults, keySource func() uint32) ([]nestedKey, error) {
	var keys []nestedKey
	deleteSize := int64(math.Ceil(float64(options.batchSize) * options.deleteFraction))
	var InsertedKeys [][]byte

	for i := int64(0); i < options.iterations; i += options.batchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = options.fillPercent

			fmt.Fprintf(cmd.ErrOrStderr(), "Starting delete iteration %d, deleteSize: %d\n", i, deleteSize)
			for i := int64(0); i < deleteSize && i < int64(len(InsertedKeys)); i++ {
				if err := b.Delete(InsertedKeys[i]); err != nil {
					return err
				}
			}
			InsertedKeys = InsertedKeys[:0]
			fmt.Fprintf(cmd.ErrOrStderr(), "Finished delete iteration %d\n", i)

			fmt.Fprintf(cmd.ErrOrStderr(), "Starting write iteration %d\n", i)
			for j := int64(0); j < options.batchSize; j++ {

				key := make([]byte, options.keySize)
				value := make([]byte, options.valueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())
				InsertedKeys = append(InsertedKeys, key)

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}
				if keys != nil {
					keys = append(keys, nestedKey{nil, key})
				}
				results.addCompletedOps(1)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "Finished write iteration %d\n", i)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func runWritesNestedWithSource(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults, keySource func() uint32) ([]nestedKey, error) {
	var keys []nestedKey
	if options.readMode == "rnd" {
		keys = make([]nestedKey, 0, options.iterations)
	}

	for i := int64(0); i < options.iterations; i += options.batchSize {
		if err := db.Update(func(tx *bolt.Tx) error {
			top, err := tx.CreateBucketIfNotExists(benchBucketName)
			if err != nil {
				return err
			}
			top.FillPercent = options.fillPercent

			// Create bucket key.
			name := make([]byte, options.keySize)
			binary.BigEndian.PutUint32(name, keySource())

			// Create bucket.
			b, err := top.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
			b.FillPercent = options.fillPercent

			fmt.Fprintf(cmd.ErrOrStderr(), "Starting write iteration %d\n", i)
			for j := int64(0); j < options.batchSize; j++ {
				var key = make([]byte, options.keySize)
				var value = make([]byte, options.valueSize)

				// Generate key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert value into subbucket.
				if err := b.Put(key, value); err != nil {
					return err
				}
				if keys != nil {
					keys = append(keys, nestedKey{name, key})
				}
				results.addCompletedOps(1)
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func runReads(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults, keys []nestedKey) error {
	// Start profiling for reads.
	if options.profileMode == "r" {
		startProfiling(cmd, options)
	}

	finishChan := make(chan interface{})
	go checkProgress(results, finishChan, cmd.ErrOrStderr())
	defer close(finishChan)

	t := time.Now()

	var err error
	switch options.readMode {
	case "seq":
		switch options.writeMode {
		case "seq-nest", "rnd-nest":
			err = runReadsSequentialNested(cmd, db, options, results)
		default:
			err = runReadsSequential(cmd, db, options, results)
		}
	case "rnd":
		switch options.writeMode {
		case "seq-nest", "rnd-nest":
			err = runReadsRandomNested(cmd, db, options, keys, results)
		default:
			err = runReadsRandom(cmd, db, options, keys, results)
		}
	default:
		return fmt.Errorf("invalid read mode: %s", options.readMode)
	}

	// Save read time.
	results.setDuration(time.Since(t))

	// Stop profiling for reads.
	if options.profileMode == "rw" || options.profileMode == "r" {
		stopProfiling(cmd)
	}

	return err
}

type nestedKey struct{ bucket, key []byte }

func runReadsSequential(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			err := func() error {
				defer func() { results.addCompletedOps(numReads) }()

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

			if options.writeMode == "seq" && numReads != options.iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", options.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func runReadsRandom(cmd *cobra.Command, db *bolt.DB, options *benchOptions, keys []nestedKey, results *benchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			err := func() error {
				defer func() { results.addCompletedOps(numReads) }()

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

			if options.writeMode == "seq" && numReads != options.iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", options.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func runReadsSequentialNested(cmd *cobra.Command, db *bolt.DB, options *benchOptions, results *benchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			var top = tx.Bucket(benchBucketName)
			if err := top.ForEach(func(name, _ []byte) error {
				defer func() { results.addCompletedOps(numReads) }()
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

			if options.writeMode == "seq-nest" && numReads != options.iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", options.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func runReadsRandomNested(cmd *cobra.Command, db *bolt.DB, options *benchOptions, nestedKeys []nestedKey, results *benchResults) error {
	return db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := int64(0)
			err := func() error {
				defer func() { results.addCompletedOps(numReads) }()

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

			if options.writeMode == "seq-nest" && numReads != options.iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", options.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func checkProgress(results *benchResults, finishChan chan interface{}, stderr io.Writer) {
	ticker := time.Tick(time.Second)
	lastCompleted, lastTime := int64(0), time.Now()
	for {
		select {
		case <-finishChan:
			return
		case t := <-ticker:
			completed, taken := results.getCompletedOps(), t.Sub(lastTime)
			fmt.Fprintf(stderr, "Completed %d requests, %d/s \n",
				completed, ((completed-lastCompleted)*int64(time.Second))/int64(taken),
			)
			lastCompleted, lastTime = completed, t
		}
	}
}

var cpuprofile, memprofile, blockprofile *os.File

func startProfiling(cmd *cobra.Command, options *benchOptions) {
	var err error

	// Start CPU profiling.
	if options.cpuProfile != "" {
		cpuprofile, err = os.Create(options.cpuProfile)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "bench: could not create cpu profile %q: %v\n", options.cpuProfile, err)
			os.Exit(1)
		}
		err = pprof.StartCPUProfile(cpuprofile)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "bench: could not start cpu profile %q: %v\n", options.cpuProfile, err)
			os.Exit(1)
		}
	}

	// Start memory profiling.
	if options.memProfile != "" {
		memprofile, err = os.Create(options.memProfile)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "bench: could not create memory profile %q: %v\n", options.memProfile, err)
			os.Exit(1)
		}
		runtime.MemProfileRate = 4096
	}

	// Start fatal profiling.
	if options.blockProfile != "" {
		blockprofile, err = os.Create(options.blockProfile)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "bench: could not create block profile %q: %v\n", options.blockProfile, err)
			os.Exit(1)
		}
		runtime.SetBlockProfileRate(1)
	}
}

func stopProfiling(cmd *cobra.Command) {
	if cpuprofile != nil {
		pprof.StopCPUProfile()
		cpuprofile.Close()
		cpuprofile = nil
	}

	if memprofile != nil {
		err := pprof.Lookup("heap").WriteTo(memprofile, 0)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "bench: could not write mem profile")
		}
		memprofile.Close()
		memprofile = nil
	}

	if blockprofile != nil {
		err := pprof.Lookup("block").WriteTo(blockprofile, 0)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "bench: could not write block profile")
		}
		blockprofile.Close()
		blockprofile = nil
		runtime.SetBlockProfileRate(0)
	}
}

// benchResults represents the performance results of the benchmark and is thread-safe.
type benchResults struct {
	completedOps int64
	duration     int64
}

func (r *benchResults) addCompletedOps(amount int64) {
	atomic.AddInt64(&r.completedOps, amount)
}

func (r *benchResults) getCompletedOps() int64 {
	return atomic.LoadInt64(&r.completedOps)
}

func (r *benchResults) setDuration(dur time.Duration) {
	atomic.StoreInt64(&r.duration, int64(dur))
}

func (r *benchResults) getDuration() time.Duration {
	return time.Duration(atomic.LoadInt64(&r.duration))
}

// opDuration returns the duration for a single read/write operation.
func (r *benchResults) opDuration() time.Duration {
	if r.getCompletedOps() == 0 {
		return 0
	}
	return r.getDuration() / time.Duration(r.getCompletedOps())
}

// opsPerSecond returns average number of read/write operations that can be performed per second.
func (r *benchResults) opsPerSecond() int {
	var op = r.opDuration()
	if op == 0 {
		return 0
	}
	return int(time.Second) / int(op)
}

func printGoBenchResult(w io.Writer, r benchResults, maxLen int, benchName string) {
	gobenchResult := testing.BenchmarkResult{}
	gobenchResult.T = r.getDuration()
	gobenchResult.N = int(r.getCompletedOps())
	fmt.Fprintf(w, "%-*s\t%s\n", maxLen, benchName, gobenchResult.String())
}

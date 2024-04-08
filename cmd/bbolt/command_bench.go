package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	bolt "go.etcd.io/bbolt"
)

var (
	// ErrBatchNonDivisibleBatchSize is returned when the batch size can't be evenly
	// divided by the iteration count.
	ErrBatchNonDivisibleBatchSize = errors.New("the number of iterations must be divisible by the batch size")

	// ErrBatchInvalidWriteMode is returned when the write mode is other than seq, rnd, seq-nest, or rnd-nest.
	ErrBatchInvalidWriteMode = errors.New("the write mode should be one of seq, rnd, seq-nest, or rnd-nest")
)

var benchBucketName = []byte("bench")

func newBenchCobraCommand() *cobra.Command {
	o := newBenchOptions()
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
			benchExec := newBenchExecution(cmd, o)
			return benchExec.Run()
		},
	}

	o.AddFlags(benchCmd.Flags())
	return benchCmd
}

type benchOptions struct {
	batchSize    uint32
	blockProfile string
	iterations   uint32
	cpuProfile   string
	fillPercent  float64
	keySize      int
	memProfile   string
	noSync       bool
	path         string
	profileMode  string
	valueSize    int
	work         bool
	writeMode    string
}

// Returns a new benchOptions for the `bench` command with the default values applied.
func newBenchOptions() benchOptions {
	return benchOptions{
		iterations:  1000,
		fillPercent: bolt.DefaultFillPercent,
		keySize:     8,
		profileMode: "rw",
		valueSize:   32,
		writeMode:   "seq",
	}
}

// AddFlags sets the flags for the `bench` command.
func (o *benchOptions) AddFlags(fs *pflag.FlagSet) {
	fs.Uint32Var(&o.batchSize, "batch-size", o.batchSize, "the step size for each iteration, if not provided iteration size is used, it needs to be evenly divided by the iteration count (count)")
	fs.StringVar(&o.blockProfile, "blockprofile", o.blockProfile, "output file for the pprof block profile")
	fs.Uint32Var(&o.iterations, "count", o.iterations, "the number of iterations")
	fs.StringVar(&o.cpuProfile, "cpuprofile", o.cpuProfile, "output file for the pprof CPU profile")
	fs.Float64Var(&o.fillPercent, "fill-percent", o.fillPercent, "the percentage that split pages are filled")
	fs.IntVar(&o.keySize, "key-size", o.keySize, "the size for the key, from the key value insertion")
	fs.StringVar(&o.memProfile, "memprofile", o.memProfile, "output file for the pprof memoery profile")
	fs.BoolVar(&o.noSync, "no-sync", o.noSync, "skip fsync() calls after each commit")
	fs.StringVar(&o.path, "path", o.path, "path to the database file")
	fs.StringVar(&o.profileMode, "profile-mode", o.profileMode, "the profile mode to execute, valid modes are r, w and rw")
	fs.IntVar(&o.valueSize, "value-size", o.valueSize, "the size for the value, from the key value insertion")
	fs.BoolVar(&o.work, "work", o.work, "if set, the database path won't be removed after the execution")
	fs.StringVar(&o.writeMode, "write-mode", o.writeMode, "the write mode, valid values are seq, rnd, seq-nest and rnd-nest")
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

type benchExecution struct {
	cfg          benchOptions
	db           *bolt.DB
	writeResults *benchResults
	readResults  *benchResults
	cpuProfile   *os.File
	memProfile   *os.File
	blockProfile *os.File
	stderr       io.Writer
}

func newBenchExecution(cmd *cobra.Command, cfg benchOptions) *benchExecution {
	return &benchExecution{
		cfg:          cfg,
		writeResults: new(benchResults),
		readResults:  new(benchResults),
		stderr:       cmd.OutOrStderr(),
	}
}

func (e *benchExecution) Run() error {
	// Ensure that profiling files are closed if there's an early exit from an error.
	defer e.stopProfiling()

	// Remove path if "-work" is not set. Otherwise keep path.
	if e.cfg.work {
		fmt.Fprintf(e.stderr, "work: %s\n", e.cfg.path)
	} else {
		defer os.Remove(e.cfg.path)
	}

	var err error
	// Create database.
	e.db, err = bolt.Open(e.cfg.path, 0600, nil)
	if err != nil {
		return err
	}
	e.db.NoSync = e.cfg.noSync
	defer e.db.Close()

	// Write to the database.
	fmt.Fprintf(e.stderr, "starting write benchmark.\n")
	if err := e.benchWrites(); err != nil {
		return fmt.Errorf("write: %v", err)
	}

	fmt.Fprintf(e.stderr, "starting read benchmark.\n")
	// Read from the database.
	if err := e.benchReads(); err != nil {
		return fmt.Errorf("bench: read: %s", err)
	}

	// Print results.
	fmt.Fprintf(e.stderr, "# Write\t%s\n", e.writeResults)
	fmt.Fprintf(e.stderr, "# Read\t%s\n\n", e.readResults)
	return nil
}

// benchResults represents the performance results of the benchmark and is thread-safe.
type benchResults struct {
	completedOps int64
	duration     int64
}

func (r *benchResults) AddCompletedOps(amount int64) {
	atomic.AddInt64(&r.completedOps, amount)
}

func (r *benchResults) CompletedOps() int64 {
	return atomic.LoadInt64(&r.completedOps)
}

func (r *benchResults) SetDuration(dur time.Duration) {
	atomic.StoreInt64(&r.duration, int64(dur))
}

func (r *benchResults) Duration() time.Duration {
	return time.Duration(atomic.LoadInt64(&r.duration))
}

// Returns the duration for a single read/write operation.
func (r *benchResults) OpDuration() time.Duration {
	if r.CompletedOps() == 0 {
		return 0
	}
	return r.Duration() / time.Duration(r.CompletedOps())
}

// Returns average number of read/write operations that can be performed per second.
func (r *benchResults) OpsPerSecond() int {
	var op = r.OpDuration()
	if op == 0 {
		return 0
	}
	return int(time.Second) / int(op)
}

// Returns the printable results.
func (r *benchResults) String() string {
	return fmt.Sprintf("%v(ops)\t%v\t(%v/op)\t(%v op/sec)", r.CompletedOps(), r.Duration(), r.OpDuration(), r.OpsPerSecond())
}

func (e *benchExecution) benchWrites() error {
	// Start profiling for writes.
	if e.cfg.profileMode == "rw" || e.cfg.profileMode == "w" {
		if err := e.startProfiling(); err != nil {
			return err
		}
	}

	finishChan := make(chan interface{})
	go benchCheckProgress(e.writeResults, finishChan)
	defer close(finishChan)

	t := time.Now()

	var err error
	switch e.cfg.writeMode {
	case "seq":
		err = e.benchWritesWithSource(benchSequentialKeySource())
	case "rnd":
		err = e.benchWritesWithSource(benchRandomKeySource())
	case "seq-nest":
		err = e.benchWritesNestedWithSource(benchSequentialKeySource())
	case "rnd-nest":
		err = e.benchWritesNestedWithSource(benchRandomKeySource())
	default:
		return fmt.Errorf("invalid write mode: %s", e.cfg.writeMode)
	}

	// Save time to write.
	e.writeResults.SetDuration(time.Since(t))

	// Stop profiling for writes only.
	if e.cfg.profileMode == "w" {
		e.stopProfiling()
	}

	return err
}

func benchSequentialKeySource() func() uint32 {
	i := uint32(0)
	return func() uint32 { i++; return i }
}

func benchRandomKeySource() func() uint32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func() uint32 { return r.Uint32() }
}

func (e *benchExecution) benchWritesWithSource(keySource func() uint32) error {
	for i := uint32(0); i < e.cfg.iterations; i += e.cfg.batchSize {
		if err := e.db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists(benchBucketName)
			b.FillPercent = e.cfg.fillPercent

			fmt.Fprintf(e.stderr, "Starting write iteration %d/%d+%d\n", i, e.cfg.iterations, e.cfg.batchSize)
			for j := uint32(0); j < e.cfg.batchSize; j++ {
				key := make([]byte, e.cfg.keySize)
				value := make([]byte, e.cfg.valueSize)

				// Write key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert key/value.
				if err := b.Put(key, value); err != nil {
					return err
				}

				e.writeResults.AddCompletedOps(1)
			}
			fmt.Fprintf(e.stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *benchExecution) benchWritesNestedWithSource(keySource func() uint32) error {
	for i := uint32(0); i < e.cfg.iterations; i += e.cfg.batchSize {
		if err := e.db.Update(func(tx *bolt.Tx) error {
			top, err := tx.CreateBucketIfNotExists(benchBucketName)
			if err != nil {
				return err
			}
			top.FillPercent = e.cfg.fillPercent

			// Create bucket key.
			name := make([]byte, e.cfg.keySize)
			binary.BigEndian.PutUint32(name, keySource())

			// Create bucket.
			b, err := top.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
			b.FillPercent = e.cfg.fillPercent

			fmt.Fprintf(e.stderr, "Starting write iteration %d\n", i)
			for j := uint32(0); j < e.cfg.batchSize; j++ {
				var key = make([]byte, e.cfg.keySize)
				var value = make([]byte, e.cfg.valueSize)

				// Generate key as uint32.
				binary.BigEndian.PutUint32(key, keySource())

				// Insert value into subbucket.
				if err := b.Put(key, value); err != nil {
					return err
				}

				e.writeResults.AddCompletedOps(1)
			}
			fmt.Fprintf(e.stderr, "Finished write iteration %d\n", i)

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// Reads from the database.
func (e *benchExecution) benchReads() error {
	// Start profiling for reads.
	if e.cfg.profileMode == "r" {
		if err := e.startProfiling(); err != nil {
			return err
		}
	}

	finishChan := make(chan interface{})
	go benchCheckProgress(e.readResults, finishChan)
	defer close(finishChan)

	t := time.Now()

	var err error
	switch e.cfg.writeMode {
	case "seq-nest", "rnd-nest":
		err = e.benchReadsSequentialNested()
	default:
		err = e.benchReadsSequential()
	}

	// Save read time.
	e.readResults.SetDuration(time.Since(t))

	// Stop profiling for reads.
	if e.cfg.profileMode == "rw" || e.cfg.profileMode == "r" {
		e.stopProfiling()
	}

	return err
}

func (e *benchExecution) benchReadsSequential() error {
	return e.db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := uint32(0)
			c := tx.Bucket(benchBucketName).Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				numReads++
				e.readResults.AddCompletedOps(1)
				if v == nil {
					return errors.New("invalid value")
				}
			}

			if e.cfg.writeMode == "seq" && numReads != e.cfg.iterations {
				return fmt.Errorf("read seq: iter mismatch: expected %d, got %d", e.cfg.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func (e *benchExecution) benchReadsSequentialNested() error {
	return e.db.View(func(tx *bolt.Tx) error {
		t := time.Now()

		for {
			numReads := uint32(0)
			var top = tx.Bucket(benchBucketName)
			if err := top.ForEach(func(name, _ []byte) error {
				if b := top.Bucket(name); b != nil {
					c := b.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						numReads++
						e.readResults.AddCompletedOps(1)
						if v == nil {
							return ErrInvalidValue
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}

			if e.cfg.writeMode == "seq-nest" && numReads != e.cfg.iterations {
				return fmt.Errorf("read seq-nest: iter mismatch: expected %d, got %d", e.cfg.iterations, numReads)
			}

			// Make sure we do this for at least a second.
			if time.Since(t) >= time.Second {
				break
			}
		}

		return nil
	})
}

func benchCheckProgress(results *benchResults, finishChan chan interface{}) {
	ticker := time.Tick(time.Second)
	lastCompleted, lastTime := int64(0), time.Now()
	for {
		select {
		case <-finishChan:
			return
		case t := <-ticker:
			completed, taken := results.CompletedOps(), t.Sub(lastTime)
			fmt.Fprintf(os.Stderr, "Completed %d requests, %d/s \n",
				completed, ((completed-lastCompleted)*int64(time.Second))/int64(taken),
			)
			lastCompleted, lastTime = completed, t
		}
	}
}

// Starts all profiles set on the options.
func (e *benchExecution) startProfiling() error {
	var err error

	// Start CPU profiling.
	if e.cfg.cpuProfile != "" {
		e.cpuProfile, err = os.Create(e.cfg.cpuProfile)
		if err != nil {
			return fmt.Errorf("could not create cpu profile %q: %v\n", e.cfg.cpuProfile, err)
		}
		err = pprof.StartCPUProfile(e.cpuProfile)
		if err != nil {
			return fmt.Errorf("could not start cpu profile %q: %v\n", e.cfg.cpuProfile, err)
		}
	}

	// Start memory profiling.
	if e.cfg.memProfile != "" {
		e.memProfile, err = os.Create(e.cfg.memProfile)
		if err != nil {
			return fmt.Errorf("could not create memory profile %q: %v\n", e.cfg.memProfile, err)
		}
		runtime.MemProfileRate = 4096
	}

	// Start fatal profiling.
	if e.cfg.blockProfile != "" {
		e.blockProfile, err = os.Create(e.cfg.blockProfile)
		if err != nil {
			return fmt.Errorf("could not create block profile %q: %v\n", e.cfg.blockProfile, err)
		}
		runtime.SetBlockProfileRate(1)
	}

	return nil
}

// Stops all profiles.
func (e *benchExecution) stopProfiling() {
	if e.cpuProfile != nil {
		pprof.StopCPUProfile()
		e.cpuProfile.Close()
		e.cpuProfile = nil
	}

	if e.memProfile != nil {
		err := pprof.Lookup("heap").WriteTo(e.memProfile, 0)
		if err != nil {
			fmt.Fprint(e.stderr, "could not write memory profile")
		}
		e.memProfile.Close()
		e.memProfile = nil
	}

	if e.blockProfile != nil {
		err := pprof.Lookup("block").WriteTo(e.blockProfile, 0)
		if err != nil {
			fmt.Fprint(e.stderr, "could not write block profile")
		}
		e.blockProfile.Close()
		e.blockProfile = nil
		runtime.SetBlockProfileRate(0)
	}
}

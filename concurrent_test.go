package bbolt_test

import (
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	bolt "go.etcd.io/bbolt"
)

type duration struct {
	min time.Duration
	max time.Duration
}

type bytesRange struct {
	min int
	max int
}

type operationChance struct {
	operation OperationType
	chance    int
}

type concurrentConfig struct {
	workInterval   duration
	operationRatio []operationChance
	readInterval   duration   // only used by readOpeartion
	writeBytes     bytesRange // only used by writeOperation
}

/*
TestConcurrentReadAndWrite verifies:
 1. Repeatable read: a read transaction should always see the same data
    view during its lifecycle.
 2. Any data written by a writing transaction should be visible to any
    following reading transactions (with txid >= previous writing txid).
 3. The txid should never decrease.
*/
func TestConcurrentReadAndWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	bucket := []byte("data")
	keys := []string{"key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"}
	conf := concurrentConfig{
		workInterval: duration{
			min: 5 * time.Millisecond,
			max: 10 * time.Millisecond,
		},
		operationRatio: []operationChance{
			{operation: Read, chance: 60},
			{operation: Write, chance: 20},
			{operation: Delete, chance: 20},
		},
		readInterval: duration{
			min: 50 * time.Millisecond,
			max: 100 * time.Millisecond,
		},
		writeBytes: bytesRange{
			min: 200,
			max: 16000,
		},
	}

	testCases := []struct {
		name         string
		workerCount  int
		conf         concurrentConfig
		testDuration time.Duration
	}{
		{
			name:         "1 worker",
			workerCount:  1,
			conf:         conf,
			testDuration: 30 * time.Second,
		},
		{
			name:         "10 workers",
			workerCount:  10,
			conf:         conf,
			testDuration: 30 * time.Second,
		},
		{
			name:         "50 workers",
			workerCount:  50,
			conf:         conf,
			testDuration: 30 * time.Second,
		},
		{
			name:         "100 workers",
			workerCount:  100,
			conf:         conf,
			testDuration: 30 * time.Second,
		},
		{
			name:         "200 workers",
			workerCount:  200,
			conf:         conf,
			testDuration: 30 * time.Second,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			concurrentReadAndWrite(t,
				bucket,
				keys,
				tc.workerCount,
				tc.conf,
				tc.testDuration)
		})
	}
}

func concurrentReadAndWrite(t *testing.T,
	bucket []byte,
	keys []string,
	workerCount int,
	conf concurrentConfig,
	testDuration time.Duration) {

	t.Log("Preparing db.")
	db := mustCreateDB(t, nil)
	defer db.Close()
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucket)
		return err
	})
	require.NoError(t, err)

	var records historyRecords
	// t.Failed() returns false during panicking. We need to forcibly
	// save data on panicking.
	// Refer to: https://github.com/golang/go/issues/49929
	panicked := true
	defer func() {
		saveDataIfFailed(t, db, records, panicked)
	}()

	t.Log("Starting workers.")
	records = runWorkers(t,
		db, bucket, keys,
		workerCount,
		conf,
		testDuration)

	t.Log("Analyzing the history records.")
	if err := validateSequential(records); err != nil {
		t.Errorf("The history records are not sequential:\n %v", err)
	}

	t.Log("Checking database consistency.")
	if err := checkConsistency(t, db); err != nil {
		t.Errorf("The data isn't consistency: %v", err)
	}

	panicked = false
	// TODO (ahrtr):
	//   1. intentionally inject a random failpoint.
}

// mustCreateDB is created in place of `btesting.MustCreateDB`, and it's
// only supposed to be used by the concurrent test case. The purpose is
// to ensure the test case can be executed on old branches or versions,
// e.g. `release-1.3` or `1.3.[5-7]`.
func mustCreateDB(t *testing.T, o *bolt.Options) *bolt.DB {
	f := filepath.Join(t.TempDir(), "db")

	t.Logf("Opening bbolt DB at: %s", f)
	if o == nil {
		o = bolt.DefaultOptions
	}

	freelistType := bolt.FreelistArrayType
	if env := os.Getenv("TEST_FREELIST_TYPE"); env == string(bolt.FreelistMapType) {
		freelistType = bolt.FreelistMapType
	}

	o.FreelistType = freelistType

	db, err := bolt.Open(f, 0666, o)
	require.NoError(t, err)

	return db
}

func checkConsistency(t *testing.T, db *bolt.DB) error {
	return db.View(func(tx *bolt.Tx) error {
		cnt := 0
		for err := range tx.Check() {
			t.Errorf("Consistency error: %v", err)
			cnt++
		}
		if cnt > 0 {
			return fmt.Errorf("%d consistency errors found", cnt)
		}
		return nil
	})
}

/*
*********************************************************
Data structures and functions/methods for running concurrent
workers, which execute different operations, including `Read`,
`Write` and `Delete`.
*********************************************************
*/
func runWorkers(t *testing.T,
	db *bolt.DB,
	bucket []byte,
	keys []string,
	workerCount int,
	conf concurrentConfig,
	testDuration time.Duration) historyRecords {
	stopCh := make(chan struct{}, 1)
	errCh := make(chan error, workerCount)

	var mu sync.Mutex
	var rs historyRecords

	g := new(errgroup.Group)
	for i := 0; i < workerCount; i++ {
		w := &worker{
			id:     i,
			db:     db,
			bucket: bucket,
			keys:   keys,

			conf: conf,

			errCh:  errCh,
			stopCh: stopCh,
			t:      t,
		}
		g.Go(func() error {
			wrs, err := runWorker(t, w, errCh)
			mu.Lock()
			rs = append(rs, wrs...)
			mu.Unlock()
			return err
		})
	}

	t.Logf("Keep all workers running for about %s.", testDuration)
	select {
	case <-time.After(testDuration):
	case <-errCh:
	}

	close(stopCh)
	t.Log("Waiting for all workers to finish.")
	if err := g.Wait(); err != nil {
		t.Errorf("Received error: %v", err)
	}

	return rs
}

func runWorker(t *testing.T, w *worker, errCh chan error) (historyRecords, error) {
	rs, err := w.run()
	if len(rs) > 0 && err == nil {
		if terr := validateIncrementalTxid(rs); terr != nil {
			txidErr := fmt.Errorf("[%s]: %w", w.name(), terr)
			t.Error(txidErr)
			errCh <- txidErr
			return rs, txidErr
		}
	}
	return rs, err
}

type worker struct {
	id int
	db *bolt.DB

	bucket []byte
	keys   []string

	conf concurrentConfig

	errCh  chan error
	stopCh chan struct{}

	t *testing.T
}

func (w *worker) name() string {
	return fmt.Sprintf("worker-%d", w.id)
}

func (w *worker) run() (historyRecords, error) {
	var rs historyRecords
	for {
		select {
		case <-w.stopCh:
			w.t.Logf("%q finished.", w.name())
			return rs, nil
		default:
		}

		op := w.pickOperation()
		rec, err := executeOperation(op, w.db, w.bucket, w.keys, w.conf)
		if err != nil {
			readErr := fmt.Errorf("[%s: %s]: %w", w.name(), op, err)
			w.t.Error(readErr)
			w.errCh <- readErr
			return rs, readErr
		}

		rs = append(rs, rec)
		time.Sleep(randomDurationInRange(w.conf.workInterval.min, w.conf.workInterval.max))
	}
}

func (w *worker) pickOperation() OperationType {
	sum := 0
	for _, op := range w.conf.operationRatio {
		sum += op.chance
	}
	roll := mrand.Int() % sum
	for _, op := range w.conf.operationRatio {
		if roll < op.chance {
			return op.operation
		}
		roll -= op.chance
	}
	panic("unexpected")
}

func executeOperation(op OperationType, db *bolt.DB, bucket []byte, keys []string, conf concurrentConfig) (historyRecord, error) {
	switch op {
	case Read:
		return executeRead(db, bucket, keys, conf.readInterval)
	case Write:
		return executeWrite(db, bucket, keys, conf.writeBytes)
	case Delete:
		return executeDelete(db, bucket, keys)
	default:
		panic(fmt.Sprintf("unexpected operation type: %s", op))
	}
}

func executeRead(db *bolt.DB, bucket []byte, keys []string, readInterval duration) (historyRecord, error) {
	var rec historyRecord
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)

		selectedKey := keys[mrand.Intn(len(keys))]
		initialVal := b.Get([]byte(selectedKey))
		time.Sleep(randomDurationInRange(readInterval.min, readInterval.max))
		val := b.Get([]byte(selectedKey))

		if !reflect.DeepEqual(initialVal, val) {
			return fmt.Errorf("read different values for the same key (%q), value1: %q, value2: %q",
				selectedKey, formatBytes(initialVal), formatBytes(val))
		}

		clonedVal := make([]byte, len(val))
		copy(clonedVal, val)

		rec = historyRecord{
			OperationType: Read,
			Key:           selectedKey,
			Value:         clonedVal,
			Txid:          tx.ID(),
		}

		return nil
	})

	return rec, err
}

func executeWrite(db *bolt.DB, bucket []byte, keys []string, writeBytes bytesRange) (historyRecord, error) {
	var rec historyRecord

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)

		selectedKey := keys[mrand.Intn(len(keys))]

		valueBytes := randomIntInRange(writeBytes.min, writeBytes.max)
		v := make([]byte, valueBytes)
		if _, cErr := crand.Read(v); cErr != nil {
			return cErr
		}

		putErr := b.Put([]byte(selectedKey), v)
		if putErr == nil {
			rec = historyRecord{
				OperationType: Write,
				Key:           selectedKey,
				Value:         v,
				Txid:          tx.ID(),
			}
		}

		return putErr
	})

	return rec, err
}

func executeDelete(db *bolt.DB, bucket []byte, keys []string) (historyRecord, error) {
	var rec historyRecord

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)

		selectedKey := keys[mrand.Intn(len(keys))]

		deleteErr := b.Delete([]byte(selectedKey))
		if deleteErr == nil {
			rec = historyRecord{
				OperationType: Delete,
				Key:           selectedKey,
				Txid:          tx.ID(),
			}
		}

		return deleteErr
	})

	return rec, err
}

func randomDurationInRange(min, max time.Duration) time.Duration {
	d := int64(max) - int64(min)
	d = int64(mrand.Intn(int(d))) + int64(min)
	return time.Duration(d)
}

func randomIntInRange(min, max int) int {
	return mrand.Intn(max-min) + min
}

func formatBytes(val []byte) string {
	if utf8.ValidString(string(val)) {
		return string(val)
	}

	return hex.EncodeToString(val)
}

/*
*********************************************************
Functions for persisting test data, including db file
and operation history
*********************************************************
*/
func saveDataIfFailed(t *testing.T, db *bolt.DB, rs historyRecords, force bool) {
	if t.Failed() || force {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close db: %v", err)
		}
		backupPath := testResultsDirectory(t)
		backupDB(t, db, backupPath)
		persistHistoryRecords(t, rs, backupPath)
	}
}

func backupDB(t *testing.T, db *bolt.DB, path string) {
	targetFile := filepath.Join(path, "db.bak")
	t.Logf("Saving the DB file to %s", targetFile)
	err := copyFile(db.Path(), targetFile)
	require.NoError(t, err)
	t.Logf("DB file saved to %s", targetFile)
}

func copyFile(srcPath, dstPath string) error {
	// Ensure source file exists.
	_, err := os.Stat(srcPath)
	if os.IsNotExist(err) {
		return fmt.Errorf("source file %q not found", srcPath)
	} else if err != nil {
		return err
	}

	// Ensure output file not exist.
	_, err = os.Stat(dstPath)
	if err == nil {
		return fmt.Errorf("output file %q already exists", dstPath)
	} else if !os.IsNotExist(err) {
		return err
	}

	srcDB, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source file %q: %w", srcPath, err)
	}
	defer srcDB.Close()
	dstDB, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %q: %w", dstPath, err)
	}
	defer dstDB.Close()
	written, err := io.Copy(dstDB, srcDB)
	if err != nil {
		return fmt.Errorf("failed to copy database file from %q to %q: %w", srcPath, dstPath, err)
	}

	srcFi, err := srcDB.Stat()
	if err != nil {
		return fmt.Errorf("failed to get source file info %q: %w", srcPath, err)
	}
	initialSize := srcFi.Size()
	if initialSize != written {
		return fmt.Errorf("the byte copied (%q: %d) isn't equal to the initial db size (%q: %d)", dstPath, written, srcPath, initialSize)
	}

	return nil
}

func persistHistoryRecords(t *testing.T, rs historyRecords, path string) {
	recordFilePath := filepath.Join(path, "history_records.json")
	t.Logf("Saving history records to %s", recordFilePath)
	recordFile, err := os.OpenFile(recordFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	require.NoError(t, err)
	defer recordFile.Close()
	encoder := json.NewEncoder(recordFile)
	for _, rec := range rs {
		err := encoder.Encode(rec)
		require.NoError(t, err)
	}
}

func testResultsDirectory(t *testing.T) string {
	resultsDirectory, ok := os.LookupEnv("RESULTS_DIR")
	var err error
	if !ok {
		resultsDirectory, err = os.MkdirTemp("", "*.db")
		require.NoError(t, err)
	}
	resultsDirectory, err = filepath.Abs(resultsDirectory)
	require.NoError(t, err)

	path, err := filepath.Abs(filepath.Join(resultsDirectory, strings.ReplaceAll(t.Name(), "/", "_")))
	require.NoError(t, err)

	err = os.RemoveAll(path)
	require.NoError(t, err)

	err = os.MkdirAll(path, 0700)
	require.NoError(t, err)

	return path
}

/*
*********************************************************
Data structures and functions for analyzing history records
*********************************************************
*/
type OperationType string

const (
	Read   OperationType = "read"
	Write  OperationType = "write"
	Delete OperationType = "delete"
)

type historyRecord struct {
	OperationType OperationType `json:"operationType,omitempty"`
	Txid          int           `json:"txid,omitempty"`
	Key           string        `json:"key,omitempty"`
	Value         []byte        `json:"value,omitempty"`
}

type historyRecords []historyRecord

func (rs historyRecords) Len() int {
	return len(rs)
}

func (rs historyRecords) Less(i, j int) bool {
	// Sorted by key firstly: all records with the same key are grouped together.
	keyCmp := strings.Compare(rs[i].Key, rs[j].Key)
	if keyCmp != 0 {
		return keyCmp < 0
	}

	// Sorted by txid
	if rs[i].Txid != rs[j].Txid {
		return rs[i].Txid < rs[j].Txid
	}

	// Sorted by operation type: put `Read` after other operation types
	// if they operate on the same key and have the same txid.
	if rs[i].OperationType == Read {
		return false
	}

	return true
}

func (rs historyRecords) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

func validateIncrementalTxid(rs historyRecords) error {
	lastTxid := rs[0].Txid

	for i := 1; i < len(rs); i++ {
		if (rs[i].OperationType == Read && rs[i].Txid < lastTxid) || (rs[i].OperationType != Read && rs[i].Txid <= lastTxid) {
			return fmt.Errorf("detected non-incremental txid(%d, %d) in %s mode", lastTxid, rs[i].Txid, rs[i].OperationType)
		}
		lastTxid = rs[i].Txid
	}

	return nil
}

func validateSequential(rs historyRecords) error {
	sort.Sort(rs)

	lastWriteKeyValueMap := make(map[string]*historyRecord)

	for _, rec := range rs {
		if v, ok := lastWriteKeyValueMap[rec.Key]; ok {
			if rec.OperationType == Write {
				v.Value = rec.Value
				v.Txid = rec.Txid
			} else if rec.OperationType == Delete {
				delete(lastWriteKeyValueMap, rec.Key)
			} else {
				if !reflect.DeepEqual(v.Value, rec.Value) {
					return fmt.Errorf("readOperation[txid: %d, key: %s] read %x, \nbut writer[txid: %d, key: %s] wrote %x",
						rec.Txid, rec.Key, rec.Value,
						v.Txid, v.Key, v.Value)
				}
			}
		} else {
			if rec.OperationType == Write {
				lastWriteKeyValueMap[rec.Key] = &historyRecord{
					OperationType: Write,
					Key:           rec.Key,
					Value:         rec.Value,
					Txid:          rec.Txid,
				}
			} else if rec.OperationType == Read {
				if len(rec.Value) != 0 {
					return fmt.Errorf("expected the first readOperation[txid: %d, key: %s] read nil, \nbut got %x",
						rec.Txid, rec.Key, rec.Value)
				}
			}
		}
	}

	return nil
}

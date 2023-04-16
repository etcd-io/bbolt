package bbolt_test

import (
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
)

type duration struct {
	min time.Duration
	max time.Duration
}

type bytesRange struct {
	min int
	max int
}

type concurrentConfig struct {
	readTime   duration
	writeTime  duration
	writeBytes bytesRange
}

/*
TestConcurrentReadAndWrite verifies:
 1. Repeatable read: a read transaction should always see the same data
    view during its lifecycle;
 2. Any data written by a writing transaction should be visible to any
    following reading transactions (with txid >= previous writing txid).
*/
func TestConcurrentReadAndWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	bucket := []byte("data")
	keys := []string{"key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"}

	testCases := []struct {
		name         string
		readerCount  int
		conf         concurrentConfig
		testDuration time.Duration
	}{
		{
			name:        "1 reader",
			readerCount: 1,
			conf: concurrentConfig{
				readTime: duration{
					min: 50 * time.Millisecond,
					max: 100 * time.Millisecond,
				},
				writeTime: duration{
					min: 10 * time.Millisecond,
					max: 20 * time.Millisecond,
				},
				writeBytes: bytesRange{
					min: 200,
					max: 8000,
				},
			},
			testDuration: 30 * time.Second,
		},
		{
			name:        "10 readers",
			readerCount: 10,
			conf: concurrentConfig{
				readTime: duration{
					min: 50 * time.Millisecond,
					max: 100 * time.Millisecond,
				},
				writeTime: duration{
					min: 10 * time.Millisecond,
					max: 20 * time.Millisecond,
				},
				writeBytes: bytesRange{
					min: 200,
					max: 8000,
				},
			},
			testDuration: 30 * time.Second,
		},
		{
			name:        "50 readers",
			readerCount: 50,
			conf: concurrentConfig{
				readTime: duration{
					min: 50 * time.Millisecond,
					max: 100 * time.Millisecond,
				},
				writeTime: duration{
					min: 10 * time.Millisecond,
					max: 20 * time.Millisecond,
				},
				writeBytes: bytesRange{
					min: 500,
					max: 8000,
				},
			},

			testDuration: 30 * time.Second,
		},
		{
			name:        "100 readers",
			readerCount: 100,
			conf: concurrentConfig{
				readTime: duration{
					min: 50 * time.Millisecond,
					max: 100 * time.Millisecond,
				},
				writeTime: duration{
					min: 10 * time.Millisecond,
					max: 20 * time.Millisecond,
				},
				writeBytes: bytesRange{
					min: 500,
					max: 8000,
				},
			},

			testDuration: 30 * time.Second,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			concurrentReadAndWrite(t,
				bucket,
				keys,
				tc.readerCount,
				tc.conf,
				tc.testDuration)
		})
	}
}

func concurrentReadAndWrite(t *testing.T,
	bucket []byte,
	keys []string,
	readerCount int,
	conf concurrentConfig,
	testDuration time.Duration) {

	t.Log("Preparing db.")
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucket)
		return err
	})
	require.NoError(t, err)

	t.Log("Starting workers.")
	records := runWorkers(t,
		db, bucket, keys,
		readerCount,
		conf,
		testDuration)

	t.Log("Analyzing the history records.")
	if err := validateSerializable(records); err != nil {
		t.Errorf("The history records are not serializable:\n %v", err)
	}

	saveDataIfFailed(t, db, records)

	// TODO (ahrtr):
	//   1. intentionally inject a random failpoint.
	//   2. check db consistency at the end.
}

/*
*********************************************************
Data structures and functions/methods for running
concurrent workers, including reading and writing workers
*********************************************************
*/
func runWorkers(t *testing.T,
	db *btesting.DB,
	bucket []byte,
	keys []string,
	readerCount int,
	conf concurrentConfig,
	testDuration time.Duration) historyRecords {
	stopCh := make(chan struct{}, 1)
	errCh := make(chan error, readerCount+1)

	var mu sync.Mutex
	var rs historyRecords

	// start write transaction
	g := new(errgroup.Group)
	writer := &writeWorker{
		id:     0,
		db:     db,
		bucket: bucket,
		keys:   keys,

		writeBytes: conf.writeBytes,
		writeTime:  conf.writeTime,

		errCh:  errCh,
		stopCh: stopCh,
		t:      t,
	}
	g.Go(func() error {
		wrs, err := runWorker(t, writer, errCh)
		mu.Lock()
		rs = append(rs, wrs...)
		mu.Unlock()
		return err
	})

	// start readonly transactions
	for i := 0; i < readerCount; i++ {
		reader := &readWorker{
			id:     i,
			db:     db,
			bucket: bucket,
			keys:   keys,

			readTime: conf.readTime,

			errCh:  errCh,
			stopCh: stopCh,
			t:      t,
		}
		g.Go(func() error {
			rrs, err := runWorker(t, reader, errCh)
			mu.Lock()
			rs = append(rs, rrs...)
			mu.Unlock()
			return err
		})
	}

	t.Logf("Keep reading and writing transactions running for about %s.", testDuration)
	select {
	case <-time.After(testDuration):
	case <-errCh:
	}

	close(stopCh)
	t.Log("Waiting for all transactions to finish.")
	if err := g.Wait(); err != nil {
		t.Errorf("Received error: %v", err)
	}

	return rs
}

func runWorker(t *testing.T, w worker, errCh chan error) (historyRecords, error) {
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

type worker interface {
	name() string
	run() (historyRecords, error)
}

type readWorker struct {
	id int
	db *btesting.DB

	bucket []byte
	keys   []string

	readTime duration

	errCh  chan error
	stopCh chan struct{}

	t *testing.T
}

func (w *readWorker) name() string {
	return fmt.Sprintf("readWorker-%d", w.id)
}

func (r *readWorker) run() (historyRecords, error) {
	var rs historyRecords
	for {
		select {
		case <-r.stopCh:
			r.t.Log("Reading transaction finished.")
			return rs, nil
		default:
		}

		err := r.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(r.bucket)

			selectedKey := r.keys[mrand.Intn(len(r.keys))]
			initialVal := b.Get([]byte(selectedKey))
			time.Sleep(randomDurationInRange(r.readTime.min, r.readTime.max))
			val := b.Get([]byte(selectedKey))

			if !reflect.DeepEqual(initialVal, val) {
				return fmt.Errorf("read different values for the same key (%q), value1: %q, value2: %q",
					selectedKey, formatBytes(initialVal), formatBytes(val))
			}

			clonedVal := make([]byte, len(val))
			copy(clonedVal, val)

			rs = append(rs, historyRecord{
				OperationType: Read,
				Key:           selectedKey,
				Value:         clonedVal,
				Txid:          tx.ID(),
			})

			return nil
		})

		if err != nil {
			readErr := fmt.Errorf("[reader error]: %w", err)
			r.t.Error(readErr)
			r.errCh <- readErr
			return rs, readErr
		}
	}
}

type writeWorker struct {
	id int
	db *btesting.DB

	bucket []byte
	keys   []string

	writeBytes bytesRange
	writeTime  duration

	errCh  chan error
	stopCh chan struct{}

	t *testing.T
}

func (w *writeWorker) name() string {
	return fmt.Sprintf("writeWorker-%d", w.id)
}

func (w *writeWorker) run() (historyRecords, error) {
	var rs historyRecords
	for {
		select {
		case <-w.stopCh:
			w.t.Log("Writing transaction finished.")
			return rs, nil
		default:
		}

		err := w.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(w.bucket)

			selectedKey := w.keys[mrand.Intn(len(w.keys))]

			valueBytes := randomIntInRange(w.writeBytes.min, w.writeBytes.max)
			v := make([]byte, valueBytes)
			if _, cErr := crand.Read(v); cErr != nil {
				return cErr
			}

			putErr := b.Put([]byte(selectedKey), v)
			if putErr == nil {
				rs = append(rs, historyRecord{
					OperationType: Write,
					Key:           selectedKey,
					Value:         v,
					Txid:          tx.ID(),
				})
			}

			return putErr
		})

		if err != nil {
			writeErr := fmt.Errorf("[writer error]: %w", err)
			w.t.Error(writeErr)
			w.errCh <- writeErr
			return rs, writeErr
		}

		time.Sleep(randomDurationInRange(w.writeTime.min, w.writeTime.max))
	}
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
func saveDataIfFailed(t *testing.T, db *btesting.DB, rs historyRecords) {
	if t.Failed() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close db: %v", err)
		}
		backupPath := testResultsDirectory(t)
		backupDB(t, db, backupPath)
		persistHistoryRecords(t, rs, backupPath)
	}
}

func backupDB(t *testing.T, db *btesting.DB, path string) {
	targetFile := filepath.Join(path, "db.bak")
	t.Logf("Saving the DB file to %s", targetFile)
	err := common.CopyFile(db.Path(), targetFile)
	require.NoError(t, err)
	t.Logf("DB file saved to %s", targetFile)
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
	Read  OperationType = "read"
	Write OperationType = "write"
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

	// Sorted by workerType: put writer before reader if they have the same txid.
	if rs[i].OperationType == Write {
		return true
	}

	return false
}

func (rs historyRecords) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

func validateIncrementalTxid(rs historyRecords) error {
	lastTxid := rs[0].Txid

	for i := 1; i < len(rs); i++ {
		if (rs[i].OperationType == Write && rs[i].Txid <= lastTxid) || (rs[i].OperationType == Read && rs[i].Txid < lastTxid) {
			return fmt.Errorf("detected non-incremental txid(%d, %d) in %s mode", lastTxid, rs[i].Txid, rs[i].OperationType)
		}
		lastTxid = rs[i].Txid
	}

	return nil
}

func validateSerializable(rs historyRecords) error {
	sort.Sort(rs)

	lastWriteKeyValueMap := make(map[string]*historyRecord)

	for _, rec := range rs {
		if v, ok := lastWriteKeyValueMap[rec.Key]; ok {
			if rec.OperationType == Write {
				v.Value = rec.Value
				v.Txid = rec.Txid
			} else {
				if !reflect.DeepEqual(v.Value, rec.Value) {
					return fmt.Errorf("reader[txid: %d, key: %s] read %x, \nbut writer[txid: %d, key: %s] wrote %x",
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
			} else {
				if len(rec.Value) != 0 {
					return fmt.Errorf("expected the first reader[txid: %d, key: %s] read nil, \nbut got %x",
						rec.Txid, rec.Key, rec.Value)
				}
			}
		}
	}

	return nil
}

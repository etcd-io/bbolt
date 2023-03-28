package bbolt_test

import (
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/common"
)

func TestConcurrentReadAndWrite(t *testing.T) {
	bucket := []byte("data")
	keys := []string{"key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"}

	testCases := []struct {
		name             string
		readerCount      int
		minReadInterval  time.Duration
		maxReadInterval  time.Duration
		minWriteInterval time.Duration
		maxWriteInterval time.Duration
		minWriteBytes    int
		maxWriteBytes    int
		testDuration     time.Duration
	}{
		{
			name:             "1 reader",
			readerCount:      1,
			minReadInterval:  50 * time.Millisecond,
			maxReadInterval:  100 * time.Millisecond,
			minWriteInterval: 10 * time.Millisecond,
			maxWriteInterval: 20 * time.Millisecond,
			minWriteBytes:    200,
			maxWriteBytes:    8000,
			testDuration:     30 * time.Second,
		},
		{
			name:             "10 readers",
			readerCount:      10,
			minReadInterval:  50 * time.Millisecond,
			maxReadInterval:  100 * time.Millisecond,
			minWriteInterval: 10 * time.Millisecond,
			maxWriteInterval: 20 * time.Millisecond,
			minWriteBytes:    200,
			maxWriteBytes:    8000,
			testDuration:     30 * time.Second,
		},
		{
			name:             "50 readers",
			readerCount:      50,
			minReadInterval:  50 * time.Millisecond,
			maxReadInterval:  100 * time.Millisecond,
			minWriteInterval: 10 * time.Millisecond,
			maxWriteInterval: 20 * time.Millisecond,
			minWriteBytes:    500,
			maxWriteBytes:    8000,
			testDuration:     30 * time.Second,
		},
		{
			name:             "100 readers",
			readerCount:      100,
			minReadInterval:  50 * time.Millisecond,
			maxReadInterval:  100 * time.Millisecond,
			minWriteInterval: 10 * time.Millisecond,
			maxWriteInterval: 20 * time.Millisecond,
			minWriteBytes:    500,
			maxWriteBytes:    8000,
			testDuration:     30 * time.Second,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			concurrentReadAndWrite(t,
				bucket,
				keys,
				tc.readerCount,
				tc.minReadInterval, tc.maxReadInterval,
				tc.minWriteInterval, tc.maxWriteInterval,
				tc.minWriteBytes, tc.maxWriteBytes,
				tc.testDuration)
		})
	}
}

func concurrentReadAndWrite(t *testing.T,
	bucket []byte,
	keys []string,
	readerCount int,
	minReadInterval, maxReadInterval time.Duration,
	minWriteInterval, maxWriteInterval time.Duration,
	minWriteBytes, maxWriteBytes int,
	testDuration time.Duration) {

	// prepare the db
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucket)
		return err
	})
	require.NoError(t, err)

	stopCh := make(chan struct{}, 1)
	errCh := make(chan error, readerCount+1)

	// start readonly transactions
	g := new(errgroup.Group)
	for i := 0; i < readerCount; i++ {
		reader := &readWorker{
			db:              db,
			bucket:          bucket,
			keys:            keys,
			minReadInterval: minReadInterval,
			maxReadInterval: maxReadInterval,
			errCh:           errCh,
			stopCh:          stopCh,
			t:               t,
		}
		g.Go(reader.run)
	}

	// start write transaction
	writer := writeWorker{
		db:               db,
		bucket:           bucket,
		keys:             keys,
		minWriteBytes:    minWriteBytes,
		maxWriteBytes:    maxWriteBytes,
		minWriteInterval: minWriteInterval,
		maxWriteInterval: maxWriteInterval,

		errCh:  errCh,
		stopCh: stopCh,
		t:      t,
	}
	g.Go(writer.run)

	t.Logf("Keep reading and writing transactions running for about %s.", testDuration)
	select {
	case <-time.After(testDuration):
	case <-errCh:
	}

	close(stopCh)
	t.Log("Wait for all transactions to finish.")
	if err := g.Wait(); err != nil {
		t.Errorf("Received error: %v", err)
	}

	saveDataIfFailed(t, db)

	// TODO (ahrtr):
	//   1. intentionally inject a random failpoint.
	//   2. validate the linearizablity: each reading transaction
	//      should read the value written by previous writing transaction.
}

type readWorker struct {
	db *btesting.DB

	bucket []byte
	keys   []string

	minReadInterval time.Duration
	maxReadInterval time.Duration
	errCh           chan error
	stopCh          chan struct{}

	t *testing.T
}

func (reader *readWorker) run() error {
	for {
		select {
		case <-reader.stopCh:
			reader.t.Log("Reading transaction finished.")
			return nil
		default:
		}

		err := reader.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(reader.bucket)

			selectedKey := reader.keys[mrand.Intn(len(reader.keys))]
			initialVal := b.Get([]byte(selectedKey))
			time.Sleep(randomDurationInRange(reader.minReadInterval, reader.maxReadInterval))
			val := b.Get([]byte(selectedKey))

			if !reflect.DeepEqual(initialVal, val) {
				return fmt.Errorf("read different values for the same key (%q), value1: %q, value2: %q",
					selectedKey, formatBytes(initialVal), formatBytes(val))
			}

			return nil
		})

		if err != nil {
			readErr := fmt.Errorf("[reader error]: %w", err)
			reader.t.Log(readErr)
			reader.errCh <- readErr
			return readErr
		}
	}
}

type writeWorker struct {
	db *btesting.DB

	bucket []byte
	keys   []string

	minWriteBytes    int
	maxWriteBytes    int
	minWriteInterval time.Duration
	maxWriteInterval time.Duration
	errCh            chan error
	stopCh           chan struct{}

	t *testing.T
}

func (writer *writeWorker) run() error {
	for {
		select {
		case <-writer.stopCh:
			writer.t.Log("Writing transaction finished.")
			return nil
		default:
		}

		err := writer.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(writer.bucket)

			selectedKey := writer.keys[mrand.Intn(len(writer.keys))]

			valueBytes := randomIntInRange(writer.minWriteBytes, writer.maxWriteBytes)
			v := make([]byte, valueBytes)
			if _, cErr := crand.Read(v); cErr != nil {
				return cErr
			}

			return b.Put([]byte(selectedKey), v)
		})

		if err != nil {
			writeErr := fmt.Errorf("[writer error]: %w", err)
			writer.t.Log(writeErr)
			writer.errCh <- writeErr
			return writeErr
		}

		time.Sleep(randomDurationInRange(writer.minWriteInterval, writer.maxWriteInterval))
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

func saveDataIfFailed(t *testing.T, db *btesting.DB) {
	if t.Failed() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close db: %v", err)
		}
		backupPath := testResultsDirectory(t)
		targetFile := filepath.Join(backupPath, "db.bak")

		t.Logf("Saving the DB file to %s", targetFile)
		err := common.CopyFile(db.Path(), targetFile)
		require.NoError(t, err)
		t.Logf("DB file saved to %s", targetFile)
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

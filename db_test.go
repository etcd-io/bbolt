package bbolt_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

// pageSize is the size of one page in the data file.
const pageSize = 4096

// pageHeaderSize is the size of a page header.
const pageHeaderSize = 16

// meta represents a simplified version of a database meta page for testing.
type meta struct {
	_       uint32
	version uint32
	_       uint32
	_       uint32
	_       [16]byte
	_       uint64
	pgid    uint64
	_       uint64
	_       uint64
}

// Ensure that a database can be opened without error.
func TestOpen(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NotNilf(t, db, "expected db")

	require.Equalf(t, db.Path(), path, "unexpected path")

	require.NoError(t, db.Close())
}

// Regression validation for https://github.com/etcd-io/bbolt/pull/122.
// Tests multiple goroutines simultaneously opening a database.
func TestOpen_MultipleGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	const (
		instances  = 30
		iterations = 30
	)
	path := tempfile()
	defer os.RemoveAll(path)
	var wg sync.WaitGroup
	errCh := make(chan error, iterations*instances)
	for iteration := 0; iteration < iterations; iteration++ {
		for instance := 0; instance < instances; instance++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				db, err := bolt.Open(path, 0o600, nil)
				if err != nil {
					errCh <- err
					return
				}
				if err := db.Close(); err != nil {
					errCh <- err
					return
				}
			}()
		}
		wg.Wait()
	}
	close(errCh)
	for err := range errCh {
		require.NoErrorf(t, err, "error from inside goroutine: %v", err)
	}
}

// Ensure that opening a database with a blank path returns an error.
func TestOpen_ErrPathRequired(t *testing.T) {
	_, err := bolt.Open("", 0o600, nil)
	require.Errorf(t, err, "expected error")
}

// Ensure that opening a database with a bad path returns an error.
func TestOpen_ErrNotExists(t *testing.T) {
	_, err := bolt.Open(filepath.Join(tempfile(), "bad-path"), 0o600, nil)
	require.Errorf(t, err, "expected error")
}

// Ensure that opening a file that is not a Bolt database returns ErrInvalid.
func TestOpen_ErrInvalid(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = fmt.Fprintln(f, "this is not a bolt database")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = bolt.Open(path, 0o600, nil)
	require.Equalf(t, err, berrors.ErrInvalid, "unexpected error: %s", err)
}

// Ensure that opening a file with two invalid versions returns ErrVersionMismatch.
func TestOpen_ErrVersionMismatch(t *testing.T) {
	if pageSize != os.Getpagesize() {
		t.Skip("page size mismatch")
	}

	// Create empty database.
	db := btesting.MustCreateDB(t)
	path := db.Path()

	// Close database.
	require.NoError(t, db.Close())

	// Read data file.
	buf, err := os.ReadFile(path)
	require.NoError(t, err)

	// Rewrite meta pages.
	meta0 := (*meta)(unsafe.Pointer(&buf[pageHeaderSize]))
	meta0.version++
	meta1 := (*meta)(unsafe.Pointer(&buf[pageSize+pageHeaderSize]))
	meta1.version++
	require.NoError(t, os.WriteFile(path, buf, 0o666))

	// Reopen data file.
	_, err = bolt.Open(path, 0o600, nil)
	require.Equalf(t, err, berrors.ErrVersionMismatch, "unexpected error: %s", err)
}

// Ensure that opening a file with two invalid checksums returns ErrChecksum.
func TestOpen_ErrChecksum(t *testing.T) {
	if pageSize != os.Getpagesize() {
		t.Skip("page size mismatch")
	}

	// Create empty database.
	db := btesting.MustCreateDB(t)
	path := db.Path()

	// Close database.
	require.NoError(t, db.Close())

	// Read data file.
	buf, err := os.ReadFile(path)
	require.NoError(t, err)

	// Rewrite meta pages.
	meta0 := (*meta)(unsafe.Pointer(&buf[pageHeaderSize]))
	meta0.pgid++
	meta1 := (*meta)(unsafe.Pointer(&buf[pageSize+pageHeaderSize]))
	meta1.pgid++
	require.NoError(t, os.WriteFile(path, buf, 0o666))

	// Reopen data file.
	_, err = bolt.Open(path, 0o600, nil)
	require.Equalf(t, err, berrors.ErrChecksum, "unexpected error: %s", err)
}

// Ensure that it can read the page size from the second meta page if the first one is invalid.
// The page size is expected to be the OS's page size in this case.
func TestOpen_ReadPageSize_FromMeta1_OS(t *testing.T) {
	// Create empty database.
	db := btesting.MustCreateDB(t)
	path := db.Path()
	// Close the database
	db.MustClose()

	// Read data file.
	buf, err := os.ReadFile(path)
	require.NoError(t, err)

	// Rewrite first meta page.
	meta0 := (*meta)(unsafe.Pointer(&buf[pageHeaderSize]))
	meta0.pgid++
	require.NoError(t, os.WriteFile(path, buf, 0o666))

	// Reopen data file.
	db = btesting.MustOpenDBWithOption(t, path, nil)
	require.Equalf(t, os.Getpagesize(), db.Info().PageSize, "check page size failed")
}

// Ensure that it can read the page size from the second meta page if the first one is invalid.
// The page size is expected to be the given page size in this case.
func TestOpen_ReadPageSize_FromMeta1_Given(t *testing.T) {
	// test page size from 1KB (1024<<0) to 16MB(1024<<14)
	for i := 0; i <= 14; i++ {
		givenPageSize := 1024 << uint(i)
		t.Logf("Testing page size %d", givenPageSize)
		// Create empty database.
		db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: givenPageSize})
		path := db.Path()
		// Close the database
		db.MustClose()

		// Read data file.
		buf, err := os.ReadFile(path)
		require.NoError(t, err)

		// Rewrite meta pages.
		if i%3 == 0 {
			t.Logf("#%d: Intentionally corrupt the first meta page for pageSize %d", i, givenPageSize)
			meta0 := (*meta)(unsafe.Pointer(&buf[pageHeaderSize]))
			meta0.pgid++
			require.NoError(t, os.WriteFile(path, buf, 0o666))
		}

		// Reopen data file.
		db = btesting.MustOpenDBWithOption(t, path, nil)
		require.Equalf(t, givenPageSize, db.Info().PageSize, "check page size failed")
		db.MustClose()
	}
}

// Ensure that opening a database does not increase its size.
// https://github.com/boltdb/bolt/issues/291
func TestOpen_Size(t *testing.T) {
	// Open a data file.
	db := btesting.MustCreateDB(t)

	pagesize := db.Info().PageSize

	// Insert until we get above the minimum 4MB size.
	err := db.Fill([]byte("data"), 1, 10000,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 1000) },
	)
	require.NoError(t, err)

	path := db.Path()
	db.MustClose()

	sz := fileSize(path)
	require.NotEqualf(t, 0, sz, "unexpected new file size: %d", sz)

	db.MustReopen()
	err = db.Update(func(tx *bolt.Tx) error {
		require.NoError(t, tx.Bucket([]byte("data")).Put([]byte{0}, []byte{0}))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	newSz := fileSize(path)
	require.NotEqualf(t, 0, newSz, "unexpected new file size: %d", newSz)

	// Compare the original size with the new size.
	// db size might increase by a few page sizes due to the new small update.
	require.GreaterOrEqualf(t, sz, newSz-5*int64(pagesize), "unexpected file growth: %d => %d", sz, newSz)
}

// Ensure that opening a database beyond the max step size does not increase its size.
// https://github.com/boltdb/bolt/issues/303
func TestOpen_Size_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	// Open a data file.
	db := btesting.MustCreateDB(t)
	path := db.Path()

	pagesize := db.Info().PageSize

	// Insert until we get above the minimum 4MB size.
	var index uint64
	for i := 0; i < 10000; i++ {
		err := db.Update(func(tx *bolt.Tx) error {
			b, _ := tx.CreateBucketIfNotExists([]byte("data"))
			for j := 0; j < 1000; j++ {
				require.NoError(t, b.Put(u64tob(index), make([]byte, 50)))
				index++
			}
			return nil
		})
		require.NoError(t, err)
	}

	// Close database and grab the size.
	require.NoError(t, db.Close())
	sz := fileSize(path)
	require.NotEqualf(t, 0, sz, "unexpected new file size: %d", sz)
	require.GreaterOrEqualf(t, sz, int64(1<<30), "expected larger initial size: %d", sz)

	// Reopen database, update, and check size again.
	db0, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	err = db0.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("data")).Put([]byte{0}, []byte{0})
	})
	require.NoError(t, err)
	require.NoError(t, db0.Close())

	newSz := fileSize(path)
	require.NotEqualf(t, 0, newSz, "unexpected new file size: %d", newSz)

	// Compare the original size with the new size.
	// db size might increase by a few page sizes due to the new small update.
	require.GreaterOrEqualf(t, sz, newSz-5*int64(pagesize), "unexpected file growth: %d => %d", sz, newSz)
}

// Ensure that a re-opened database is consistent.
func TestOpen_Check(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error { return <-tx.Check() }))
	require.NoError(t, db.Close())

	db, err = bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error { return <-tx.Check() }))
	require.NoError(t, db.Close())
}

// Ensure that write errors to the meta file handler during initialization are returned.
func TestOpen_MetaInitWriteError(t *testing.T) {
	t.Skip("pending")
}

// Ensure that a database that is too small returns an error.
func TestOpen_FileTooSmall(t *testing.T) {
	path := tempfile()
	defer os.RemoveAll(path)

	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	pageSize := int64(db.Info().PageSize)
	require.NoError(t, db.Close())

	// corrupt the database
	require.NoError(t, os.Truncate(path, pageSize))

	_, err = bolt.Open(path, 0o600, nil)
	require.ErrorContainsf(t, err, "file size too small", "unexpected error: %s", err)
}

// TestDB_Open_InitialMmapSize tests if having InitialMmapSize large enough
// to hold data from concurrent write transaction resolves the issue that
// read transaction blocks the write transaction and causes deadlock.
// This is a very hacky test since the mmap size is not exposed.
func TestDB_Open_InitialMmapSize(t *testing.T) {
	path := tempfile()
	defer os.Remove(path)

	initMmapSize := 1 << 30  // 1GB
	testWriteSize := 1 << 27 // 134MB

	db, err := bolt.Open(path, 0o600, &bolt.Options{InitialMmapSize: initMmapSize})
	require.NoError(t, err)

	// create a long-running read transaction
	// that never gets closed while writing
	rtx, err := db.Begin(false)
	require.NoError(t, err)

	// create a write transaction
	wtx, err := db.Begin(true)
	require.NoError(t, err)

	b, err := wtx.CreateBucket([]byte("test"))
	require.NoError(t, err)

	// and commit a large write
	require.NoError(t, b.Put([]byte("foo"), make([]byte, testWriteSize)))

	done := make(chan error, 1)

	go func() {
		err := wtx.Commit()
		done <- err
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Errorf("unexpected that the reader blocks writer")
	case err := <-done:
		require.NoError(t, err)
	}

	require.NoError(t, rtx.Rollback())
}

// TestDB_Open_ReadOnly checks a database in read only mode can read but not write.
func TestDB_Open_ReadOnly(t *testing.T) {
	// Create a writable db, write k-v and close it.
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	f := db.Path()
	o := &bolt.Options{ReadOnly: true}
	readOnlyDB, err := bolt.Open(f, 0o600, o)
	if err != nil {
		panic(err)
	}

	require.Truef(t, readOnlyDB.IsReadOnly(), "expect db in read only mode")

	// Read from a read-only transaction.
	err = readOnlyDB.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		require.Truef(t, bytes.Equal(value, []byte("bar")), "expect value 'bar', got %s", value)
		return nil
	})
	require.NoError(t, err)

	// Can't launch read-write transaction.
	_, err = readOnlyDB.Begin(true)
	require.Equalf(t, err, berrors.ErrDatabaseReadOnly, "unexpected error: %s", err)

	require.NoError(t, readOnlyDB.Close())
}

func TestDB_Open_ReadOnly_NoCreate(t *testing.T) {
	f := filepath.Join(t.TempDir(), "db")
	_, err := bolt.Open(f, 0o600, &bolt.Options{ReadOnly: true})
	require.ErrorIs(t, err, os.ErrNotExist)
}

// TestOpen_BigPage checks the database uses bigger pages when
// changing PageSize.
func TestOpen_BigPage(t *testing.T) {
	pageSize := os.Getpagesize()

	db1 := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize * 2})

	db2 := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: pageSize * 4})

	db1sz, db2sz := fileSize(db1.Path()), fileSize(db2.Path())
	assert.Lessf(t, db1sz, db2sz, "expected %d < %d", db1sz, db2sz)
}

// TestOpen_RecoverFreeList tests opening the DB with free-list
// write-out after no free list sync will recover the free list
// and write it out.
func TestOpen_RecoverFreeList(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{NoFreelistSync: true})

	// Write some pages.
	tx, err := db.Begin(true)
	require.NoError(t, err)
	wbuf := make([]byte, 8192)
	for i := 0; i < 100; i++ {
		s := fmt.Sprintf("%d", i)
		b, err := tx.CreateBucket([]byte(s))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte(s), wbuf))
	}
	require.NoError(t, tx.Commit())

	// Generate free pages.
	tx, err = db.Begin(true)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		s := fmt.Sprintf("%d", i)
		b := tx.Bucket([]byte(s))
		require.NotNil(t, b)
		require.NoError(t, b.Delete([]byte(s)))
	}
	require.NoError(t, tx.Commit())
	db.MustClose()

	// Record freelist count from opening with NoFreelistSync.
	db.MustReopen()
	freepages := db.Stats().FreePageN
	require.NotEqualf(t, 0, freepages, "no free pages on NoFreelistSync reopen")
	db.MustClose()

	// Check free page count is reconstructed when opened with freelist sync.
	db.SetOptions(&bolt.Options{})
	db.MustReopen()
	// One less free page for syncing the free list on open.
	freepages--
	fp := db.Stats().FreePageN
	require.GreaterOrEqualf(t, fp, freepages, "closed with %d free pages, opened with %d", freepages, fp)
}

// Ensure that a database cannot open a transaction when it's not open.
func TestDB_Begin_ErrDatabaseNotOpen(t *testing.T) {
	var db bolt.DB
	_, err := db.Begin(false)
	require.Equalf(t, err, berrors.ErrDatabaseNotOpen, "unexpected error: %s", err)
}

// Ensure that a read-write transaction can be retrieved.
func TestDB_BeginRW(t *testing.T) {
	db := btesting.MustCreateDB(t)

	tx, err := db.Begin(true)
	require.NoError(t, err)
	require.NotNilf(t, tx, "expected tx")
	defer func() { require.NoError(t, tx.Commit()) }()

	require.Truef(t, tx.Writable(), "expected writable tx")
	require.Same(t, db.DB, tx.DB())
}

// TestDB_Concurrent_WriteTo checks that issuing WriteTo operations concurrently
// with commits does not produce corrupted db files. It also verifies that all
// readonly transactions, which are created based on the same data view, should
// always read the same data.
func TestDB_Concurrent_WriteTo_and_ConsistentRead(t *testing.T) {
	o := &bolt.Options{
		NoFreelistSync: false,
		PageSize:       4096,
	}
	db := btesting.MustCreateDBWithOption(t, o)

	wtxs, rtxs := 50, 5
	bucketName := []byte("data")

	var dataLock sync.Mutex
	dataCache := make(map[int][]map[string]string)

	var wg sync.WaitGroup
	wg.Add(wtxs * rtxs)
	f := func(round int, tx *bolt.Tx) {
		defer wg.Done()
		time.Sleep(time.Duration(rand.Intn(200)+10) * time.Millisecond)
		f := filepath.Join(t.TempDir(), fmt.Sprintf("%d-bolt-", round))
		err := tx.CopyFile(f, 0o600)
		require.NoError(t, err)

		// read all the data
		b := tx.Bucket(bucketName)
		data := make(map[string]string)
		err = b.ForEach(func(k, v []byte) error {
			data[string(k)] = string(v)
			return nil
		})
		require.NoError(t, err)

		// cache the data
		dataLock.Lock()
		dataSlice := dataCache[round]
		dataSlice = append(dataSlice, data)
		dataCache[round] = dataSlice
		dataLock.Unlock()

		require.NoError(t, tx.Rollback())

		copyOpt := *o
		snap := btesting.MustOpenDBWithOption(t, f, &copyOpt)
		defer snap.MustClose()
		snap.MustCheck()
	}

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucketName)
		return err
	})
	require.NoError(t, err)

	for i := 0; i < wtxs; i++ {
		tx, err := db.Begin(true)
		require.NoError(t, err)

		b := tx.Bucket(bucketName)

		for j := 0; j < rtxs; j++ {
			rtx, rerr := db.Begin(false)
			require.NoError(t, rerr)
			go f(i, rtx)

			for k := 0; k < 10; k++ {
				key, value := fmt.Sprintf("key_%d", rand.Intn(10)), fmt.Sprintf("value_%d", rand.Intn(100))
				perr := b.Put([]byte(key), []byte(value))
				require.NoError(t, perr)
			}
		}
		require.NoError(t, tx.Commit())
	}
	wg.Wait()

	// compare the data. The data generated in the same round
	// should be exactly the same.
	for round, dataSlice := range dataCache {
		data0 := dataSlice[0]

		for i := 1; i < len(dataSlice); i++ {
			datai := dataSlice[i]
			same := reflect.DeepEqual(data0, datai)
			require.Truef(t, same, "found inconsistent data in round %d, data[0]: %v, data[%d] : %v", round, data0, i, datai)
		}
	}
}

// Ensure that opening a transaction while the DB is closed returns an error.
func TestDB_BeginRW_Closed(t *testing.T) {
	var db bolt.DB
	_, err := db.Begin(true)
	require.Equalf(t, err, berrors.ErrDatabaseNotOpen, "unexpected error: %s", err)
}

func TestDB_Close_PendingTx_RW(t *testing.T) { testDB_Close_PendingTx(t, true) }
func TestDB_Close_PendingTx_RO(t *testing.T) { testDB_Close_PendingTx(t, false) }

// Ensure that a database cannot close while transactions are open.
func testDB_Close_PendingTx(t *testing.T, writable bool) {
	db := btesting.MustCreateDB(t)

	// Start transaction.
	tx, err := db.Begin(writable)
	require.NoError(t, err)

	// Open update in separate goroutine.
	startCh := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		startCh <- struct{}{}
		err := db.Close()
		done <- err
	}()
	// wait for the above goroutine to get scheduled.
	<-startCh

	// Ensure database hasn't closed.
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		require.NoErrorf(t, err, "error from inside goroutine: %v", err)
		t.Fatal("database closed too early")
	default:
	}

	// Commit/close transaction.
	if writable {
		require.NoError(t, tx.Commit())
	} else {
		require.NoError(t, tx.Rollback())
	}

	// Ensure database closed now.
	select {
	case err := <-done:
		require.NoErrorf(t, err, "error from inside goroutine: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("database did not close")
	}
}

// Ensure a database can provide a transactional block.
func TestDB_Update(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		require.NoError(t, b.Put([]byte("baz"), []byte("bat")))
		require.NoError(t, b.Delete([]byte("foo")))
		return nil
	})
	require.NoError(t, err)
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		require.Nilf(t, b.Get([]byte("foo")), "expected nil value")
		v := b.Get([]byte("baz"))
		require.Truef(t, bytes.Equal(v, []byte("bat")), "unexpected value: %v", v)
		return nil
	})
	require.NoError(t, err)
}

// Ensure a closed database returns an error while running a transaction block
func TestDB_Update_Closed(t *testing.T) {
	var db bolt.DB
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.Equalf(t, err, berrors.ErrDatabaseNotOpen, "unexpected error: %s", err)
}

// Ensure a panic occurs while trying to commit a managed transaction.
func TestDB_Update_ManualCommit(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var panicked bool
	err := db.Update(func(tx *bolt.Tx) error {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()

			require.NoError(t, tx.Commit())
		}()
		return nil
	})
	require.NoError(t, err)
	require.Truef(t, panicked, "expected panic")
}

// Ensure a panic occurs while trying to rollback a managed transaction.
func TestDB_Update_ManualRollback(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var panicked bool
	err := db.Update(func(tx *bolt.Tx) error {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()

			require.NoError(t, tx.Rollback())
		}()
		return nil
	})
	require.NoError(t, err)
	require.Truef(t, panicked, "expected panic")
}

// Ensure a panic occurs while trying to commit a managed transaction.
func TestDB_View_ManualCommit(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var panicked bool
	err := db.View(func(tx *bolt.Tx) error {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()

			require.NoError(t, tx.Commit())
		}()
		return nil
	})
	require.NoError(t, err)
	require.Truef(t, panicked, "expected panic")
}

// Ensure a panic occurs while trying to rollback a managed transaction.
func TestDB_View_ManualRollback(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var panicked bool
	err := db.View(func(tx *bolt.Tx) error {
		func() {
			defer func() {
				if r := recover(); r != nil {
					panicked = true
				}
			}()

			require.NoError(t, tx.Rollback())
		}()
		return nil
	})
	require.NoError(t, err)
	require.Truef(t, panicked, "expected panic")
}

// Ensure a write transaction that panics does not hold open locks.
func TestDB_Update_Panic(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Panic during update but recover.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Log("recover: update", r)
			}
		}()

		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			require.NoError(t, err)
			panic("omg")
		})
		require.NoError(t, err)
	}()

	// Verify we can update again.
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Verify that our change persisted.
	err = db.Update(func(tx *bolt.Tx) error {
		require.NotNilf(t, tx.Bucket([]byte("widgets")), "expected bucket")
		return nil
	})
	require.NoError(t, err)
}

// Ensure a database can return an error through a read-only transactional block.
func TestDB_View_Error(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.View(func(tx *bolt.Tx) error {
		return errors.New("xxx")
	})
	require.EqualErrorf(t, err, "xxx", "unexpected error: %s", err)
}

// Ensure a read transaction that panics does not hold open locks.
func TestDB_View_Panic(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Panic during view transaction but recover.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Log("recover: view", r)
			}
		}()

		err := db.View(func(tx *bolt.Tx) error {
			require.NotNilf(t, tx.Bucket([]byte("widgets")), "expected bucket")
			panic("omg")
		})
		require.NoError(t, err)
	}()

	// Verify that we can still use read transactions.
	err = db.View(func(tx *bolt.Tx) error {
		require.NotNilf(t, tx.Bucket([]byte("widgets")), "expected bucket")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that DB stats can be returned.
func TestDB_Stats(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	})
	require.NoError(t, err)

	stats := db.Stats()
	require.Equalf(t, int64(2), stats.TxStats.GetPageCount(), "unexpected TxStats.PageCount: %d", stats.TxStats.GetPageCount())
	require.Equalf(t, 0, stats.FreePageN, "unexpected FreePageN != 0: %d", stats.FreePageN)
	require.Equalf(t, 2, stats.PendingPageN, "unexpected PendingPageN != 2: %d", stats.PendingPageN)
}

// Ensure that database pages are in expected order and type.
func TestDB_Consistency(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		err := db.Update(func(tx *bolt.Tx) error {
			err := tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		p, _ := tx.Page(0)
		require.NotNilf(t, p, "expected page")
		require.Equalf(t, "meta", p.Type, "unexpected page type: %s", p.Type)

		p, _ = tx.Page(1)
		require.NotNilf(t, p, "expected page")
		require.Equalf(t, "meta", p.Type, "unexpected page type: %s", p.Type)

		p, _ = tx.Page(2)
		require.NotNilf(t, p, "expected page")
		require.Equalf(t, "free", p.Type, "unexpected page type: %s", p.Type)

		p, _ = tx.Page(3)
		require.NotNilf(t, p, "expected page")
		require.Equalf(t, "free", p.Type, "unexpected page type: %s", p.Type)

		p, _ = tx.Page(4)
		require.NotNilf(t, p, "expected page")
		require.Equalf(t, "leaf", p.Type, "unexpected page type: %s", p.Type)

		p, _ = tx.Page(5)
		require.NotNilf(t, p, "expected page")
		require.Equalf(t, "freelist", p.Type, "unexpected page type: %s", p.Type)

		p, _ = tx.Page(6)
		require.Nilf(t, p, "unexpected page")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that DB stats can be subtracted from one another.
func TestDBStats_Sub(t *testing.T) {
	var a, b bolt.Stats
	a.TxStats.PageCount = 3
	a.FreePageN = 4
	b.TxStats.PageCount = 10
	b.FreePageN = 14
	diff := b.Sub(&a)
	require.Equalf(t, int64(7), diff.TxStats.GetPageCount(), "unexpected TxStats.PageCount: %d", diff.TxStats.GetPageCount())

	// free page stats are copied from the receiver and not subtracted
	require.Equalf(t, 14, diff.FreePageN, "unexpected FreePageN: %d", diff.FreePageN)
}

// Ensure two functions can perform updates in a single batch.
func TestDB_Batch(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Iterate over multiple updates in separate goroutines.
	n := 2
	ch := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			ch <- db.Batch(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("widgets")).Put(u64tob(uint64(i)), []byte{})
			})
		}(i)
	}

	// Check all responses to make sure there's no error.
	for i := 0; i < n; i++ {
		err := <-ch
		require.NoError(t, err)
	}

	// Ensure data is correct.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < n; i++ {
			assert.NotNilf(t, b.Get(u64tob(uint64(i))), "key not found: %d", i)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestDB_Batch_Panic(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var sentinel int
	bork := &sentinel
	var problem interface{}
	var err error

	// Execute a function inside a batch that panics.
	func() {
		defer func() {
			if p := recover(); p != nil {
				problem = p
			}
		}()
		err = db.Batch(func(tx *bolt.Tx) error {
			panic(bork)
		})
	}()

	// Verify there is no error.
	if g, e := err, error(nil); g != e {
		t.Fatalf("wrong error: %v != %v", g, e)
	}
	// Verify the panic was captured.
	if g, e := problem, bork; g != e {
		t.Fatalf("wrong error: %v != %v", g, e)
	}
}

func TestDB_BatchFull(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	})
	require.NoError(t, err)

	const size = 3
	// buffered so we never leak goroutines
	ch := make(chan error, size)
	put := func(i int) {
		ch <- db.Batch(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("widgets")).Put(u64tob(uint64(i)), []byte{})
		})
	}

	db.MaxBatchSize = size
	// high enough to never trigger here
	db.MaxBatchDelay = 1 * time.Hour

	go put(1)
	go put(2)

	// Give the batch a chance to exhibit bugs.
	time.Sleep(10 * time.Millisecond)

	// not triggered yet
	select {
	case <-ch:
		t.Fatalf("batch triggered too early")
	default:
	}

	go put(3)

	// Check all responses to make sure there's no error.
	for i := 0; i < size; i++ {
		err := <-ch
		require.NoError(t, err)
	}

	// Ensure data is correct.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 1; i <= size; i++ {
			assert.NotNilf(t, b.Get(u64tob(uint64(i))), "key not found: %d", i)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestDB_BatchTime(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	})
	require.NoError(t, err)

	const size = 1
	// buffered so we never leak goroutines
	ch := make(chan error, size)
	put := func(i int) {
		ch <- db.Batch(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("widgets")).Put(u64tob(uint64(i)), []byte{})
		})
	}

	db.MaxBatchSize = 1000
	db.MaxBatchDelay = 0

	go put(1)

	// Batch must trigger by time alone.

	// Check all responses to make sure there's no error.
	for i := 0; i < size; i++ {
		err := <-ch
		require.NoError(t, err)
	}

	// Ensure data is correct.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 1; i <= size; i++ {
			assert.NotNilf(t, b.Get(u64tob(uint64(i))), "key not found: %d", i)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestDBUnmap verifes that `dataref`, `data` and `datasz` must be reset
// to zero values respectively after unmapping the db.
func TestDBUnmap(t *testing.T) {
	db := btesting.MustCreateDB(t)

	require.NoError(t, db.DB.Close())

	// Ignore the following error:
	// Error: copylocks: call of reflect.ValueOf copies lock value: go.etcd.io/bbolt.DB contains sync.Once contains sync.Mutex (govet)
	//nolint:govet
	v := reflect.ValueOf(*db.DB)
	dataref := v.FieldByName("dataref")
	data := v.FieldByName("data")
	datasz := v.FieldByName("datasz")
	assert.True(t, dataref.IsNil())
	assert.True(t, data.IsNil())
	assert.True(t, datasz.IsZero())

	// Set db.DB to nil to prevent MustCheck from panicking.
	db.DB = nil
}

// Convenience function for inserting a bunch of keys with 1000 byte values
func fillDBWithKeys(db *btesting.DB, numKeys int) error {
	return db.Fill([]byte("data"), 1, numKeys,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 1000) },
	)
}

// Creates a new database size, forces a specific allocation size jump, and fills it with the number of keys specified
func createFilledDB(t testing.TB, o *bolt.Options, allocSize int, numKeys int) *btesting.DB {
	// Open a data file.
	db := btesting.MustCreateDBWithOption(t, o)
	db.AllocSize = allocSize

	// Insert a reasonable amount of data below the max size.
	err := db.Fill([]byte("data"), 1, numKeys,
		func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
		func(tx int, k int) []byte { return make([]byte, 1000) },
	)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// Ensure that a database cannot exceed its maximum size
// https://github.com/etcd-io/bbolt/issues/928
func TestDB_MaxSizeNotExceeded(t *testing.T) {
	testCases := []struct {
		name    string
		options bolt.Options
	}{
		{
			name: "Standard case",
			options: bolt.Options{
				MaxSize:  5 * 1024 * 1024, // 5 MiB
				PageSize: 4096,
			},
		},
		{
			name: "NoGrowSync",
			options: bolt.Options{
				MaxSize:    5 * 1024 * 1024, // 5 MiB
				PageSize:   4096,
				NoGrowSync: true,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db := createFilledDB(t,
				&testCase.options,
				4*1024*1024, // adjust allocation jumps to 4 MiB
				2000,
			)

			path := db.Path()

			// The data file should be 4 MiB now (expanded once from zero).
			// It should have space for roughly 16 more entries before trying to grow
			// Keep inserting until grow is required
			err := fillDBWithKeys(db, 100)
			require.ErrorIs(t, err, berrors.ErrMaxSizeReached)

			newSz := fileSize(path)
			require.Positivef(t, newSz, "unexpected new file size: %d", newSz)
			assert.LessOrEqualf(t, newSz, int64(db.MaxSize), "The size of the data file should not exceed db.MaxSize")

			err = db.Close()
			require.NoErrorf(t, err, "Closing the re-opened database should succeed")
		})
	}
}

// Ensure that opening a database that is beyond the maximum size succeeds
// The maximum size should only apply to growing the data file
// https://github.com/etcd-io/bbolt/issues/928
func TestDB_MaxSizeExceededCanOpen(t *testing.T) {
	// Open a data file.
	db := createFilledDB(t, nil, 4*1024*1024, 2000) // adjust allocation jumps to 4 MiB, fill with 2000, 1KB keys
	path := db.Path()

	// Insert a reasonable amount of data below the max size.
	err := fillDBWithKeys(db, 2000)
	require.NoErrorf(t, err, "fillDbWithKeys should succeed")

	err = db.Close()
	require.NoErrorf(t, err, "Close should succeed")

	// The data file should be 4 MiB now (expanded once from zero).
	minimumSizeForTest := int64(1024 * 1024)
	newSz := fileSize(path)
	require.GreaterOrEqualf(t, newSz, minimumSizeForTest, "unexpected new file size: %d. Expected at least %d", newSz, minimumSizeForTest)

	// Now try to re-open the database with an extremely small max size
	t.Logf("Reopening bbolt DB at: %s", path)
	db, err = btesting.OpenDBWithOption(t, path, &bolt.Options{
		MaxSize: 1,
	})
	require.NoErrorf(t, err, "Should be able to open database bigger than MaxSize")

	err = db.Close()
	require.NoErrorf(t, err, "Closing the re-opened database should succeed")
}

// Ensure that opening a database that is beyond the maximum size succeeds,
// even when InitialMmapSize is above the limit (mmaps should not affect file size)
// This test exists for platforms where Truncate should not be called during mmap
// https://github.com/etcd-io/bbolt/issues/928
func TestDB_MaxSizeExceededCanOpenWithHighMmap(t *testing.T) {
	if runtime.GOOS == "windows" {
		// In Windows, the file must be expanded to the mmap initial size,
		// so this test doesn't run in Windows.
		t.SkipNow()
	}

	// Open a data file.
	db := createFilledDB(t, nil, 4*1024*1024, 2000) // adjust allocation jumps to 4 MiB, fill with 2000 1KB entries
	path := db.Path()

	err := db.Close()
	require.NoErrorf(t, err, "Close should succeed")

	// The data file should be 4 MiB now (expanded once from zero).
	minimumSizeForTest := int64(1024 * 1024)
	newSz := fileSize(path)
	require.GreaterOrEqualf(t, newSz, minimumSizeForTest, "unexpected new file size: %d. Expected at least %d", newSz, minimumSizeForTest)

	// Now try to re-open the database with an extremely small max size
	t.Logf("Reopening bbolt DB at: %s", path)
	db, err = btesting.OpenDBWithOption(t, path, &bolt.Options{
		MaxSize:         1,
		InitialMmapSize: int(minimumSizeForTest) * 2,
	})
	require.NoErrorf(t, err, "Should be able to open database bigger than MaxSize when InitialMmapSize set high")

	err = db.Close()
	require.NoErrorf(t, err, "Closing the re-opened database should succeed")
}

// Ensure that when InitialMmapSize is above the limit, opening a database
// that is beyond the maximum size fails in Windows.
// In Windows, the file must be expanded to the mmap initial size.
// https://github.com/etcd-io/bbolt/issues/928
func TestDB_MaxSizeExceededDoesNotGrow(t *testing.T) {
	if runtime.GOOS != "windows" {
		// This test is only relevant on Windows
		t.SkipNow()
	}

	// Open a data file.
	db := createFilledDB(t, nil, 4*1024*1024, 2000) // adjust allocation jumps to 4 MiB, fill with 2000 1KB entries
	path := db.Path()

	err := db.Close()
	require.NoErrorf(t, err, "Close should succeed")

	// The data file should be 4 MiB now (expanded once from zero).
	minimumSizeForTest := int64(1024 * 1024)
	newSz := fileSize(path)
	assert.GreaterOrEqualf(t, newSz, minimumSizeForTest, "unexpected new file size: %d. Expected at least %d", newSz, minimumSizeForTest)

	// Now try to re-open the database with an extremely small max size and
	// an initial mmap size to be greater than the actual file size, forcing an illegal grow on open
	t.Logf("Reopening bbolt DB at: %s", path)
	_, err = btesting.OpenDBWithOption(t, path, &bolt.Options{
		MaxSize:         1,
		InitialMmapSize: int(newSz) * 2,
	})
	assert.Errorf(t, err, "Opening the DB with InitialMmapSize > MaxSize should cause an error on Windows")
}

func ExampleDB_Update() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0o600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Execute several commands within a read-write transaction.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Read the value back from a separate read-only transaction.
	if err := db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value of 'foo' is: %s\n", value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release the file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// The value of 'foo' is: bar
}

func ExampleDB_View() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0o600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Insert data into a bucket.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("people"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("john"), []byte("doe")); err != nil {
			return err
		}
		if err := b.Put([]byte("susy"), []byte("que")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Access data from within a read-only transactional block.
	if err := db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("people")).Get([]byte("john"))
		fmt.Printf("John's last name is %s.\n", v)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release the file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// John's last name is doe.
}

func ExampleDB_Begin() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0o600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Create a bucket using a read-write transaction.
	if err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	}); err != nil {
		log.Fatal(err)
	}

	// Create several keys in a transaction.
	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}
	b := tx.Bucket([]byte("widgets"))
	if err = b.Put([]byte("john"), []byte("blue")); err != nil {
		log.Fatal(err)
	}
	if err = b.Put([]byte("abby"), []byte("red")); err != nil {
		log.Fatal(err)
	}
	if err = b.Put([]byte("zephyr"), []byte("purple")); err != nil {
		log.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Iterate over the values in sorted key order.
	tx, err = db.Begin(false)
	if err != nil {
		log.Fatal(err)
	}
	c := tx.Bucket([]byte("widgets")).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Printf("%s likes %s\n", k, v)
	}

	if err = tx.Rollback(); err != nil {
		log.Fatal(err)
	}

	if err = db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// abby likes red
	// john likes blue
	// zephyr likes purple
}

func BenchmarkDBBatchAutomatic(b *testing.B) {
	db := btesting.MustCreateDB(b)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	})
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := make(chan struct{})
		var wg sync.WaitGroup

		for round := 0; round < 1000; round++ {
			wg.Add(1)

			go func(id uint32) {
				defer wg.Done()
				<-start

				h := fnv.New32a()
				buf := make([]byte, 4)
				binary.LittleEndian.PutUint32(buf, id)
				_, _ = h.Write(buf[:])
				k := h.Sum(nil)
				insert := func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("bench"))
					return b.Put(k, []byte("filler"))
				}
				if err := db.Batch(insert); err != nil {
					b.Error(err)
					return
				}
			}(uint32(round))
		}
		close(start)
		wg.Wait()
	}

	b.StopTimer()
	validateBatchBench(b, db)
}

func BenchmarkDBBatchSingle(b *testing.B) {
	db := btesting.MustCreateDB(b)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	})
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := make(chan struct{})
		var wg sync.WaitGroup

		for round := 0; round < 1000; round++ {
			wg.Add(1)
			go func(id uint32) {
				defer wg.Done()
				<-start

				h := fnv.New32a()
				buf := make([]byte, 4)
				binary.LittleEndian.PutUint32(buf, id)
				_, _ = h.Write(buf[:])
				k := h.Sum(nil)
				insert := func(tx *bolt.Tx) error {
					b := tx.Bucket([]byte("bench"))
					return b.Put(k, []byte("filler"))
				}
				if err := db.Update(insert); err != nil {
					b.Error(err)
					return
				}
			}(uint32(round))
		}
		close(start)
		wg.Wait()
	}

	b.StopTimer()
	validateBatchBench(b, db)
}

func BenchmarkDBBatchManual10x100(b *testing.B) {
	db := btesting.MustCreateDB(b)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("bench"))
		return err
	})
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		errCh := make(chan error, 10)

		for major := 0; major < 10; major++ {
			wg.Add(1)
			go func(id uint32) {
				defer wg.Done()
				<-start

				insert100 := func(tx *bolt.Tx) error {
					h := fnv.New32a()
					buf := make([]byte, 4)
					for minor := uint32(0); minor < 100; minor++ {
						binary.LittleEndian.PutUint32(buf, uint32(id*100+minor))
						h.Reset()
						_, _ = h.Write(buf[:])
						k := h.Sum(nil)
						b := tx.Bucket([]byte("bench"))
						if err := b.Put(k, []byte("filler")); err != nil {
							return err
						}
					}
					return nil
				}
				err := db.Update(insert100)
				errCh <- err
			}(uint32(major))
		}
		close(start)
		wg.Wait()
		close(errCh)
		for err := range errCh {
			require.NoError(b, err)
		}
	}

	b.StopTimer()
	validateBatchBench(b, db)
}

func validateBatchBench(b *testing.B, db *btesting.DB) {
	rollback := errors.New("sentinel error to cause rollback")
	validate := func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("bench"))
		h := fnv.New32a()
		buf := make([]byte, 4)
		for id := uint32(0); id < 1000; id++ {
			binary.LittleEndian.PutUint32(buf, id)
			h.Reset()
			_, _ = h.Write(buf[:])
			k := h.Sum(nil)
			v := bucket.Get(k)
			if v == nil {
				b.Errorf("not found id=%d key=%x", id, k)
				continue
			}
			if g, e := v, []byte("filler"); !bytes.Equal(g, e) {
				b.Errorf("bad value for id=%d key=%x: %s != %q", id, k, g, e)
			}
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}
		// should be empty now
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			b.Errorf("unexpected key: %x = %q", k, v)
		}
		return rollback
	}
	if err := db.Update(validate); err != nil && err != rollback {
		b.Error(err)
	}
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, err := os.CreateTemp("", "bolt-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}

func trunc(b []byte, length int) []byte {
	if length < len(b) {
		return b[:length]
	}
	return b
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

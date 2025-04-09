package bbolt_test

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

// TestTx_Check_ReadOnly tests consistency checking on a ReadOnly database.
func TestTx_Check_ReadOnly(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	readOnlyDB, err := bolt.Open(db.Path(), 0600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer readOnlyDB.Close()

	tx, err := readOnlyDB.Begin(false)
	require.NoError(t, err)
	// ReadOnly DB will load freelist on Check call.
	numChecks := 2
	errc := make(chan error, numChecks)
	check := func() {
		errc <- <-tx.Check()
	}
	// Ensure the freelist is not reloaded and does not race.
	for i := 0; i < numChecks; i++ {
		go check()
	}
	for i := 0; i < numChecks; i++ {
		err := <-errc
		require.NoError(t, err)
	}
	// Close the view transaction
	require.NoError(t, tx.Rollback())
}

// Ensure that committing a closed transaction returns an error.
func TestTx_Commit_ErrTxClosed(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(true)
	require.NoError(t, err)

	_, err = tx.CreateBucket([]byte("foo"))
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	require.Equalf(t, tx.Commit(), berrors.ErrTxClosed, "unexpected error")
}

// Ensure that rolling back a closed transaction returns an error.
func TestTx_Rollback_ErrTxClosed(t *testing.T) {
	db := btesting.MustCreateDB(t)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())
	require.Equalf(t, tx.Rollback(), berrors.ErrTxClosed, "unexpected error")
}

// Ensure that committing a read-only transaction returns an error.
func TestTx_Commit_ErrTxNotWritable(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(false)
	require.NoError(t, err)
	require.Equal(t, tx.Commit(), berrors.ErrTxNotWritable)
	// Close the view transaction
	require.NoError(t, tx.Rollback())
}

// Ensure that a transaction can retrieve a cursor on the root bucket.
func TestTx_Cursor(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		_, err = tx.CreateBucket([]byte("woojits"))
		require.NoError(t, err)

		c := tx.Cursor()
		k, v := c.First()
		require.Truef(t, bytes.Equal(k, []byte("widgets")), "unexpected key: %v", k)
		require.Nilf(t, v, "unexpected value: %v", v)

		k, v = c.Next()
		require.Truef(t, bytes.Equal(k, []byte("woojits")), "unexpected key: %v", k)
		require.Nilf(t, v, "unexpected value: %v", v)

		k, v = c.Next()
		require.Nilf(t, k, "unexpected key: %v", k)
		require.Nilf(t, v, "unexpected value: %v", k)

		return nil
	})
	require.NoError(t, err)
}

// Ensure that creating a bucket with a read-only transaction returns an error.
func TestTx_CreateBucket_ErrTxNotWritable(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.View(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("foo"))
		require.Equalf(t, err, berrors.ErrTxNotWritable, "unexpected error: %s", err)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that creating a bucket on a closed transaction returns an error.
func TestTx_CreateBucket_ErrTxClosed(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(true)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	_, err = tx.CreateBucket([]byte("foo"))
	require.Equalf(t, err, berrors.ErrTxClosed, "unexpected error: %s", err)
}

// Ensure that a Tx can retrieve a bucket.
func TestTx_Bucket(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NotNilf(t, tx.Bucket([]byte("widgets")), "expected bucket")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a Tx retrieving a non-existent key returns nil.
func TestTx_Get_NotFound(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		require.Nilf(t, b.Get([]byte("no_such_key")), "expected nil value")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can be created and retrieved.
func TestTx_CreateBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Create a bucket.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NotNilf(t, b, "expected bucket")
		return nil
	})
	require.NoError(t, err)

	// Read the bucket through a separate transaction.
	err = db.View(func(tx *bolt.Tx) error {
		require.NotNilf(t, tx.Bucket([]byte("widgets")), "expected bucket")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can be created if it doesn't already exist.
func TestTx_CreateBucketIfNotExists(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		// Create bucket.
		b, err := tx.CreateBucketIfNotExists([]byte("widgets"))
		require.NoError(t, err)
		require.NotNilf(t, b, "expected bucket")

		// Create bucket again.
		b, err = tx.CreateBucketIfNotExists([]byte("widgets"))
		require.NoError(t, err)
		require.NotNilf(t, b, "expected bucket")

		return nil
	})
	require.NoError(t, err)

	// Read the bucket through a separate transaction.
	err = db.View(func(tx *bolt.Tx) error {
		require.NotNilf(t, tx.Bucket([]byte("widgets")), "expected bucket")
		return nil
	})
	require.NoError(t, err)
}

// Ensure transaction returns an error if creating an unnamed bucket.
func TestTx_CreateBucketIfNotExists_ErrBucketNameRequired(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte{})
		require.Equalf(t, err, berrors.ErrBucketNameRequired, "unexpected error: %s", err)

		_, err = tx.CreateBucketIfNotExists(nil)
		require.Equalf(t, err, berrors.ErrBucketNameRequired, "unexpected error: %s", err)

		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket cannot be created twice.
func TestTx_CreateBucket_ErrBucketExists(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Create a bucket.
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Create the same bucket again.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.Equalf(t, err, berrors.ErrBucketExists, "unexpected error: %s", err)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket is created with a non-blank name.
func TestTx_CreateBucket_ErrBucketNameRequired(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(nil)
		require.Equalf(t, err, berrors.ErrBucketNameRequired, "unexpected error: %s", err)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can be deleted.
func TestTx_DeleteBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Create a bucket and add a value.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		return nil
	})
	require.NoError(t, err)

	// Delete the bucket and make sure we can't get the value.
	err = db.Update(func(tx *bolt.Tx) error {
		require.NoError(t, tx.DeleteBucket([]byte("widgets")))
		require.Nilf(t, tx.Bucket([]byte("widgets")), "unexpected bucket")
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		// Create the bucket again and make sure there's not a phantom value.
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.Nilf(t, b.Get([]byte("foo")), "unexpected phantom value")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that deleting a bucket on a closed transaction returns an error.
func TestTx_DeleteBucket_ErrTxClosed(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(true)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.Equalf(t, tx.DeleteBucket([]byte("foo")), berrors.ErrTxClosed, "unexpected error")
}

// Ensure that deleting a bucket with a read-only transaction returns an error.
func TestTx_DeleteBucket_ReadOnly(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.View(func(tx *bolt.Tx) error {
		require.Equalf(t, tx.DeleteBucket([]byte("foo")), berrors.ErrTxNotWritable, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that nothing happens when deleting a bucket that doesn't exist.
func TestTx_DeleteBucket_NotFound(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		require.Equalf(t, tx.DeleteBucket([]byte("widgets")), berrors.ErrBucketNotFound, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that no error is returned when a tx.ForEach function does not return
// an error.
func TestTx_ForEach_NoError(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))

		err = tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return nil
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that an error is returned when a tx.ForEach function returns an error.
func TestTx_ForEach_WithError(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))

		marker := errors.New("marker")
		err = tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return marker
		})
		require.Equalf(t, err, marker, "unexpected error: %s", err)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that Tx commit handlers are called after a transaction successfully commits.
func TestTx_OnCommit(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var x int
	err := db.Update(func(tx *bolt.Tx) error {
		tx.OnCommit(func() { x += 1 })
		tx.OnCommit(func() { x += 2 })
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
	require.Equalf(t, 3, x, "unexpected x: %d", x)
}

// Ensure that Tx commit handlers are NOT called after a transaction rolls back.
func TestTx_OnCommit_Rollback(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var x int
	err := db.Update(func(tx *bolt.Tx) error {
		tx.OnCommit(func() { x += 1 })
		tx.OnCommit(func() { x += 2 })
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return errors.New("rollback this commit")
	})
	require.EqualErrorf(t, err, "rollback this commit", "unexpected error: %s", err)
	require.Equalf(t, 0, x, "unexpected x: %d", x)
}

// Ensure that the database can be copied to a file path.
func TestTx_CopyFile(t *testing.T) {
	db := btesting.MustCreateDB(t)

	path := tempfile()
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		require.NoError(t, b.Put([]byte("baz"), []byte("bat")))
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(path, 0600)
	})
	require.NoError(t, err)

	db2, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)

	err = db2.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		require.Truef(t, bytes.Equal(v, []byte("bar")), "unexpected value: %v", v)
		v = tx.Bucket([]byte("widgets")).Get([]byte("baz"))
		require.Truef(t, bytes.Equal(v, []byte("bat")), "unexpected value: %v", v)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db2.Close())
}

type failWriterError struct{}

func (failWriterError) Error() string {
	return "error injected for tests"
}

type failWriter struct {
	// fail after this many bytes
	After int
}

func (f *failWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	if n > f.After {
		n = f.After
		err = failWriterError{}
	}
	f.After -= n
	return n, err
}

// Ensure that Copy handles write errors right.
func TestTx_CopyFile_Error_Meta(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		require.NoError(t, b.Put([]byte("baz"), []byte("bat")))
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		return tx.Copy(&failWriter{})
	})
	require.EqualErrorf(t, err, "meta 0 copy: error injected for tests", "unexpected error: %v", err)
}

// Ensure that Copy handles write errors right.
func TestTx_CopyFile_Error_Normal(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		require.NoError(t, b.Put([]byte("baz"), []byte("bat")))
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		return tx.Copy(&failWriter{3 * db.Info().PageSize})
	})
	require.EqualErrorf(t, err, "error injected for tests", "unexpected error: %v", err)
}

// TestTx_Rollback ensures there is no error when tx rollback whether we sync freelist or not.
func TestTx_Rollback(t *testing.T) {
	for _, isSyncFreelist := range []bool{false, true} {
		// Open the database.
		db, err := bolt.Open(tempfile(), 0600, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer os.Remove(db.Path())
		db.NoFreelistSync = isSyncFreelist

		tx, err := db.Begin(true)
		require.NoErrorf(t, err, "Error starting tx: %v", err)
		bucket := []byte("mybucket")
		_, err = tx.CreateBucket(bucket)
		require.NoErrorf(t, err, "Error creating bucket: %v", err)
		require.NoErrorf(t, tx.Commit(), "Error on commit")

		tx, err = db.Begin(true)
		require.NoErrorf(t, err, "Error starting tx: %v", err)
		b := tx.Bucket(bucket)
		require.NoErrorf(t, b.Put([]byte("k"), []byte("v")), "Error on put")
		// Imagine there is an error and tx needs to be rolled-back
		require.NoErrorf(t, tx.Rollback(), "Error on rollback")

		tx, err = db.Begin(false)
		require.NoErrorf(t, err, "Error starting tx: %v", err)
		b = tx.Bucket(bucket)
		require.Nilf(t, b.Get([]byte("k")), "Value for k should not have been stored")
		require.NoErrorf(t, tx.Rollback(), "Error on rollback")

	}
}

// TestTx_releaseRange ensures db.freePages handles page releases
// correctly when there are transaction that are no longer reachable
// via any read/write transactions and are "between" ongoing read
// transactions, which requires they must be freed by
// freelist.releaseRange.
func TestTx_releaseRange(t *testing.T) {
	// Set initial mmap size well beyond the limit we will hit in this
	// test, since we are testing with long running read transactions
	// and will deadlock if db.grow is triggered.
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{InitialMmapSize: os.Getpagesize() * 100})

	bucket := "bucket"

	put := func(key, value string) {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucket))
			require.NoError(t, err)
			return b.Put([]byte(key), []byte(value))
		})
		require.NoError(t, err)
	}

	del := func(key string) {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucket))
			require.NoError(t, err)
			return b.Delete([]byte(key))
		})
		require.NoError(t, err)
	}

	getWithTxn := func(txn *bolt.Tx, key string) []byte {
		return txn.Bucket([]byte(bucket)).Get([]byte(key))
	}

	openReadTxn := func() *bolt.Tx {
		readTx, err := db.Begin(false)
		require.NoError(t, err)
		return readTx
	}

	checkWithReadTxn := func(txn *bolt.Tx, key string, wantValue []byte) {
		value := getWithTxn(txn, key)
		assert.Truef(t, bytes.Equal(value, wantValue), "Wanted value to be %s for key %s, but got %s", wantValue, key, string(value))
	}

	rollback := func(txn *bolt.Tx) {
		require.NoError(t, txn.Rollback())
	}

	put("k1", "v1")
	rtx1 := openReadTxn()
	put("k2", "v2")
	hold1 := openReadTxn()
	put("k3", "v3")
	hold2 := openReadTxn()
	del("k3")
	rtx2 := openReadTxn()
	del("k1")
	hold3 := openReadTxn()
	del("k2")
	hold4 := openReadTxn()
	put("k4", "v4")
	hold5 := openReadTxn()

	// Close the read transactions we established to hold a portion of the pages in pending state.
	rollback(hold1)
	rollback(hold2)
	rollback(hold3)
	rollback(hold4)
	rollback(hold5)

	// Execute a write transaction to trigger a releaseRange operation in the db
	// that will free multiple ranges between the remaining open read transactions, now that the
	// holds have been rolled back.
	put("k4", "v4")

	// Check that all long running reads still read correct values.
	checkWithReadTxn(rtx1, "k1", []byte("v1"))
	checkWithReadTxn(rtx2, "k2", []byte("v2"))
	rollback(rtx1)
	rollback(rtx2)

	// Check that the final state is correct.
	rtx7 := openReadTxn()
	checkWithReadTxn(rtx7, "k1", nil)
	checkWithReadTxn(rtx7, "k2", nil)
	checkWithReadTxn(rtx7, "k3", nil)
	checkWithReadTxn(rtx7, "k4", []byte("v4"))
	rollback(rtx7)
}

func ExampleTx_Rollback() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Create a bucket.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	}); err != nil {
		log.Fatal(err)
	}

	// Set a value for a key.
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
	}); err != nil {
		log.Fatal(err)
	}

	// Update the key but rollback the transaction so it never saves.
	tx, err := db.Begin(true)
	if err != nil {
		log.Fatal(err)
	}
	b := tx.Bucket([]byte("widgets"))
	if err := b.Put([]byte("foo"), []byte("baz")); err != nil {
		log.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		log.Fatal(err)
	}

	// Ensure that our original value is still set.
	if err := db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value for 'foo' is still: %s\n", value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// The value for 'foo' is still: bar
}

func ExampleTx_CopyFile() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Create a bucket and a key.
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

	// Copy the database to another file.
	toFile := tempfile()
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(toFile, 0666)
	}); err != nil {
		log.Fatal(err)
	}
	defer os.Remove(toFile)

	// Open the cloned database.
	db2, err := bolt.Open(toFile, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Ensure that the key exists in the copy.
	if err := db2.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value for 'foo' in the clone is: %s\n", value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	if err := db2.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// The value for 'foo' in the clone is: bar
}

func TestTxStats_GetAndIncAtomically(t *testing.T) {
	var stats bolt.TxStats

	stats.IncPageCount(1)
	assert.Equal(t, int64(1), stats.GetPageCount())

	stats.IncPageAlloc(2)
	assert.Equal(t, int64(2), stats.GetPageAlloc())

	stats.IncCursorCount(3)
	assert.Equal(t, int64(3), stats.GetCursorCount())

	stats.IncNodeCount(100)
	assert.Equal(t, int64(100), stats.GetNodeCount())

	stats.IncNodeDeref(101)
	assert.Equal(t, int64(101), stats.GetNodeDeref())

	stats.IncRebalance(1000)
	assert.Equal(t, int64(1000), stats.GetRebalance())

	stats.IncRebalanceTime(1001 * time.Second)
	assert.Equal(t, 1001*time.Second, stats.GetRebalanceTime())

	stats.IncSplit(10000)
	assert.Equal(t, int64(10000), stats.GetSplit())

	stats.IncSpill(10001)
	assert.Equal(t, int64(10001), stats.GetSpill())

	stats.IncSpillTime(10001 * time.Second)
	assert.Equal(t, 10001*time.Second, stats.GetSpillTime())

	stats.IncWrite(100000)
	assert.Equal(t, int64(100000), stats.GetWrite())

	stats.IncWriteTime(100001 * time.Second)
	assert.Equal(t, 100001*time.Second, stats.GetWriteTime())

	assert.Equal(t,
		bolt.TxStats{
			PageCount:     1,
			PageAlloc:     2,
			CursorCount:   3,
			NodeCount:     100,
			NodeDeref:     101,
			Rebalance:     1000,
			RebalanceTime: 1001 * time.Second,
			Split:         10000,
			Spill:         10001,
			SpillTime:     10001 * time.Second,
			Write:         100000,
			WriteTime:     100001 * time.Second,
		},
		stats,
	)
}

func TestTxStats_Sub(t *testing.T) {
	statsA := bolt.TxStats{
		PageCount:     1,
		PageAlloc:     2,
		CursorCount:   3,
		NodeCount:     100,
		NodeDeref:     101,
		Rebalance:     1000,
		RebalanceTime: 1001 * time.Second,
		Split:         10000,
		Spill:         10001,
		SpillTime:     10001 * time.Second,
		Write:         100000,
		WriteTime:     100001 * time.Second,
	}

	statsB := bolt.TxStats{
		PageCount:     2,
		PageAlloc:     3,
		CursorCount:   4,
		NodeCount:     101,
		NodeDeref:     102,
		Rebalance:     1001,
		RebalanceTime: 1002 * time.Second,
		Split:         11001,
		Spill:         11002,
		SpillTime:     11002 * time.Second,
		Write:         110001,
		WriteTime:     110010 * time.Second,
	}

	diff := statsB.Sub(&statsA)
	assert.Equal(t, int64(1), diff.GetPageCount())
	assert.Equal(t, int64(1), diff.GetPageAlloc())
	assert.Equal(t, int64(1), diff.GetCursorCount())
	assert.Equal(t, int64(1), diff.GetNodeCount())
	assert.Equal(t, int64(1), diff.GetNodeDeref())
	assert.Equal(t, int64(1), diff.GetRebalance())
	assert.Equal(t, time.Second, diff.GetRebalanceTime())
	assert.Equal(t, int64(1001), diff.GetSplit())
	assert.Equal(t, int64(1001), diff.GetSpill())
	assert.Equal(t, 1001*time.Second, diff.GetSpillTime())
	assert.Equal(t, int64(10001), diff.GetWrite())
	assert.Equal(t, 10009*time.Second, diff.GetWriteTime())
}

// TestTx_TruncateBeforeWrite ensures the file is truncated ahead whether we sync freelist or not.
func TestTx_TruncateBeforeWrite(t *testing.T) {
	if runtime.GOOS == "windows" {
		return
	}
	for _, isSyncFreelist := range []bool{false, true} {
		t.Run(fmt.Sprintf("isSyncFreelist:%v", isSyncFreelist), func(t *testing.T) {
			// Open the database.
			db := btesting.MustCreateDBWithOption(t, &bolt.Options{
				NoFreelistSync: isSyncFreelist,
			})

			bigvalue := make([]byte, db.AllocSize/100)
			count := 0
			for {
				count++
				tx, err := db.Begin(true)
				require.NoError(t, err)
				b, err := tx.CreateBucketIfNotExists([]byte("bucket"))
				require.NoError(t, err)
				err = b.Put([]byte{byte(count)}, bigvalue)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())

				size := fileSize(db.Path())

				if size > int64(db.AllocSize) && size < int64(db.AllocSize)*2 {
					// db.grow expands the file aggresively, that double the size while smaller than db.AllocSize,
					// or increase with a step of db.AllocSize if larger, by which we can test if db.grow has run.
					t.Fatalf("db.grow doesn't run when file size changes. file size: %d", size)
				}
				if size > int64(db.AllocSize) {
					break
				}
			}
			db.MustClose()
			db.MustDeleteFile()
		})
	}
}

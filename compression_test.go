package bbolt_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// TestCompression_BasicWriteRead verifies that data written with compression
// enabled can be read back correctly.
func TestCompression_BasicWriteRead(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{Compression: true})

	// Write data.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("test"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			// Use repeating bytes to make data compressible.
			value := bytes.Repeat([]byte(fmt.Sprintf("value-%05d-", i)), 10)
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Read data back.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		require.NotNil(t, b)
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			expected := bytes.Repeat([]byte(fmt.Sprintf("value-%05d-", i)), 10)
			value := b.Get(key)
			require.Equal(t, expected, value, "mismatch for key %s", key)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompression_CursorTraversal verifies that cursor operations work
// correctly with compressed pages.
func TestCompression_CursorTraversal(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{Compression: true})

	const numKeys = 200

	// Write data.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("cursor-test"))
		if err != nil {
			return err
		}
		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := bytes.Repeat([]byte{byte(i % 256)}, 100)
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Traverse with cursor.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("cursor-test"))
		require.NotNil(t, b)

		c := b.Cursor()
		count := 0
		for k, v := c.First(); k != nil; k, v = c.Next() {
			expected := []byte(fmt.Sprintf("key-%05d", count))
			require.Equal(t, expected, k)
			require.Len(t, v, 100)
			count++
		}
		require.Equal(t, numKeys, count)

		// Seek.
		k, v := c.Seek([]byte("key-00050"))
		require.Equal(t, []byte("key-00050"), k)
		require.Len(t, v, 100)

		return nil
	})
	require.NoError(t, err)
}

// TestCompression_MultipleTransactions verifies that multiple write
// transactions work correctly with compression.
func TestCompression_MultipleTransactions(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{Compression: true})

	// First transaction: write initial data.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("multi"))
		if err != nil {
			return err
		}
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := bytes.Repeat([]byte("a"), 200)
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Second transaction: add more data.
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("multi"))
		for i := 50; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := bytes.Repeat([]byte("b"), 200)
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Verify all data.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("multi"))
		require.NotNil(t, b)
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := b.Get(key)
			require.Equal(t, bytes.Repeat([]byte("a"), 200), value)
		}
		for i := 50; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := b.Get(key)
			require.Equal(t, bytes.Repeat([]byte("b"), 200), value)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompression_NestedBuckets verifies that nested buckets work
// correctly with compression.
func TestCompression_NestedBuckets(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{Compression: true})

	err := db.Update(func(tx *bolt.Tx) error {
		parent, err := tx.CreateBucket([]byte("parent"))
		if err != nil {
			return err
		}
		child, err := parent.CreateBucket([]byte("child"))
		if err != nil {
			return err
		}
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("nested-key-%05d", i))
			value := bytes.Repeat([]byte("nested"), 20)
			if err := child.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		parent := tx.Bucket([]byte("parent"))
		require.NotNil(t, parent)
		child := parent.Bucket([]byte("child"))
		require.NotNil(t, child)
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("nested-key-%05d", i))
			expected := bytes.Repeat([]byte("nested"), 20)
			value := child.Get(key)
			require.Equal(t, expected, value)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompression_DeleteAndRewrite verifies that deleting keys and
// rewriting them works correctly with compression.
func TestCompression_DeleteAndRewrite(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{Compression: true})

	// Write initial data.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("deltest"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := bytes.Repeat([]byte("original"), 20)
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Delete even-numbered keys.
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("deltest"))
		for i := 0; i < 100; i += 2 {
			key := []byte(fmt.Sprintf("key-%05d", i))
			if err := b.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Verify only odd-numbered keys remain.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("deltest"))
		require.NotNil(t, b)
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := b.Get(key)
			if i%2 == 0 {
				require.Nil(t, value, "expected deleted key %s to be nil", key)
			} else {
				require.Equal(t, bytes.Repeat([]byte("original"), 20), value)
			}
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompression_LargeValues verifies that pages with overflow work
// correctly with compression.
func TestCompression_LargeValues(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{Compression: true})

	// Write large values that span multiple pages.
	largeValue := bytes.Repeat([]byte("LARGE"), 2000) // 10KB of repeating data

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("large"))
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(i))
			if err := b.Put(key, largeValue); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Verify.
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("large"))
		require.NotNil(t, b)
		for i := 0; i < 10; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(i))
			value := b.Get(key)
			require.Equal(t, largeValue, value)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompression_Check verifies that db.Check() passes with
// compressed pages.
func TestCompression_Check(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{Compression: true})

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("check"))
		if err != nil {
			return err
		}
		for i := 0; i < 200; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := bytes.Repeat([]byte(fmt.Sprintf("v%05d", i)), 10)
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Run Check.
	err = db.View(func(tx *bolt.Tx) error {
		for err := range tx.Check(bolt.WithKVStringer(bolt.HexKVStringer())) {
			return err
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompression_ReopenDB verifies that a compressed DB can be closed
// and reopened, and all data can still be read.
func TestCompression_ReopenDB(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/reopen.db"

	// Create and populate.
	db, err := bolt.Open(path, 0600, &bolt.Options{Compression: true})
	require.NoError(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("reopen"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := bytes.Repeat([]byte("reopen-data"), 15)
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Reopen.
	db, err = bolt.Open(path, 0600, &bolt.Options{Compression: true})
	require.NoError(t, err)
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("reopen"))
		require.NotNil(t, b)
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			expected := bytes.Repeat([]byte("reopen-data"), 15)
			value := b.Get(key)
			require.Equal(t, expected, value)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestCompression_ConcurrentReads verifies that multiple goroutines can
// concurrently read from a compressed database using the same read-only
// transaction without triggering a data race.
func TestCompression_ConcurrentReads(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{Compression: true})

	const numKeys = 500

	// Populate the database with enough data to span many pages.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("concurrent"))
		if err != nil {
			return err
		}
		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value := bytes.Repeat([]byte(fmt.Sprintf("val-%05d-", i)), 10)
			if err := b.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Open a single read-only transaction and share it across goroutines,
	// which is the pattern etcd uses.
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback()) }()

	b := tx.Bucket([]byte("concurrent"))
	require.NotNil(t, b)

	const numGoroutines = 16
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			// Each goroutine creates its own cursor and reads all keys.
			c := b.Cursor()
			count := 0
			for k, v := c.First(); k != nil; k, v = c.Next() {
				expected := []byte(fmt.Sprintf("key-%05d", count))
				if !bytes.Equal(k, expected) {
					t.Errorf("key mismatch: got %q, want %q", k, expected)
					return
				}
				if len(v) == 0 {
					t.Errorf("empty value for key %q", k)
					return
				}
				count++
			}
			if count != numKeys {
				t.Errorf("expected %d keys, got %d", numKeys, count)
			}
		}()
	}

	wg.Wait()
}

// TestCompression_ReducesAllocatedPages verifies that compression actually
// reduces the number of pages allocated compared to an uncompressed database
// with the same data. We compare tx.Size() which reflects the logical
// database size (highest page ID * page size), not the mmap/file size.
func TestCompression_ReducesAllocatedPages(t *testing.T) {
	dir := t.TempDir()
	pathCompressed := dir + "/compressed.db"
	pathUncompressed := dir + "/uncompressed.db"

	const numKeys = 1000
	// Use highly compressible values (repeating patterns).
	writeData := func(db *bolt.DB) {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucket([]byte("data"))
			if err != nil {
				return err
			}
			for i := 0; i < numKeys; i++ {
				key := []byte(fmt.Sprintf("key-%05d", i))
				value := bytes.Repeat([]byte(fmt.Sprintf("value-%05d-", i)), 50)
				if err := b.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)
	}

	readTxSize := func(db *bolt.DB) int64 {
		var size int64
		err := db.View(func(tx *bolt.Tx) error {
			size = tx.Size()
			return nil
		})
		require.NoError(t, err)
		return size
	}

	// Create compressed DB.
	dbC, err := bolt.Open(pathCompressed, 0600, &bolt.Options{Compression: true})
	require.NoError(t, err)
	writeData(dbC)
	sizeC := readTxSize(dbC)
	require.NoError(t, dbC.Close())

	// Create uncompressed DB.
	dbU, err := bolt.Open(pathUncompressed, 0600, nil)
	require.NoError(t, err)
	writeData(dbU)
	sizeU := readTxSize(dbU)
	require.NoError(t, dbU.Close())

	t.Logf("Uncompressed logical size: %d bytes (%d pages)", sizeU, sizeU/4096)
	t.Logf("Compressed logical size:   %d bytes (%d pages)", sizeC, sizeC/4096)
	t.Logf("Ratio: %.1f%%", float64(sizeC)/float64(sizeU)*100)

	require.Less(t, sizeC, sizeU,
		"compressed DB logical size (%d) should be smaller than uncompressed (%d)",
		sizeC, sizeU)
}

package bbolt_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

// TestCursor_RepeatOperations verifies that a cursor can continue to
// iterate over all elements in reverse direction when it has already
// reached to the end or beginning.
// Refer to https://github.com/etcd-io/bbolt/issues/733
func TestCursor_RepeatOperations(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t2 *testing.T, bucket *bolt.Bucket)
	}{
		{
			name:     "Repeat NextPrevNext",
			testFunc: testRepeatCursorOperations_NextPrevNext,
		},
		{
			name:     "Repeat PrevNextPrev",
			testFunc: testRepeatCursorOperations_PrevNextPrev,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})

			bucketName := []byte("data")

			_ = db.Update(func(tx *bolt.Tx) error {
				b, _ := tx.CreateBucketIfNotExists(bucketName)
				testCursorRepeatOperations_PrepareData(t, b)
				return nil
			})

			_ = db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket(bucketName)
				tc.testFunc(t, b)
				return nil
			})
		})
	}
}

func testCursorRepeatOperations_PrepareData(t *testing.T, b *bolt.Bucket) {
	// ensure we have at least one branch page.
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("%05d", i))
		err := b.Put(k, k)
		require.NoError(t, err)
	}
}

func testRepeatCursorOperations_NextPrevNext(t *testing.T, b *bolt.Bucket) {
	c := b.Cursor()
	c.First()
	startKey := []byte(fmt.Sprintf("%05d", 2))
	returnedKey, _ := c.Seek(startKey)
	require.Equal(t, startKey, returnedKey)

	// Step 1: verify next
	for i := 3; i < 1000; i++ {
		expectedKey := []byte(fmt.Sprintf("%05d", i))
		actualKey, _ := c.Next()
		require.Equal(t, expectedKey, actualKey)
	}

	// Once we've reached the end, it should always return nil no matter how many times we call `Next`.
	for i := 0; i < 10; i++ {
		k, _ := c.Next()
		require.Equal(t, []byte(nil), k)
	}

	// Step 2: verify prev
	for i := 998; i >= 0; i-- {
		expectedKey := []byte(fmt.Sprintf("%05d", i))
		actualKey, _ := c.Prev()
		require.Equal(t, expectedKey, actualKey)
	}

	// Once we've reached the beginning, it should always return nil no matter how many times we call `Prev`.
	for i := 0; i < 10; i++ {
		k, _ := c.Prev()
		require.Equal(t, []byte(nil), k)
	}

	// Step 3: verify next again
	for i := 1; i < 1000; i++ {
		expectedKey := []byte(fmt.Sprintf("%05d", i))
		actualKey, _ := c.Next()
		require.Equal(t, expectedKey, actualKey)
	}
}

func testRepeatCursorOperations_PrevNextPrev(t *testing.T, b *bolt.Bucket) {
	c := b.Cursor()

	startKey := []byte(fmt.Sprintf("%05d", 998))
	returnedKey, _ := c.Seek(startKey)
	require.Equal(t, startKey, returnedKey)

	// Step 1: verify prev
	for i := 997; i >= 0; i-- {
		expectedKey := []byte(fmt.Sprintf("%05d", i))
		actualKey, _ := c.Prev()
		require.Equal(t, expectedKey, actualKey)
	}

	// Once we've reached the beginning, it should always return nil no matter how many times we call `Prev`.
	for i := 0; i < 10; i++ {
		k, _ := c.Prev()
		require.Equal(t, []byte(nil), k)
	}

	// Step 2: verify next
	for i := 1; i < 1000; i++ {
		expectedKey := []byte(fmt.Sprintf("%05d", i))
		actualKey, _ := c.Next()
		require.Equal(t, expectedKey, actualKey)
	}

	// Once we've reached the end, it should always return nil no matter how many times we call `Next`.
	for i := 0; i < 10; i++ {
		k, _ := c.Next()
		require.Equal(t, []byte(nil), k)
	}

	// Step 3: verify prev again
	for i := 998; i >= 0; i-- {
		expectedKey := []byte(fmt.Sprintf("%05d", i))
		actualKey, _ := c.Prev()
		require.Equal(t, expectedKey, actualKey)
	}
}

// Ensure that a cursor can return a reference to the bucket that created it.
func TestCursor_Bucket(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		cb := b.Cursor().Bucket()
		require.Truef(t, reflect.DeepEqual(cb, b), "cursor bucket mismatch")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a Tx cursor can seek to the appropriate keys.
func TestCursor_Seek(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("0001")))
		require.NoError(t, b.Put([]byte("bar"), []byte("0002")))
		require.NoError(t, b.Put([]byte("baz"), []byte("0003")))

		_, err = b.CreateBucket([]byte("bkt"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("widgets")).Cursor()

		// Exact match should go to the key.
		k, v := c.Seek([]byte("bar"))
		require.Truef(t, bytes.Equal(k, []byte("bar")), "unexpected key: %v", k)
		require.Truef(t, bytes.Equal(v, []byte("0002")), "unexpected value: %v", v)

		// Inexact match should go to the next key.
		k, v = c.Seek([]byte("bas"))
		require.Truef(t, bytes.Equal(k, []byte("baz")), "unexpected key: %v", k)
		require.Truef(t, bytes.Equal(v, []byte("0003")), "unexpected value: %v", v)

		// Low key should go to the first key.
		k, v = c.Seek([]byte(""))
		require.Truef(t, bytes.Equal(k, []byte("bar")), "unexpected key: %v", k)
		require.Truef(t, bytes.Equal(v, []byte("0002")), "unexpected value: %v", v)

		// High key should return no key.
		k, v = c.Seek([]byte("zzz"))
		require.Nilf(t, k, "expected nil key: %v", k)
		require.Nilf(t, v, "expected nil value: %v", v)

		// Buckets should return their key but no value.
		k, v = c.Seek([]byte("bkt"))
		require.Truef(t, bytes.Equal(k, []byte("bkt")), "unexpected key: %v", k)
		require.Nilf(t, v, "expected nil value: %v", v)

		return nil
	})
	require.NoError(t, err)
}

func TestCursor_Delete(t *testing.T) {
	db := btesting.MustCreateDB(t)

	const count = 1000

	// Insert every other key between 0 and $count.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		for i := 0; i < count; i += 1 {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i))
			require.NoError(t, b.Put(k, make([]byte, 100)))
		}
		_, err = b.CreateBucket([]byte("sub"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("widgets")).Cursor()
		bound := make([]byte, 8)
		binary.BigEndian.PutUint64(bound, uint64(count/2))
		for key, _ := c.First(); bytes.Compare(key, bound) < 0; key, _ = c.Next() {
			require.NoError(t, c.Delete())
		}

		c.Seek([]byte("sub"))
		require.Equalf(t, c.Delete(), errors.ErrIncompatibleValue, "unexpected error")

		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		stats := tx.Bucket([]byte("widgets")).Stats()
		require.Equalf(t, count/2+1, stats.KeyN, "unexpected KeyN: %d", stats.KeyN)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a Tx cursor can seek to the appropriate keys when there are a
// large number of keys. This test also checks that seek will always move
// forward to the next key.
//
// Related: https://github.com/boltdb/bolt/pull/187
func TestCursor_Seek_Large(t *testing.T) {
	db := btesting.MustCreateDB(t)

	var count = 10000

	// Insert every other key between 0 and $count.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		for i := 0; i < count; i += 100 {
			for j := i; j < i+100; j += 2 {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(j))
				require.NoError(t, b.Put(k, make([]byte, 100)))
			}
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("widgets")).Cursor()
		for i := 0; i < count; i++ {
			seek := make([]byte, 8)
			binary.BigEndian.PutUint64(seek, uint64(i))

			k, _ := c.Seek(seek)

			// The last seek is beyond the end of the range so
			// it should return nil.
			if i == count-1 {
				require.Nilf(t, k, "expected nil key")
				continue
			}

			// Otherwise we should seek to the exact key or the next key.
			num := binary.BigEndian.Uint64(k)
			if i%2 == 0 {
				require.Equalf(t, num, uint64(i), "unexpected num: %d", num)
			} else {
				require.Equalf(t, num, uint64(i+1), "unexpected num: %d", num)
			}
		}

		return nil
	})
	require.NoError(t, err)
}

// Ensure that a cursor can iterate over an empty bucket without error.
func TestCursor_EmptyBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("widgets")).Cursor()
		k, v := c.First()
		require.Nilf(t, k, "unexpected key: %v", k)
		require.Nilf(t, v, "unexpected value: %v", v)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a Tx cursor can reverse iterate over an empty bucket without error.
func TestCursor_EmptyBucketReverse(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	})
	require.NoError(t, err)
	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("widgets")).Cursor()
		k, v := c.Last()
		require.Nilf(t, k, "unexpected key: %v", k)
		require.Nilf(t, v, "unexpected value: %v", v)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a Tx cursor can iterate over a single root with a couple elements.
func TestCursor_Iterate_Leaf(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("baz"), []byte{}))
		require.NoError(t, b.Put([]byte("foo"), []byte{0}))
		require.NoError(t, b.Put([]byte("bar"), []byte{1}))
		return nil
	})
	require.NoError(t, err)
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	c := tx.Bucket([]byte("widgets")).Cursor()

	k, v := c.First()
	require.Truef(t, bytes.Equal(k, []byte("bar")), "unexpected key: %v", k)
	require.Truef(t, bytes.Equal(v, []byte{1}), "unexpected value: %v", v)

	k, v = c.Next()
	require.Truef(t, bytes.Equal(k, []byte("baz")), "unexpected key: %v", k)
	require.Truef(t, bytes.Equal(v, []byte{}), "unexpected value: %v", v)

	k, v = c.Next()
	require.Truef(t, bytes.Equal(k, []byte("foo")), "unexpected key: %v", k)
	require.Truef(t, bytes.Equal(v, []byte{0}), "unexpected value: %v", v)

	k, v = c.Next()
	require.Nilf(t, k, "expected nil key: %v", k)
	require.Nilf(t, v, "expected nil value: %v", v)

	k, v = c.Next()
	require.Nilf(t, k, "expected nil key: %v", k)
	require.Nilf(t, v, "expected nil value: %v", v)

	require.NoError(t, tx.Rollback())
}

// Ensure that a Tx cursor can iterate in reverse over a single root with a couple elements.
func TestCursor_LeafRootReverse(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("baz"), []byte{}))
		require.NoError(t, b.Put([]byte("foo"), []byte{0}))
		require.NoError(t, b.Put([]byte("bar"), []byte{1}))
		return nil
	})
	require.NoError(t, err)
	tx, err := db.Begin(false)
	require.NoError(t, err)
	c := tx.Bucket([]byte("widgets")).Cursor()

	k, v := c.Last()
	require.Truef(t, bytes.Equal(k, []byte("foo")), "unexpected key: %v", k)
	require.Truef(t, bytes.Equal(v, []byte{0}), "unexpected value: %v", v)

	k, v = c.Prev()
	require.Truef(t, bytes.Equal(k, []byte("baz")), "unexpected key: %v", k)
	require.Truef(t, bytes.Equal(v, []byte{}), "unexpected value: %v", v)

	k, v = c.Prev()
	require.Truef(t, bytes.Equal(k, []byte("bar")), "unexpected key: %v", k)
	require.Truef(t, bytes.Equal(v, []byte{1}), "unexpected value: %v", v)

	k, v = c.Prev()
	require.Nilf(t, k, "expected nil key: %v", k)
	require.Nilf(t, v, "expected nil value: %v", v)

	k, v = c.Prev()
	require.Nilf(t, k, "expected nil key: %v", k)
	require.Nilf(t, v, "expected nil value: %v", v)

	require.NoError(t, tx.Rollback())
}

// Ensure that a Tx cursor can restart from the beginning.
func TestCursor_Restart(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("bar"), []byte{}))
		require.NoError(t, b.Put([]byte("foo"), []byte{}))
		return nil
	})
	require.NoError(t, err)

	tx, err := db.Begin(false)
	require.NoError(t, err)
	c := tx.Bucket([]byte("widgets")).Cursor()

	k, _ := c.First()
	require.Truef(t, bytes.Equal(k, []byte("bar")), "unexpected key: %v", k)
	k, _ = c.Next()
	require.Truef(t, bytes.Equal(k, []byte("foo")), "unexpected key: %v", k)

	k, _ = c.First()
	require.Truef(t, bytes.Equal(k, []byte("bar")), "unexpected key: %v", k)
	k, _ = c.Next()
	require.Truef(t, bytes.Equal(k, []byte("foo")), "unexpected key: %v", k)

	require.NoError(t, tx.Rollback())
}

// Ensure that a cursor can skip over empty pages that have been deleted.
func TestCursor_First_EmptyPages(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Create 1000 keys in the "widgets" bucket.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		for i := 0; i < 1000; i++ {
			require.NoError(t, b.Put(u64tob(uint64(i)), []byte{}))
		}

		return nil
	})
	require.NoError(t, err)

	// Delete half the keys and then try to iterate.
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 600; i++ {
			require.NoError(t, b.Delete(u64tob(uint64(i))))
		}

		c := b.Cursor()
		var n int
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			n++
		}
		require.Equalf(t, 400, n, "unexpected key count: %d", n)

		return nil
	})
	require.NoError(t, err)
}

// Ensure that a cursor can skip over empty pages that have been deleted.
func TestCursor_Last_EmptyPages(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Create 1000 keys in the "widgets" bucket.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		for i := 0; i < 1000; i++ {
			require.NoError(t, b.Put(u64tob(uint64(i)), []byte{}))
		}

		return nil
	})
	require.NoError(t, err)

	// Delete last 800 elements to ensure last page is empty
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 200; i < 1000; i++ {
			require.NoError(t, b.Delete(u64tob(uint64(i))))
		}

		c := b.Cursor()
		var n int
		for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
			n++
		}
		require.Equalf(t, 200, n, "unexpected key count: %d", n)

		return nil
	})
	require.NoError(t, err)
}

// Ensure that a Tx can iterate over all elements in a bucket.
func TestCursor_QuickCheck(t *testing.T) {
	f := func(items testdata) bool {
		db := btesting.MustCreateDB(t)
		defer db.MustClose()

		// Bulk insert all values.
		tx, err := db.Begin(true)
		require.NoError(t, err)
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		for _, item := range items {
			require.NoError(t, b.Put(item.Key, item.Value))
		}
		require.NoError(t, tx.Commit())

		// Sort test data.
		sort.Sort(items)

		// Iterate over all items and check consistency.
		var index = 0
		tx, err = db.Begin(false)
		require.NoError(t, err)

		c := tx.Bucket([]byte("widgets")).Cursor()
		for k, v := c.First(); k != nil && index < len(items); k, v = c.Next() {
			require.Truef(t, bytes.Equal(k, items[index].Key), "unexpected key: %v", k)
			require.Truef(t, bytes.Equal(v, items[index].Value), "unexpected value: %v", v)
			index++
		}
		require.Equalf(t, len(items), index, "unexpected item count: %v, expected %v", len(items), index)

		require.NoError(t, tx.Rollback())

		return true
	}
	assert.NoError(t, quick.Check(f, qconfig()))
}

// Ensure that a transaction can iterate over all elements in a bucket in reverse.
func TestCursor_QuickCheck_Reverse(t *testing.T) {
	f := func(items testdata) bool {
		db := btesting.MustCreateDB(t)
		defer db.MustClose()

		// Bulk insert all values.
		tx, err := db.Begin(true)
		require.NoError(t, err)
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		for _, item := range items {
			require.NoError(t, b.Put(item.Key, item.Value))
		}
		require.NoError(t, tx.Commit())

		// Sort test data.
		sort.Sort(revtestdata(items))

		// Iterate over all items and check consistency.
		var index = 0
		tx, err = db.Begin(false)
		require.NoError(t, err)
		c := tx.Bucket([]byte("widgets")).Cursor()
		for k, v := c.Last(); k != nil && index < len(items); k, v = c.Prev() {
			require.Truef(t, bytes.Equal(k, items[index].Key), "unexpected key: %v", k)
			require.Truef(t, bytes.Equal(v, items[index].Value), "unexpected value: %v", v)
			index++
		}
		require.Equalf(t, len(items), index, "unexpected item count: %v, expected %v", len(items), index)

		require.NoError(t, tx.Rollback())

		return true
	}
	assert.NoError(t, quick.Check(f, qconfig()))
}

// Ensure that a Tx cursor can iterate over subbuckets.
func TestCursor_QuickCheck_BucketsOnly(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		_, err = b.CreateBucket([]byte("foo"))
		require.NoError(t, err)
		_, err = b.CreateBucket([]byte("bar"))
		require.NoError(t, err)
		_, err = b.CreateBucket([]byte("baz"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		var names []string
		c := tx.Bucket([]byte("widgets")).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			names = append(names, string(k))
			require.Nilf(t, v, "unexpected value: %v", v)
		}
		require.Truef(t, reflect.DeepEqual(names, []string{"bar", "baz", "foo"}), "unexpected names: %+v", names)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a Tx cursor can reverse iterate over subbuckets.
func TestCursor_QuickCheck_BucketsOnly_Reverse(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		_, err = b.CreateBucket([]byte("foo"))
		require.NoError(t, err)
		_, err = b.CreateBucket([]byte("bar"))
		require.NoError(t, err)
		_, err = b.CreateBucket([]byte("baz"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		var names []string
		c := tx.Bucket([]byte("widgets")).Cursor()
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			names = append(names, string(k))
			require.Nilf(t, v, "unexpected value: %v", v)
		}
		require.Truef(t, reflect.DeepEqual(names, []string{"foo", "baz", "bar"}), "unexpected names: %+v", names)
		return nil
	})
	require.NoError(t, err)
}

func ExampleCursor() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Start a read-write transaction.
	if err := db.Update(func(tx *bolt.Tx) error {
		// Create a new bucket.
		b, err := tx.CreateBucket([]byte("animals"))
		if err != nil {
			return err
		}

		// Insert data into a bucket.
		if err := b.Put([]byte("dog"), []byte("fun")); err != nil {
			log.Fatal(err)
		}
		if err := b.Put([]byte("cat"), []byte("lame")); err != nil {
			log.Fatal(err)
		}
		if err := b.Put([]byte("liger"), []byte("awesome")); err != nil {
			log.Fatal(err)
		}

		// Create a cursor for iteration.
		c := b.Cursor()

		// Iterate over items in sorted key order. This starts from the
		// first key/value pair and updates the k/v variables to the
		// next key/value on each iteration.
		//
		// The loop finishes at the end of the cursor when a nil key is returned.
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("A %s is %s.\n", k, v)
		}

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// A cat is lame.
	// A dog is fun.
	// A liger is awesome.
}

func ExampleCursor_reverse() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Start a read-write transaction.
	if err := db.Update(func(tx *bolt.Tx) error {
		// Create a new bucket.
		b, err := tx.CreateBucket([]byte("animals"))
		if err != nil {
			return err
		}

		// Insert data into a bucket.
		if err := b.Put([]byte("dog"), []byte("fun")); err != nil {
			log.Fatal(err)
		}
		if err := b.Put([]byte("cat"), []byte("lame")); err != nil {
			log.Fatal(err)
		}
		if err := b.Put([]byte("liger"), []byte("awesome")); err != nil {
			log.Fatal(err)
		}

		// Create a cursor for iteration.
		c := b.Cursor()

		// Iterate over items in reverse sorted key order. This starts
		// from the last key/value pair and updates the k/v variables to
		// the previous key/value on each iteration.
		//
		// The loop finishes at the beginning of the cursor when a nil key
		// is returned.
		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			fmt.Printf("A %s is %s.\n", k, v)
		}

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close the database to release the file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// A liger is awesome.
	// A dog is fun.
	// A cat is lame.
}

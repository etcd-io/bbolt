package bbolt_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

// Ensure that a bucket that gets a non-existent key returns nil.
func TestBucket_Get_NonExistent(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.Nilf(t, b.Get([]byte("foo")), "expected nil value")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can read a value that is not flushed yet.
func TestBucket_Get_FromNode(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		v := b.Get([]byte("foo"))
		require.Truef(t, bytes.Equal(v, []byte("bar")), "unexpected value: %v", v)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket retrieved via Get() returns a nil.
func TestBucket_Get_IncompatibleValue(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		_, err = tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo"))
		require.NoError(t, err)

		require.Nilf(t, tx.Bucket([]byte("widgets")).Get([]byte("foo")), "expected nil value")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a slice returned from a bucket has a capacity equal to its length.
// This also allows slices to be appended to since it will require a realloc by Go.
//
// https://github.com/boltdb/bolt/issues/544
func TestBucket_Get_Capacity(t *testing.T) {
	db := btesting.MustCreateDB(t)

	// Write key to a bucket.
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		return b.Put([]byte("key"), []byte("val"))
	})
	require.NoError(t, err)

	// Retrieve value and attempt to append to it.
	err = db.Update(func(tx *bolt.Tx) error {
		k, v := tx.Bucket([]byte("bucket")).Cursor().First()

		// Verify capacity.
		require.Equalf(t, len(k), cap(k), "unexpected key slice capacity: %d", cap(k))
		require.Equalf(t, len(v), cap(v), "unexpected value slice capacity: %d", cap(v))

		// Ensure slice can be appended to without a segfault.
		k = append(k, []byte("123")...)
		v = append(v, []byte("123")...)
		_, _ = k, v // to pass ineffassign

		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can write a key/value.
func TestBucket_Put(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))

		v := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		require.Truef(t, bytes.Equal([]byte("bar"), v), "unexpected value: %v", v)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can rewrite a key in the same transaction.
func TestBucket_Put_Repeat(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		require.NoError(t, b.Put([]byte("foo"), []byte("baz")))

		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		require.Truef(t, bytes.Equal([]byte("baz"), value), "unexpected value: %v", value)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can write a bunch of large values.
func TestBucket_Put_Large(t *testing.T) {
	db := btesting.MustCreateDB(t)

	count, factor := 100, 200
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		for i := 1; i < count; i++ {
			require.NoError(t, b.Put([]byte(strings.Repeat("0", i*factor)), []byte(strings.Repeat("X", (count-i)*factor))))
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 1; i < count; i++ {
			value := b.Get([]byte(strings.Repeat("0", i*factor)))
			require.Truef(t, bytes.Equal(value, []byte(strings.Repeat("X", (count-i)*factor))), "unexpected value: %v", value)
		}
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a database can perform multiple large appends safely.
func TestDB_Put_VeryLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	n, batchN := 400000, 200000
	ksize, vsize := 8, 500

	db := btesting.MustCreateDB(t)

	for i := 0; i < n; i += batchN {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("widgets"))
			require.NoError(t, err)
			for j := 0; j < batchN; j++ {
				k, v := make([]byte, ksize), make([]byte, vsize)
				binary.BigEndian.PutUint32(k, uint32(i+j))
				require.NoError(t, b.Put(k, v))
			}
			return nil
		})
		require.NoError(t, err)
	}
}

// Ensure that a setting a value on a key with a bucket value returns an error.
func TestBucket_Put_IncompatibleValue(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b0, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		_, err = tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo"))
		require.NoError(t, err)
		require.Equalf(t, b0.Put([]byte("foo"), []byte("bar")), berrors.ErrIncompatibleValue, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a setting a value while the transaction is closed returns an error.
func TestBucket_Put_Closed(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(true)
	require.NoError(t, err)

	b, err := tx.CreateBucket([]byte("widgets"))
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())

	require.Equalf(t, b.Put([]byte("foo"), []byte("bar")), berrors.ErrTxClosed, "unexpected error")
}

// Ensure that setting a value on a read-only bucket returns an error.
func TestBucket_Put_ReadOnly(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		require.Equalf(t, b.Put([]byte("foo"), []byte("bar")), berrors.ErrTxNotWritable, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can delete an existing key.
func TestBucket_Delete(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))
		require.NoError(t, b.Delete([]byte("foo")))
		require.Nilf(t, b.Get([]byte("foo")), "expected nil value")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that deleting a large set of keys will work correctly.
func TestBucket_Delete_Large(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			require.NoError(t, b.Put([]byte(strconv.Itoa(i)), []byte(strings.Repeat("*", 1024))))
		}

		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 100; i++ {
			require.NoError(t, b.Delete([]byte(strconv.Itoa(i))))
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 100; i++ {
			v := b.Get([]byte(strconv.Itoa(i)))
			require.Nilf(t, v, "unexpected value: %v, i=%d", v, i)
		}
		return nil
	})
	require.NoError(t, err)
}

// Deleting a very large list of keys will cause the freelist to use overflow.
func TestBucket_Delete_FreelistOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	db := btesting.MustCreateDB(t)

	k := make([]byte, 16)
	// The bigger the pages - the more values we need to write.
	for i := uint64(0); i < 2*uint64(db.Info().PageSize); i++ {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("0"))
			require.NoErrorf(t, err, "bucket error: %s", err)

			for j := uint64(0); j < 1000; j++ {
				binary.BigEndian.PutUint64(k[:8], i)
				binary.BigEndian.PutUint64(k[8:], j)
				require.NoErrorf(t, b.Put(k, nil), "put error")
			}

			return nil
		})
		require.NoError(t, err)
	}

	// Delete all of them in one large transaction
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("0"))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			require.NoError(t, c.Delete())
		}
		return nil
	})
	require.NoError(t, err)

	// Check more than an overflow's worth of pages are freed.
	stats := db.Stats()
	freePages := stats.FreePageN + stats.PendingPageN
	require.Greaterf(t, freePages, 0xFFFF, "expected more than 0xFFFF free pages, got %v", freePages)

	// Free page count should be preserved on reopen.
	db.MustClose()
	db.MustReopen()
	reopenFreePages := db.Stats().FreePageN
	require.Equalf(t, freePages, reopenFreePages, "expected %d free pages, got %+v", freePages, db.Stats())
	reopenPendingPages := db.Stats().PendingPageN
	require.Equalf(t, 0, reopenPendingPages, "expected no pending pages, got %+v", db.Stats())
}

// Ensure that deleting of non-existing key is a no-op.
func TestBucket_Delete_NonExisting(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		_, err = b.CreateBucket([]byte("nested"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		require.NoError(t, b.Delete([]byte("foo")))
		require.NotNilf(t, b.Bucket([]byte("nested")), "nested bucket has been deleted")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that accessing and updating nested buckets is ok across transactions.
func TestBucket_Nested(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		// Create a widgets bucket.
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		// Create a widgets/foo bucket.
		_, err = b.CreateBucket([]byte("foo"))
		require.NoError(t, err)

		// Create a widgets/bar key.
		require.NoError(t, b.Put([]byte("bar"), []byte("0000")))

		return nil
	})
	require.NoError(t, err)
	db.MustCheck()

	// Update widgets/bar.
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		require.NoError(t, b.Put([]byte("bar"), []byte("xxxx")))
		return nil
	})
	require.NoError(t, err)
	db.MustCheck()

	// Cause a split.
	err = db.Update(func(tx *bolt.Tx) error {
		var b = tx.Bucket([]byte("widgets"))
		for i := 0; i < 10000; i++ {
			require.NoError(t, b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
		}
		return nil
	})
	require.NoError(t, err)
	db.MustCheck()

	// Insert into widgets/foo/baz.
	err = db.Update(func(tx *bolt.Tx) error {
		var b = tx.Bucket([]byte("widgets"))
		require.NoError(t, b.Bucket([]byte("foo")).Put([]byte("baz"), []byte("yyyy")))
		return nil
	})
	require.NoError(t, err)
	db.MustCheck()

	// Verify.
	err = db.View(func(tx *bolt.Tx) error {
		var b = tx.Bucket([]byte("widgets"))
		v := b.Bucket([]byte("foo")).Get([]byte("baz"))
		require.Truef(t, bytes.Equal(v, []byte("yyyy")), "unexpected value: %v", v)
		v = b.Get([]byte("bar"))
		require.Truef(t, bytes.Equal(v, []byte("xxxx")), "unexpected value: %v", v)
		for i := 0; i < 10000; i++ {
			v := b.Get([]byte(strconv.Itoa(i)))
			require.Truef(t, bytes.Equal(v, []byte(strconv.Itoa(i))), "unexpected value: %v", v)
		}
		return nil
	})
	require.NoError(t, err)
}

// Ensure that deleting a bucket using Delete() returns an error.
func TestBucket_Delete_Bucket(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		_, err = b.CreateBucket([]byte("foo"))
		require.NoError(t, err)
		require.Equalf(t, b.Delete([]byte("foo")), berrors.ErrIncompatibleValue, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that deleting a key on a read-only bucket returns an error.
func TestBucket_Delete_ReadOnly(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		require.Equalf(t, tx.Bucket([]byte("widgets")).Delete([]byte("foo")), berrors.ErrTxNotWritable, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a deleting value while the transaction is closed returns an error.
func TestBucket_Delete_Closed(t *testing.T) {
	db := btesting.MustCreateDB(t)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	b, err := tx.CreateBucket([]byte("widgets"))
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())
	require.Equalf(t, b.Delete([]byte("foo")), berrors.ErrTxClosed, "unexpected error")
}

// Ensure that deleting a bucket causes nested buckets to be deleted.
func TestBucket_DeleteBucket_Nested(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		foo, err := widgets.CreateBucket([]byte("foo"))
		require.NoError(t, err)

		bar, err := foo.CreateBucket([]byte("bar"))
		require.NoError(t, err)
		require.NoError(t, bar.Put([]byte("baz"), []byte("bat")))
		require.NoError(t, tx.Bucket([]byte("widgets")).DeleteBucket([]byte("foo")))
		return nil
	})
	require.NoError(t, err)
}

// Ensure that deleting a bucket causes nested buckets to be deleted after they have been committed.
func TestBucket_DeleteBucket_Nested2(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		foo, err := widgets.CreateBucket([]byte("foo"))
		require.NoError(t, err)

		bar, err := foo.CreateBucket([]byte("bar"))
		require.NoError(t, err)

		require.NoError(t, bar.Put([]byte("baz"), []byte("bat")))
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		widgets := tx.Bucket([]byte("widgets"))
		require.NotNilf(t, widgets, "expected widgets bucket")

		foo := widgets.Bucket([]byte("foo"))
		require.NotNilf(t, foo, "expected foo bucket")

		bar := foo.Bucket([]byte("bar"))
		require.NotNilf(t, bar, "expected bar bucket")

		v := bar.Get([]byte("baz"))
		require.Truef(t, bytes.Equal(v, []byte("bat")), "unexpected value: %v", v)
		require.NoError(t, tx.DeleteBucket([]byte("widgets")))
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		require.Nilf(t, tx.Bucket([]byte("widgets")), "expected bucket to be deleted")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that deleting a child bucket with multiple pages causes all pages to get collected.
// NOTE: Consistency check in bolt_test.DB.Close() will panic if pages not freed properly.
func TestBucket_DeleteBucket_Large(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		foo, err := widgets.CreateBucket([]byte("foo"))
		require.NoError(t, err)

		for i := 0; i < 1000; i++ {
			require.NoError(t, foo.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%0100d", i))))
		}
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		require.NoError(t, tx.DeleteBucket([]byte("widgets")))
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a simple value retrieved via Bucket() returns a nil.
func TestBucket_Bucket_IncompatibleValue(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		require.NoError(t, widgets.Put([]byte("foo"), []byte("bar")))
		require.Nilf(t, tx.Bucket([]byte("widgets")).Bucket([]byte("foo")), "expected nil bucket")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that creating a bucket on an existing non-bucket key returns an error.
func TestBucket_CreateBucket_IncompatibleValue(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)

		require.NoError(t, widgets.Put([]byte("foo"), []byte("bar")))
		_, err = widgets.CreateBucket([]byte("foo"))
		require.Equalf(t, err, berrors.ErrIncompatibleValue, "unexpected error: %s", err)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that deleting a bucket on an existing non-bucket key returns an error.
func TestBucket_DeleteBucket_IncompatibleValue(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, widgets.Put([]byte("foo"), []byte("bar")))
		require.Equalf(t, tx.Bucket([]byte("widgets")).DeleteBucket([]byte("foo")), berrors.ErrIncompatibleValue, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure bucket can set and update its sequence number.
func TestBucket_Sequence(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucket([]byte("0"))
		require.NoError(t, err)

		// Retrieve sequence.
		require.Equalf(t, uint64(0), bkt.Sequence(), "unexpected sequence")

		// Update sequence.
		err = bkt.SetSequence(1000)
		require.NoError(t, err)

		// Read sequence again.
		require.Equalf(t, uint64(1000), bkt.Sequence(), "unexpected sequence")

		return nil
	})
	require.NoError(t, err)

	// Verify sequence in separate transaction.
	err = db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("0")).Sequence()
		require.Equalf(t, uint64(1000), v, "unexpected sequence: %d", v)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can return an autoincrementing sequence.
func TestBucket_NextSequence(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		woojits, err := tx.CreateBucket([]byte("woojits"))
		require.NoError(t, err)

		// Make sure sequence increments.
		seq, err := widgets.NextSequence()
		require.NoError(t, err)
		require.Equalf(t, uint64(1), seq, "unexpected sequence: %d", seq)

		seq, err = widgets.NextSequence()
		require.NoError(t, err)
		require.Equalf(t, uint64(2), seq, "unexpected sequence: %d", seq)

		// Buckets should be separate.
		seq, err = woojits.NextSequence()
		require.NoError(t, err)
		require.Equalf(t, uint64(1), seq, "unexpected sequence: %d", seq)

		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket will persist an autoincrementing sequence even if its
// the only thing updated on the bucket.
// https://github.com/boltdb/bolt/issues/296
func TestBucket_NextSequence_Persist(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.Bucket([]byte("widgets")).NextSequence()
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		seq, err := tx.Bucket([]byte("widgets")).NextSequence()
		require.NoErrorf(t, err, "unexpected error")
		require.Equalf(t, uint64(2), seq, "unexpected sequence: %d", seq)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that retrieving the next sequence on a read-only bucket returns an error.
func TestBucket_NextSequence_ReadOnly(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *bolt.Tx) error {
		_, err := tx.Bucket([]byte("widgets")).NextSequence()
		require.Equalf(t, err, berrors.ErrTxNotWritable, "unexpected error: %s", err)
		return nil
	})
	require.NoError(t, err)
}

// Ensure that retrieving the next sequence for a bucket on a closed database return an error.
func TestBucket_NextSequence_Closed(t *testing.T) {
	db := btesting.MustCreateDB(t)
	tx, err := db.Begin(true)
	require.NoError(t, err)
	b, err := tx.CreateBucket([]byte("widgets"))
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	_, err = b.NextSequence()
	require.Equal(t, err, berrors.ErrTxClosed)
}

// Ensure a user can loop over all key/value pairs in a bucket.
func TestBucket_ForEach(t *testing.T) {
	db := btesting.MustCreateDB(t)

	type kv struct {
		k []byte
		v []byte
	}

	expectedItems := []kv{
		{k: []byte("bar"), v: []byte("0002")},
		{k: []byte("baz"), v: []byte("0001")},
		{k: []byte("csubbucket"), v: nil},
		{k: []byte("foo"), v: []byte("0000")},
	}

	verifyReads := func(b *bolt.Bucket) {
		var items []kv
		err := b.ForEach(func(k, v []byte) error {
			items = append(items, kv{k: k, v: v})
			return nil
		})
		require.NoErrorf(t, err, "b.ForEach failed")
		assert.Equalf(t, expectedItems, items, "what we iterated (ForEach) is not what we put")
	}

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoErrorf(t, err, "bucket creation failed")

		require.NoErrorf(t, b.Put([]byte("foo"), []byte("0000")), "put 'foo' failed")
		require.NoErrorf(t, b.Put([]byte("baz"), []byte("0001")), "put 'baz' failed")
		require.NoErrorf(t, b.Put([]byte("bar"), []byte("0002")), "put 'bar' failed")
		_, err = b.CreateBucket([]byte("csubbucket"))
		require.NoErrorf(t, err, "creation of subbucket failed")

		verifyReads(b)

		return nil
	})
	require.NoErrorf(t, err, "db.Update failed")
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		require.NotNilf(t, b, "bucket opening failed")
		verifyReads(b)
		return nil
	})
	assert.NoErrorf(t, err, "db.View failed")
}

func TestBucket_ForEachBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)

	expectedItems := [][]byte{
		[]byte("csubbucket"),
		[]byte("zsubbucket"),
	}

	verifyReads := func(b *bolt.Bucket) {
		var items [][]byte
		err := b.ForEachBucket(func(k []byte) error {
			items = append(items, k)
			return nil
		})
		require.NoErrorf(t, err, "b.ForEach failed")
		assert.Equalf(t, expectedItems, items, "what we iterated (ForEach) is not what we put")
	}

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoErrorf(t, err, "bucket creation failed")

		require.NoErrorf(t, b.Put([]byte("foo"), []byte("0000")), "put 'foo' failed")
		_, err = b.CreateBucket([]byte("zsubbucket"))
		require.NoErrorf(t, err, "creation of subbucket failed")
		require.NoErrorf(t, b.Put([]byte("baz"), []byte("0001")), "put 'baz' failed")
		require.NoErrorf(t, b.Put([]byte("bar"), []byte("0002")), "put 'bar' failed")
		_, err = b.CreateBucket([]byte("csubbucket"))
		require.NoErrorf(t, err, "creation of subbucket failed")

		verifyReads(b)

		return nil
	})
	require.NoErrorf(t, err, "db.Update failed")
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		require.NotNilf(t, b, "bucket opening failed")
		verifyReads(b)
		return nil
	})
	assert.NoErrorf(t, err, "db.View failed")
}

func TestBucket_ForEachBucket_NoBuckets(t *testing.T) {
	db := btesting.MustCreateDB(t)

	verifyReads := func(b *bolt.Bucket) {
		var items [][]byte
		err := b.ForEachBucket(func(k []byte) error {
			items = append(items, k)
			return nil
		})
		require.NoErrorf(t, err, "b.ForEach failed")
		assert.Emptyf(t, items, "what we iterated (ForEach) is not what we put")
	}

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoErrorf(t, err, "bucket creation failed")

		require.NoErrorf(t, b.Put([]byte("foo"), []byte("0000")), "put 'foo' failed")
		require.NoErrorf(t, err, "creation of subbucket failed")
		require.NoErrorf(t, b.Put([]byte("baz"), []byte("0001")), "put 'baz' failed")
		require.NoErrorf(t, err, "creation of subbucket failed")

		verifyReads(b)

		return nil
	})
	require.NoErrorf(t, err, "db.Update failed")

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		require.NotNilf(t, b, "bucket opening failed")
		verifyReads(b)
		return nil
	})
	assert.NoErrorf(t, err, "db.View failed")
}

// Ensure a database can stop iteration early.
func TestBucket_ForEach_ShortCircuit(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("bar"), []byte("0000")))
		require.NoError(t, b.Put([]byte("baz"), []byte("0000")))
		require.NoError(t, b.Put([]byte("foo"), []byte("0000")))

		var index int
		err = tx.Bucket([]byte("widgets")).ForEach(func(k, v []byte) error {
			index++
			if bytes.Equal(k, []byte("baz")) {
				return errors.New("marker")
			}
			return nil
		})
		require.EqualErrorf(t, err, "marker", "unexpected error: %s", err)
		require.Equalf(t, 2, index, "unexpected index: %d", index)

		return nil
	})
	require.NoError(t, err)
}

// Ensure that looping over a bucket on a closed database returns an error.
func TestBucket_ForEach_Closed(t *testing.T) {
	db := btesting.MustCreateDB(t)

	tx, err := db.Begin(true)
	require.NoError(t, err)

	b, err := tx.CreateBucket([]byte("widgets"))
	require.NoError(t, err)

	require.NoError(t, tx.Rollback())

	require.Equalf(t, b.ForEach(func(k, v []byte) error { return nil }), berrors.ErrTxClosed, "unexpected error")
}

// Ensure that an error is returned when inserting with an empty key.
func TestBucket_Put_EmptyKey(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.Equalf(t, b.Put([]byte(""), []byte("bar")), berrors.ErrKeyRequired, "unexpected error")
		require.Equalf(t, b.Put(nil, []byte("bar")), berrors.ErrKeyRequired, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that an error is returned when inserting with a key that's too large.
func TestBucket_Put_KeyTooLarge(t *testing.T) {
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.Equalf(t, b.Put(make([]byte, 32769), []byte("bar")), berrors.ErrKeyTooLarge, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure that an error is returned when inserting a value that's too large.
func TestBucket_Put_ValueTooLarge(t *testing.T) {
	// Skip this test on DroneCI because the machine is resource constrained.
	if os.Getenv("DRONE") == "true" {
		t.Skip("not enough RAM for test")
	}

	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		require.NoError(t, err)
		require.Equalf(t, b.Put([]byte("foo"), make([]byte, bolt.MaxValueSize+1)), berrors.ErrValueTooLarge, "unexpected error")
		return nil
	})
	require.NoError(t, err)
}

// Ensure a bucket can calculate stats.
func TestBucket_Stats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	db := btesting.MustCreateDB(t)

	// Add bucket with fewer keys but one big value.
	bigKey := []byte("really-big-value")
	for i := 0; i < 500; i++ {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("woojits"))
			require.NoError(t, err)

			require.NoError(t, b.Put([]byte(fmt.Sprintf("%03d", i)), []byte(strconv.Itoa(i))))
			return nil
		})
		require.NoError(t, err)
	}
	longKeyLength := 10*db.Info().PageSize + 17
	err := db.Update(func(tx *bolt.Tx) error {
		require.NoError(t, tx.Bucket([]byte("woojits")).Put(bigKey, []byte(strings.Repeat("*", longKeyLength))))
		return nil
	})
	require.NoError(t, err)

	db.MustCheck()

	pageSize2stats := map[int]bolt.BucketStats{
		4096: {
			BranchPageN:     1,
			BranchOverflowN: 0,
			LeafPageN:       7,
			LeafOverflowN:   10,
			KeyN:            501,
			Depth:           2,
			BranchAlloc:     4096,
			BranchInuse:     149,
			LeafAlloc:       69632,
			LeafInuse: 0 +
				7*16 + // leaf page header (x LeafPageN)
				501*16 + // leaf elements
				500*3 + len(bigKey) + // leaf keys
				1*10 + 2*90 + 3*400 + longKeyLength, // leaf values: 10 * 1digit, 90*2digits, ...
			BucketN:           1,
			InlineBucketN:     0,
			InlineBucketInuse: 0},
		16384: {
			BranchPageN:     1,
			BranchOverflowN: 0,
			LeafPageN:       3,
			LeafOverflowN:   10,
			KeyN:            501,
			Depth:           2,
			BranchAlloc:     16384,
			BranchInuse:     73,
			LeafAlloc:       212992,
			LeafInuse: 0 +
				3*16 + // leaf page header (x LeafPageN)
				501*16 + // leaf elements
				500*3 + len(bigKey) + // leaf keys
				1*10 + 2*90 + 3*400 + longKeyLength, // leaf values: 10 * 1digit, 90*2digits, ...
			BucketN:           1,
			InlineBucketN:     0,
			InlineBucketInuse: 0},
		65536: {
			BranchPageN:     1,
			BranchOverflowN: 0,
			LeafPageN:       2,
			LeafOverflowN:   10,
			KeyN:            501,
			Depth:           2,
			BranchAlloc:     65536,
			BranchInuse:     54,
			LeafAlloc:       786432,
			LeafInuse: 0 +
				2*16 + // leaf page header (x LeafPageN)
				501*16 + // leaf elements
				500*3 + len(bigKey) + // leaf keys
				1*10 + 2*90 + 3*400 + longKeyLength, // leaf values: 10 * 1digit, 90*2digits, ...
			BucketN:           1,
			InlineBucketN:     0,
			InlineBucketInuse: 0},
	}

	err = db.View(func(tx *bolt.Tx) error {
		stats := tx.Bucket([]byte("woojits")).Stats()
		t.Logf("Stats: %#v", stats)
		if expected, ok := pageSize2stats[db.Info().PageSize]; ok {
			assert.Equalf(t, expected, stats, "stats differs from expectations")
		} else {
			t.Skipf("No expectations for page size: %d", db.Info().PageSize)
		}
		return nil
	})
	require.NoError(t, err)
}

// Ensure a bucket with random insertion utilizes fill percentage correctly.
func TestBucket_Stats_RandomFill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	} else if os.Getpagesize() != 4096 {
		t.Skip("invalid page size for test")
	}

	db := btesting.MustCreateDB(t)

	// Add a set of values in random order. It will be the same random
	// order so we can maintain consistency between test runs.
	var count int
	rand := rand.New(rand.NewSource(42))
	for _, i := range rand.Perm(1000) {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("woojits"))
			require.NoError(t, err)
			b.FillPercent = 0.9
			for _, j := range rand.Perm(100) {
				index := (j * 10000) + i
				require.NoError(t, b.Put([]byte(fmt.Sprintf("%d000000000000000", index)), []byte("0000000000")))
				count++
			}
			return nil
		})
		require.NoError(t, err)
	}

	db.MustCheck()

	err := db.View(func(tx *bolt.Tx) error {
		stats := tx.Bucket([]byte("woojits")).Stats()
		require.Equalf(t, 100000, stats.KeyN, "unexpected KeyN: %d", stats.KeyN)

		require.Equalf(t, 98, stats.BranchPageN, "unexpected BranchPageN: %d", stats.BranchPageN)
		require.Equalf(t, 0, stats.BranchOverflowN, "unexpected BranchOverflowN: %d", stats.BranchOverflowN)
		require.Equalf(t, 130984, stats.BranchInuse, "unexpected BranchInuse: %d", stats.BranchInuse)
		require.Equalf(t, 401408, stats.BranchAlloc, "unexpected BranchAlloc: %d", stats.BranchAlloc)

		require.Equalf(t, 3412, stats.LeafPageN, "unexpected LeafPageN: %d", stats.LeafPageN)
		require.Equalf(t, 0, stats.LeafOverflowN, "unexpected LeafOverflowN: %d", stats.LeafOverflowN)
		require.Equalf(t, 4742482, stats.LeafInuse, "unexpected LeafInuse: %d", stats.LeafInuse)
		require.Equalf(t, 13975552, stats.LeafAlloc, "unexpected LeafAlloc: %d", stats.LeafAlloc)
		return nil
	})
	require.NoError(t, err)
}

// Ensure a bucket can calculate stats.
func TestBucket_Stats_Small(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		// Add a bucket that fits on a single root leaf.
		b, err := tx.CreateBucket([]byte("whozawhats"))
		require.NoError(t, err)
		require.NoError(t, b.Put([]byte("foo"), []byte("bar")))

		return nil
	})
	require.NoError(t, err)

	db.MustCheck()

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("whozawhats"))
		stats := b.Stats()
		require.Equalf(t, 0, stats.BranchPageN, "unexpected BranchPageN: %d", stats.BranchPageN)
		require.Equalf(t, 0, stats.BranchOverflowN, "unexpected BranchOverflowN: %d", stats.BranchOverflowN)
		require.Equalf(t, 0, stats.LeafPageN, "unexpected LeafPageN: %d", stats.LeafPageN)
		require.Equalf(t, 0, stats.LeafOverflowN, "unexpected LeafOverflowN: %d", stats.LeafOverflowN)
		require.Equalf(t, 1, stats.KeyN, "unexpected KeyN: %d", stats.KeyN)
		require.Equalf(t, 1, stats.Depth, "unexpected Depth: %d", stats.Depth)
		require.Equalf(t, 0, stats.BranchInuse, "unexpected BranchInuse: %d", stats.BranchInuse)
		require.Equalf(t, 0, stats.LeafInuse, "unexpected LeafInuse: %d", stats.LeafInuse)

		if db.Info().PageSize == 4096 {
			require.Equalf(t, 0, stats.BranchAlloc, "unexpected BranchAlloc: %d", stats.BranchAlloc)
			require.Equalf(t, 0, stats.LeafAlloc, "unexpected LeafAlloc: %d", stats.LeafAlloc)
		}

		require.Equalf(t, 1, stats.BucketN, "unexpected BucketN: %d", stats.BucketN)
		require.Equalf(t, 1, stats.InlineBucketN, "unexpected InlineBucketN: %d", stats.InlineBucketN)
		require.Equalf(t, 16+16+6, stats.InlineBucketInuse, "unexpected InlineBucketInuse: %d", stats.InlineBucketInuse)

		return nil
	})
	require.NoError(t, err)
}

func TestBucket_Stats_EmptyBucket(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		// Add a bucket that fits on a single root leaf.
		_, err := tx.CreateBucket([]byte("whozawhats"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	db.MustCheck()

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("whozawhats"))
		stats := b.Stats()
		require.Equalf(t, 0, stats.BranchPageN, "unexpected BranchPageN: %d", stats.BranchPageN)
		require.Equalf(t, 0, stats.BranchOverflowN, "unexpected BranchOverflowN: %d", stats.BranchOverflowN)
		require.Equalf(t, 0, stats.LeafPageN, "unexpected LeafPageN: %d", stats.LeafPageN)
		require.Equalf(t, 0, stats.LeafOverflowN, "unexpected LeafOverflowN: %d", stats.LeafOverflowN)
		require.Equalf(t, 0, stats.KeyN, "unexpected KeyN: %d", stats.KeyN)
		require.Equalf(t, 1, stats.Depth, "unexpected Depth: %d", stats.Depth)
		require.Equalf(t, 0, stats.BranchInuse, "unexpected BranchInuse: %d", stats.BranchInuse)
		require.Equalf(t, 0, stats.LeafInuse, "unexpected LeafInuse: %d", stats.LeafInuse)

		if db.Info().PageSize == 4096 {
			require.Equalf(t, 0, stats.BranchAlloc, "unexpected BranchAlloc: %d", stats.BranchAlloc)
			require.Equalf(t, 0, stats.LeafAlloc, "unexpected LeafAlloc: %d", stats.LeafAlloc)
		}

		require.Equalf(t, 1, stats.BucketN, "unexpected BucketN: %d", stats.BucketN)
		require.Equalf(t, 1, stats.InlineBucketN, "unexpected InlineBucketN: %d", stats.InlineBucketN)
		require.Equalf(t, 16, stats.InlineBucketInuse, "unexpected InlineBucketInuse: %d", stats.InlineBucketInuse)

		return nil
	})
	require.NoError(t, err)
}

// Ensure a bucket can calculate stats.
func TestBucket_Stats_Nested(t *testing.T) {
	db := btesting.MustCreateDB(t)

	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("foo"))
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			require.NoError(t, b.Put([]byte(fmt.Sprintf("%02d", i)), []byte(fmt.Sprintf("%02d", i))))
		}

		bar, err := b.CreateBucket([]byte("bar"))
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			require.NoError(t, bar.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
		}

		baz, err := bar.CreateBucket([]byte("baz"))
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			require.NoError(t, baz.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))))
		}

		return nil
	})
	require.NoError(t, err)

	db.MustCheck()

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("foo"))
		stats := b.Stats()
		require.Equalf(t, 0, stats.BranchPageN, "unexpected BranchPageN: %d", stats.BranchPageN)
		require.Equalf(t, 0, stats.BranchOverflowN, "unexpected BranchOverflowN: %d", stats.BranchOverflowN)
		require.Equalf(t, 2, stats.LeafPageN, "unexpected LeafPageN: %d", stats.LeafPageN)
		require.Equalf(t, 0, stats.LeafOverflowN, "unexpected LeafOverflowN: %d", stats.LeafOverflowN)
		require.Equalf(t, 122, stats.KeyN, "unexpected KeyN: %d", stats.KeyN)
		require.Equalf(t, 3, stats.Depth, "unexpected Depth: %d", stats.Depth)
		require.Equalf(t, 0, stats.BranchInuse, "unexpected BranchInuse: %d", stats.BranchInuse)

		foo := 16            // foo (pghdr)
		foo += 101 * 16      // foo leaf elements
		foo += 100*2 + 100*2 // foo leaf key/values
		foo += 3 + 16        // foo -> bar key/value

		bar := 16      // bar (pghdr)
		bar += 11 * 16 // bar leaf elements
		bar += 10 + 10 // bar leaf key/values
		bar += 3 + 16  // bar -> baz key/value

		baz := 16      // baz (inline) (pghdr)
		baz += 10 * 16 // baz leaf elements
		baz += 10 + 10 // baz leaf key/values

		require.Equalf(t, stats.LeafInuse, foo+bar+baz, "unexpected LeafInuse: %d", stats.LeafInuse)

		if db.Info().PageSize == 4096 {
			require.Equalf(t, 0, stats.BranchAlloc, "unexpected BranchAlloc: %d", stats.BranchAlloc)
			require.Equalf(t, 8192, stats.LeafAlloc, "unexpected LeafAlloc: %d", stats.LeafAlloc)
		}

		require.Equalf(t, 3, stats.BucketN, "unexpected BucketN: %d", stats.BucketN)
		require.Equalf(t, 1, stats.InlineBucketN, "unexpected InlineBucketN: %d", stats.InlineBucketN)
		require.Equalf(t, stats.InlineBucketInuse, baz, "unexpected InlineBucketInuse: %d", stats.InlineBucketInuse)

		return nil
	})
	require.NoError(t, err)
}

func TestBucket_Inspect(t *testing.T) {
	db := btesting.MustCreateDB(t)

	expectedStructure := bolt.BucketStructure{
		Name: "root",
		KeyN: 0,
		Children: []bolt.BucketStructure{
			{
				Name: "b1",
				KeyN: 3,
				Children: []bolt.BucketStructure{
					{
						Name: "b1_1",
						KeyN: 6,
					},
					{
						Name: "b1_2",
						KeyN: 7,
					},
					{
						Name: "b1_3",
						KeyN: 8,
					},
				},
			},
			{
				Name: "b2",
				KeyN: 4,
				Children: []bolt.BucketStructure{
					{
						Name: "b2_1",
						KeyN: 10,
					},
					{
						Name: "b2_2",
						KeyN: 12,
						Children: []bolt.BucketStructure{
							{
								Name: "b2_2_1",
								KeyN: 2,
							},
							{
								Name: "b2_2_2",
								KeyN: 3,
							},
						},
					},
					{
						Name: "b2_3",
						KeyN: 11,
					},
				},
			},
		},
	}

	type bucketItem struct {
		b  *bolt.Bucket
		bs bolt.BucketStructure
	}

	t.Log("Populating the database")
	err := db.Update(func(tx *bolt.Tx) error {
		queue := []bucketItem{
			{
				b:  nil,
				bs: expectedStructure,
			},
		}

		for len(queue) > 0 {
			item := queue[0]
			queue = queue[1:]

			if item.b != nil {
				for i := 0; i < item.bs.KeyN; i++ {
					require.NoError(t, item.b.Put([]byte(fmt.Sprintf("%02d", i)), []byte(fmt.Sprintf("%02d", i))))
				}

				for _, child := range item.bs.Children {
					childBucket, err := item.b.CreateBucket([]byte(child.Name))
					require.NoError(t, err)
					queue = append(queue, bucketItem{b: childBucket, bs: child})
				}
			} else {
				for _, child := range item.bs.Children {
					childBucket, err := tx.CreateBucket([]byte(child.Name))
					require.NoError(t, err)
					queue = append(queue, bucketItem{b: childBucket, bs: child})
				}
			}
		}
		return nil
	})
	require.NoError(t, err)

	t.Log("Inspecting the database")
	_ = db.View(func(tx *bolt.Tx) error {
		actualStructure := tx.Inspect()
		assert.Equal(t, expectedStructure, actualStructure)
		return nil
	})
}

// Ensure a large bucket can calculate stats.
func TestBucket_Stats_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	db := btesting.MustCreateDB(t)

	var index int
	for i := 0; i < 100; i++ {
		// Add bucket with lots of keys.
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("widgets"))
			require.NoError(t, err)
			for i := 0; i < 1000; i++ {
				require.NoError(t, b.Put([]byte(strconv.Itoa(index)), []byte(strconv.Itoa(index))))
				index++
			}
			return nil
		})
		require.NoError(t, err)
	}

	db.MustCheck()

	pageSize2stats := map[int]bolt.BucketStats{
		4096: {
			BranchPageN:       13,
			BranchOverflowN:   0,
			LeafPageN:         1196,
			LeafOverflowN:     0,
			KeyN:              100000,
			Depth:             3,
			BranchAlloc:       53248,
			BranchInuse:       25257,
			LeafAlloc:         4898816,
			LeafInuse:         2596916,
			BucketN:           1,
			InlineBucketN:     0,
			InlineBucketInuse: 0},
		16384: {
			BranchPageN:       1,
			BranchOverflowN:   0,
			LeafPageN:         292,
			LeafOverflowN:     0,
			KeyN:              100000,
			Depth:             2,
			BranchAlloc:       16384,
			BranchInuse:       6094,
			LeafAlloc:         4784128,
			LeafInuse:         2582452,
			BucketN:           1,
			InlineBucketN:     0,
			InlineBucketInuse: 0},
		65536: {
			BranchPageN:       1,
			BranchOverflowN:   0,
			LeafPageN:         73,
			LeafOverflowN:     0,
			KeyN:              100000,
			Depth:             2,
			BranchAlloc:       65536,
			BranchInuse:       1534,
			LeafAlloc:         4784128,
			LeafInuse:         2578948,
			BucketN:           1,
			InlineBucketN:     0,
			InlineBucketInuse: 0},
	}

	err := db.View(func(tx *bolt.Tx) error {
		stats := tx.Bucket([]byte("widgets")).Stats()
		t.Logf("Stats: %#v", stats)
		if expected, ok := pageSize2stats[db.Info().PageSize]; ok {
			assert.Equalf(t, expected, stats, "stats differs from expectations")
		} else {
			t.Skipf("No expectations for page size: %d", db.Info().PageSize)
		}
		return nil
	})
	require.NoError(t, err)
}

// Ensure that a bucket can write random keys and values across multiple transactions.
func TestBucket_Put_Single(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	index := 0
	err := quick.Check(func(items testdata) bool {
		db := btesting.MustCreateDB(t)
		defer db.MustClose()

		m := make(map[string][]byte)

		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)

		for _, item := range items {
			err := db.Update(func(tx *bolt.Tx) error {
				if err := tx.Bucket([]byte("widgets")).Put(item.Key, item.Value); err != nil {
					panic("put error: " + err.Error())
				}
				m[string(item.Key)] = item.Value
				return nil
			})
			require.NoError(t, err)

			// Verify all key/values so far.
			err = db.View(func(tx *bolt.Tx) error {
				i := 0
				for k, v := range m {
					value := tx.Bucket([]byte("widgets")).Get([]byte(k))
					if !bytes.Equal(value, v) {
						t.Logf("value mismatch [run %d] (%d of %d):\nkey: %x\ngot: %x\nexp: %x", index, i, len(m), []byte(k), value, v)
						db.CopyTempFile()
						t.FailNow()
					}
					i++
				}
				return nil
			})
			require.NoError(t, err)
		}

		index++
		return true
	}, qconfig())
	assert.NoError(t, err)
}

// Ensure that a transaction can insert multiple key/value pairs at once.
func TestBucket_Put_Multiple(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	err := quick.Check(func(items testdata) bool {
		db := btesting.MustCreateDB(t)
		defer db.MustClose()

		// Bulk insert all values.
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)

		err = db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				require.NoError(t, b.Put(item.Key, item.Value))
			}
			return nil
		})
		require.NoError(t, err)

		// Verify all items exist.
		err = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				value := b.Get(item.Key)
				if !bytes.Equal(item.Value, value) {
					db.CopyTempFile()
					t.Fatalf("exp=%x; got=%x", item.Value, value)
				}
			}
			return nil
		})
		require.NoError(t, err)

		return true
	}, qconfig())
	assert.NoError(t, err)
}

// Ensure that a transaction can delete all key/value pairs and return to a single leaf page.
func TestBucket_Delete_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	err := quick.Check(func(items testdata) bool {
		db := btesting.MustCreateDB(t)
		defer db.MustClose()

		// Bulk insert all values.
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket([]byte("widgets"))
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)

		err = db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				require.NoError(t, b.Put(item.Key, item.Value))
			}
			return nil
		})
		require.NoError(t, err)

		// Remove items one at a time and check consistency.
		for _, item := range items {
			err := db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("widgets")).Delete(item.Key)
			})
			require.NoError(t, err)
		}

		// Anything before our deletion index should be nil.
		err = db.View(func(tx *bolt.Tx) error {
			err := tx.Bucket([]byte("widgets")).ForEach(func(k, v []byte) error {
				t.Fatalf("bucket should be empty; found: %06x", trunc(k, 3))
				return nil
			})
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)

		return true
	}, qconfig())
	assert.NoError(t, err)
}

func BenchmarkBucket_CreateBucketIfNotExists(b *testing.B) {
	db := btesting.MustCreateDB(b)
	defer db.MustClose()

	const bucketCount = 1_000_000

	err := db.Update(func(tx *bolt.Tx) error {
		for i := 0; i < bucketCount; i++ {
			bucketName := fmt.Sprintf("bucket_%d", i)
			_, berr := tx.CreateBucket([]byte(bucketName))
			require.NoError(b, berr)
		}
		return nil
	})
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Update(func(tx *bolt.Tx) error {
			_, berr := tx.CreateBucketIfNotExists([]byte("bucket_100"))
			return berr
		})
		require.NoError(b, err)
	}
}

func ExampleBucket_Put() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Start a write transaction.
	if err := db.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}

		// Set the value "bar" for the key "foo".
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Read value back in a different read-only transaction.
	if err := db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value of 'foo' is: %s\n", value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// The value of 'foo' is: bar
}

func ExampleBucket_Delete() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Start a write transaction.
	if err := db.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}

		// Set the value "bar" for the key "foo".
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
		}

		// Retrieve the key back from the database and verify it.
		value := b.Get([]byte("foo"))
		fmt.Printf("The value of 'foo' was: %s\n", value)

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Delete the key in a different write transaction.
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("widgets")).Delete([]byte("foo"))
	}); err != nil {
		log.Fatal(err)
	}

	// Retrieve the key again.
	if err := db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		if value == nil {
			fmt.Printf("The value of 'foo' is now: nil\n")
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// The value of 'foo' was: bar
	// The value of 'foo' is now: nil
}

func ExampleBucket_ForEach() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Insert data into a bucket.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("animals"))
		if err != nil {
			return err
		}

		if err := b.Put([]byte("dog"), []byte("fun")); err != nil {
			return err
		}
		if err := b.Put([]byte("cat"), []byte("lame")); err != nil {
			return err
		}
		if err := b.Put([]byte("liger"), []byte("awesome")); err != nil {
			return err
		}

		// Iterate over items in sorted key order.
		if err := b.ForEach(func(k, v []byte) error {
			fmt.Printf("A %s is %s.\n", k, v)
			return nil
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// A cat is lame.
	// A dog is fun.
	// A liger is awesome.
}

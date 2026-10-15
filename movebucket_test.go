package bbolt_test

import (
	crand "crypto/rand"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"
)

func TestTx_MoveBucket(t *testing.T) {
	testCases := []struct {
		name                    string
		srcBucketPath           []string
		dstBucketPath           []string
		bucketToMove            string
		bucketExistInSrc        bool
		bucketExistInDst        bool
		hasIncompatibleKeyInSrc bool
		hasIncompatibleKeyInDst bool
		expectedErr             error
	}{
		// normal cases
		{
			name:                    "normal case",
			srcBucketPath:           []string{"sb1", "sb2"},
			dstBucketPath:           []string{"db1", "db2"},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        true,
			bucketExistInDst:        false,
			hasIncompatibleKeyInSrc: false,
			hasIncompatibleKeyInDst: false,
			expectedErr:             nil,
		},
		{
			name:                    "the source and target bucket share the same grandparent",
			srcBucketPath:           []string{"grandparent", "sb2"},
			dstBucketPath:           []string{"grandparent", "db2"},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        true,
			bucketExistInDst:        false,
			hasIncompatibleKeyInSrc: false,
			hasIncompatibleKeyInDst: false,
			expectedErr:             nil,
		},
		{
			name:                    "bucketToMove is a top level bucket",
			srcBucketPath:           []string{},
			dstBucketPath:           []string{"db1", "db2"},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        true,
			bucketExistInDst:        false,
			hasIncompatibleKeyInSrc: false,
			hasIncompatibleKeyInDst: false,
			expectedErr:             nil,
		},
		{
			name:                    "convert bucketToMove to a top level bucket",
			srcBucketPath:           []string{"sb1", "sb2"},
			dstBucketPath:           []string{},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        true,
			bucketExistInDst:        false,
			hasIncompatibleKeyInSrc: false,
			hasIncompatibleKeyInDst: false,
			expectedErr:             nil,
		},
		// negative cases
		{
			name:                    "bucketToMove not exist in source bucket",
			srcBucketPath:           []string{"sb1", "sb2"},
			dstBucketPath:           []string{"db1", "db2"},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        false,
			bucketExistInDst:        false,
			hasIncompatibleKeyInSrc: false,
			hasIncompatibleKeyInDst: false,
			expectedErr:             errors.ErrBucketNotFound,
		},
		{
			name:                    "bucketToMove exist in target bucket",
			srcBucketPath:           []string{"sb1", "sb2"},
			dstBucketPath:           []string{"db1", "db2"},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        true,
			bucketExistInDst:        true,
			hasIncompatibleKeyInSrc: false,
			hasIncompatibleKeyInDst: false,
			expectedErr:             errors.ErrBucketExists,
		},
		{
			name:                    "incompatible key exist in source bucket",
			srcBucketPath:           []string{"sb1", "sb2"},
			dstBucketPath:           []string{"db1", "db2"},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        false,
			bucketExistInDst:        false,
			hasIncompatibleKeyInSrc: true,
			hasIncompatibleKeyInDst: false,
			expectedErr:             errors.ErrIncompatibleValue,
		},
		{
			name:                    "incompatible key exist in target bucket",
			srcBucketPath:           []string{"sb1", "sb2"},
			dstBucketPath:           []string{"db1", "db2"},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        true,
			bucketExistInDst:        false,
			hasIncompatibleKeyInSrc: false,
			hasIncompatibleKeyInDst: true,
			expectedErr:             errors.ErrIncompatibleValue,
		},
		{
			name:                    "the source and target are the same bucket",
			srcBucketPath:           []string{"sb1", "sb2"},
			dstBucketPath:           []string{"sb1", "sb2"},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        true,
			bucketExistInDst:        false,
			hasIncompatibleKeyInSrc: false,
			hasIncompatibleKeyInDst: false,
			expectedErr:             errors.ErrSameBuckets,
		},
		{
			name:                    "both the source and target are the root bucket",
			srcBucketPath:           []string{},
			dstBucketPath:           []string{},
			bucketToMove:            "bucketToMove",
			bucketExistInSrc:        true,
			bucketExistInDst:        false,
			hasIncompatibleKeyInSrc: false,
			hasIncompatibleKeyInDst: false,
			expectedErr:             errors.ErrSameBuckets,
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(*testing.T) {
			db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})

			dumpBucketBeforeMoving := filepath.Join(t.TempDir(), "dbBeforeMove")
			dumpBucketAfterMoving := filepath.Join(t.TempDir(), "dbAfterMove")

			t.Log("Creating sample db and populate some data")
			err := db.Update(func(tx *bbolt.Tx) error {
				srcBucket := prepareBuckets(t, tx, tc.srcBucketPath...)
				dstBucket := prepareBuckets(t, tx, tc.dstBucketPath...)

				if tc.bucketExistInSrc {
					_ = createBucketAndPopulateData(t, tx, srcBucket, tc.bucketToMove)
				}

				if tc.bucketExistInDst {
					_ = createBucketAndPopulateData(t, tx, dstBucket, tc.bucketToMove)
				}

				if tc.hasIncompatibleKeyInSrc {
					putErr := srcBucket.Put([]byte(tc.bucketToMove), []byte("bar"))
					require.NoError(t, putErr)
				}

				if tc.hasIncompatibleKeyInDst {
					putErr := dstBucket.Put([]byte(tc.bucketToMove), []byte("bar"))
					require.NoError(t, putErr)
				}

				return nil
			})
			require.NoError(t, err)

			t.Log("Moving bucket")
			err = db.Update(func(tx *bbolt.Tx) error {
				srcBucket := prepareBuckets(t, tx, tc.srcBucketPath...)
				dstBucket := prepareBuckets(t, tx, tc.dstBucketPath...)

				if tc.expectedErr == nil {
					t.Logf("Dump the bucket to %s before moving it", dumpBucketBeforeMoving)
					bk := openBucket(tx, srcBucket, tc.bucketToMove)
					dumpErr := dumpBucket([]byte(tc.bucketToMove), bk, dumpBucketBeforeMoving)
					require.NoError(t, dumpErr)
				}

				mErr := tx.MoveBucket([]byte(tc.bucketToMove), srcBucket, dstBucket)
				require.Equal(t, tc.expectedErr, mErr)

				if tc.expectedErr == nil {
					t.Logf("Dump the bucket to %s after moving it", dumpBucketAfterMoving)
					bk := openBucket(tx, dstBucket, tc.bucketToMove)
					dumpErr := dumpBucket([]byte(tc.bucketToMove), bk, dumpBucketAfterMoving)
					require.NoError(t, dumpErr)
				}

				return nil
			})
			require.NoError(t, err)

			// skip assertion if failure expected
			if tc.expectedErr != nil {
				return
			}

			t.Log("Verifying the bucket should be identical before and after being moved")
			dataBeforeMove, err := os.ReadFile(dumpBucketBeforeMoving)
			require.NoError(t, err)
			dataAfterMove, err := os.ReadFile(dumpBucketAfterMoving)
			require.NoError(t, err)
			require.Equal(t, dataBeforeMove, dataAfterMove)
		})
	}
}

func TestBucket_MoveBucket_DiffDB(t *testing.T) {
	srcBucketPath := []string{"sb1", "sb2"}
	dstBucketPath := []string{"db1", "db2"}
	bucketToMove := "bucketToMove"

	var srcBucket *bbolt.Bucket

	t.Log("Creating source bucket and populate some data")
	srcDB := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})
	err := srcDB.Update(func(tx *bbolt.Tx) error {
		srcBucket = prepareBuckets(t, tx, srcBucketPath...)
		return nil
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, srcDB.Close())
	}()

	t.Log("Creating target bucket and populate some data")
	dstDB := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})
	err = dstDB.Update(func(tx *bbolt.Tx) error {
		prepareBuckets(t, tx, dstBucketPath...)
		return nil
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, dstDB.Close())
	}()

	t.Log("Reading source bucket in a separate RWTx")
	sTx, sErr := srcDB.Begin(true)
	require.NoError(t, sErr)
	defer func() {
		require.NoError(t, sTx.Rollback())
	}()
	srcBucket = prepareBuckets(t, sTx, srcBucketPath...)

	t.Log("Moving the sub-bucket in a separate RWTx")
	err = dstDB.Update(func(tx *bbolt.Tx) error {
		dstBucket := prepareBuckets(t, tx, dstBucketPath...)
		mErr := srcBucket.MoveBucket([]byte(bucketToMove), dstBucket)
		require.Equal(t, errors.ErrDifferentDB, mErr)

		return nil
	})
	require.NoError(t, err)
}

func TestBucket_Write_after_MoveBucket(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})

	err := db.Update(func(tx *bbolt.Tx) error {
		b1, err := tx.CreateBucket([]byte("b1"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucket([]byte("b2"))
		if err != nil {
			return err
		}
		if _, err = b1.CreateBucket([]byte("childToMove")); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bbolt.Tx) error {
		b1 := tx.Bucket([]byte("b1"))
		b2 := tx.Bucket([]byte("b2"))
		childToMove := b1.Bucket([]byte("childToMove"))

		if err := tx.MoveBucket([]byte("childToMove"), b1, b2); err != nil {
			return err
		}

		return childToMove.Put([]byte("foo"), []byte("bar"))
	})
	require.NoError(t, err)

	_ = db.View(func(tx *bbolt.Tx) error {
		b2 := tx.Bucket([]byte("b2"))
		childToMove := b2.Bucket([]byte("childToMove"))
		val := childToMove.Get([]byte("foo"))
		require.Equal(t, []byte("bar"), val)
		return nil
	})
}

func TestTx_MoveBucket_MultipleTimes_Cyclic(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})

	err := db.Update(func(tx *bbolt.Tx) error {
		a, err := tx.CreateBucket([]byte("a"))
		if err != nil {
			return err
		}
		if _, err = a.CreateBucket([]byte("b")); err != nil {
			return err
		}
		_, err = tx.CreateBucket([]byte("x"))
		return err
	})
	require.NoError(t, err)

	var moveErr error
	err = db.Update(func(tx *bbolt.Tx) error {
		a := tx.Bucket([]byte("a"))
		x := tx.Bucket([]byte("x"))
		b := a.Bucket([]byte("b"))

		require.NotNil(t, b)
		require.Zero(t, b.Root())
		if err := tx.MoveBucket([]byte("b"), a, x); err != nil {
			return err
		}

		moveErr = tx.MoveBucket([]byte("x"), nil, b)
		return nil
	})
	require.NoError(t, err)
	require.ErrorIs(t, moveErr, errors.ErrCyclicBucketMove)
}

func TestTx_MoveBucket_Cyclic(t *testing.T) {
	testCases := []struct {
		name string
		// dstBucketPath is relative to the bucket being moved, e.g.
		// []string{"child"} means dst is a direct child of the bucket
		// being moved, and []string{"child", "grandchild"} means dst
		// is a grandchild of the bucket being moved.
		dstBucketPath []string
	}{
		{
			name:          "moving a bucket into itself",
			dstBucketPath: []string{},
		},
		{
			name:          "moving a bucket into its own child",
			dstBucketPath: []string{"child"},
		},
		{
			name:          "moving a bucket into its own grandchild",
			dstBucketPath: []string{"child", "grandchild"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})

			srcBucketPath := []string{"sb1", "sb2"}
			bucketToMove := "bucketToMove"

			err := db.Update(func(tx *bbolt.Tx) error {
				srcBucket := prepareBuckets(t, tx, srcBucketPath...)
				movedBucket := createBucketAndPopulateData(t, tx, srcBucket, bucketToMove)

				// Build the nested path (dstBucketPath) inside the bucket being moved.
				nested := movedBucket
				for _, name := range tc.dstBucketPath {
					nested = createBucketAndPopulateData(t, tx, nested, name)
				}
				return nil
			})
			require.NoError(t, err)

			err = db.Update(func(tx *bbolt.Tx) error {
				srcBucket := prepareBuckets(t, tx, srcBucketPath...)
				movedBucket := srcBucket.Bucket([]byte(bucketToMove))
				require.NotNil(t, movedBucket)

				dstBucket := movedBucket
				for _, name := range tc.dstBucketPath {
					dstBucket = dstBucket.Bucket([]byte(name))
					require.NotNil(t, dstBucket)
				}

				mErr := tx.MoveBucket([]byte(bucketToMove), srcBucket, dstBucket)
				require.Equal(t, errors.ErrCyclicBucketMove, mErr)
				return nil
			})
			require.NoError(t, err)
		})
	}
}

// TestTx_MoveBucket_Cyclic_With_InlineBucket verifies that the cyclic-move
// check also catches descendants that are inline buckets (RootPage() == 0),
// not just regular page-backed buckets.
func TestTx_MoveBucket_Cyclic_With_InlineBucket(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})
	err := db.Update(func(tx *bbolt.Tx) error {
		b1, berr := tx.CreateBucketIfNotExists([]byte("b1"))
		if berr != nil {
			return berr
		}
		b2, berr := b1.CreateBucketIfNotExists([]byte("b2"))
		if berr != nil {
			return berr
		}
		// The b3 is an inline bucket, which means its RootPage is 0.
		b3, berr := b2.CreateBucketIfNotExists([]byte("b3"))
		if berr != nil {
			return berr
		}
		return b3.Put([]byte("key"), []byte("value"))
	})
	require.NoError(t, err)

	err = db.Update(func(tx *bbolt.Tx) error {
		b1 := tx.Bucket([]byte("b1"))
		b2 := b1.Bucket([]byte("b2"))
		b3 := b2.Bucket([]byte("b3"))

		return tx.MoveBucket([]byte("b2"), b1, b3)
	})
	require.Equal(t, errors.ErrCyclicBucketMove, err)
}

func TestBucket_MoveBucket_DiffTx(t *testing.T) {
	testCases := []struct {
		name            string
		srcBucketPath   []string
		dstBucketPath   []string
		isSrcWritableTx bool
		isDstWritableTx bool
		bucketToMove    string
		expectedErr     error
	}{
		{
			name:            "src is RWTx and target is RTx",
			srcBucketPath:   []string{"sb1", "sb2"},
			dstBucketPath:   []string{"db1", "db2"},
			isSrcWritableTx: true,
			isDstWritableTx: false,
			bucketToMove:    "bucketToMove",
			expectedErr:     errors.ErrTxNotWritable,
		},
		{
			name:            "src is RTx and target is RWTx",
			srcBucketPath:   []string{"sb1", "sb2"},
			dstBucketPath:   []string{"db1", "db2"},
			isSrcWritableTx: false,
			isDstWritableTx: true,
			bucketToMove:    "bucketToMove",
			expectedErr:     errors.ErrTxNotWritable,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var srcBucket *bbolt.Bucket
			var dstBucket *bbolt.Bucket

			t.Log("Creating source and target buckets and populate some data")
			db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: 4096})
			err := db.Update(func(tx *bbolt.Tx) error {
				srcBucket = prepareBuckets(t, tx, tc.srcBucketPath...)
				dstBucket = prepareBuckets(t, tx, tc.dstBucketPath...)
				return nil
			})
			require.NoError(t, err)
			defer func() {
				require.NoError(t, db.Close())
			}()

			t.Log("Opening source bucket in a separate Tx")
			sTx, sErr := db.Begin(tc.isSrcWritableTx)
			require.NoError(t, sErr)
			defer func() {
				require.NoError(t, sTx.Rollback())
			}()
			srcBucket = prepareBuckets(t, sTx, tc.srcBucketPath...)

			t.Log("Opening target bucket in a separate Tx")
			dTx, dErr := db.Begin(tc.isDstWritableTx)
			require.NoError(t, dErr)
			defer func() {
				require.NoError(t, dTx.Rollback())
			}()
			dstBucket = prepareBuckets(t, dTx, tc.dstBucketPath...)

			t.Log("Moving the sub-bucket")
			mErr := srcBucket.MoveBucket([]byte(tc.bucketToMove), dstBucket)
			require.Equal(t, tc.expectedErr, mErr)
		})
	}
}

// prepareBuckets opens the bucket chain. For each bucket in the chain,
// open it if existed, otherwise create it and populate sample data.
func prepareBuckets(t testing.TB, tx *bbolt.Tx, buckets ...string) *bbolt.Bucket {
	var bk *bbolt.Bucket

	for _, key := range buckets {
		if childBucket := openBucket(tx, bk, key); childBucket == nil {
			bk = createBucketAndPopulateData(t, tx, bk, key)
		} else {
			bk = childBucket
		}
	}
	return bk
}

func openBucket(tx *bbolt.Tx, bk *bbolt.Bucket, bucketToOpen string) *bbolt.Bucket {
	if bk == nil {
		return tx.Bucket([]byte(bucketToOpen))
	}
	return bk.Bucket([]byte(bucketToOpen))
}

func createBucketAndPopulateData(t testing.TB, tx *bbolt.Tx, bk *bbolt.Bucket, bucketName string) *bbolt.Bucket {
	if bk == nil {
		newBucket, err := tx.CreateBucket([]byte(bucketName))
		require.NoError(t, err, "failed to create bucket %s", bucketName)
		populateSampleDataInBucket(t, newBucket, rand.Intn(4096))
		return newBucket
	}

	newBucket, err := bk.CreateBucket([]byte(bucketName))
	require.NoError(t, err, "failed to create bucket %s", bucketName)
	populateSampleDataInBucket(t, newBucket, rand.Intn(4096))
	return newBucket
}

func populateSampleDataInBucket(t testing.TB, bk *bbolt.Bucket, n int) {
	var min, max = 1, 1024

	for i := 0; i < n; i++ {
		// generate rand key/value length
		keyLength := rand.Intn(max-min) + min
		valLength := rand.Intn(max-min) + min

		keyData := make([]byte, keyLength)
		valData := make([]byte, valLength)

		_, err := crand.Read(keyData)
		require.NoError(t, err)

		_, err = crand.Read(valData)
		require.NoError(t, err)

		err = bk.Put(keyData, valData)
		require.NoError(t, err)
	}
}

func TestBucket_MoveBucket_PreservesSequence(t *testing.T) {
	db := btesting.MustCreateDB(t)

	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		src, err := tx.CreateBucket([]byte("src"))
		require.NoError(t, err)
		dst, err := tx.CreateBucket([]byte("dst"))
		require.NoError(t, err)
		child, err := src.CreateBucket([]byte("child"))
		require.NoError(t, err)
		require.NoError(t, child.SetSequence(42))

		require.NoError(t, src.MoveBucket([]byte("child"), dst))
		require.Equal(t, uint64(42), dst.Bucket([]byte("child")).Sequence())
		return nil
	}))

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		child := tx.Bucket([]byte("dst")).Bucket([]byte("child"))
		require.NotNil(t, child)
		require.Equal(t, uint64(42), child.Sequence())
		return nil
	}))
}

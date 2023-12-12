package bbolt_test

import (
	"bytes"
	crand "crypto/rand"
	"math/rand"
	"os"
	"testing"

	"go.etcd.io/bbolt"
	"go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/btesting"

	"github.com/stretchr/testify/require"
)

func TestTx_MoveBucket(t *testing.T) {
	testCases := []struct {
		name                 string
		srcBucketPath        []string
		dstBucketPath        []string
		bucketToMove         string
		incompatibleKeyInSrc bool
		incompatibleKeyInDst bool
		parentSrc            bool
		parentDst            bool
		expActErr            error
	}{
		{
			name:                 "happy path",
			srcBucketPath:        []string{"sb1", "sb2", "sb3ToMove"},
			dstBucketPath:        []string{"db1", "db2"},
			bucketToMove:         "sb3ToMove",
			incompatibleKeyInSrc: false,
			incompatibleKeyInDst: false,
			parentSrc:            true,
			parentDst:            false,
			expActErr:            nil,
		},
		{
			name:                 "bucketToMove not exist in srcBucket",
			srcBucketPath:        []string{"sb1", "sb2"},
			dstBucketPath:        []string{"db1", "db2"},
			bucketToMove:         "sb3ToMove",
			incompatibleKeyInSrc: false,
			incompatibleKeyInDst: false,
			parentSrc:            false,
			parentDst:            false,
			expActErr:            errors.ErrBucketNotFound,
		},
		{
			name:                 "bucketToMove exist in dstBucket",
			srcBucketPath:        []string{"sb1", "sb2", "sb3ToMove"},
			dstBucketPath:        []string{"db1", "db2", "sb3ToMove"},
			bucketToMove:         "sb3ToMove",
			incompatibleKeyInSrc: false,
			incompatibleKeyInDst: false,
			parentSrc:            true,
			parentDst:            true,
			expActErr:            errors.ErrBucketExists,
		},
		{
			name:                 "bucketToMove key exist in srcBucket but no subBucket value",
			srcBucketPath:        []string{"sb1", "sb2"},
			dstBucketPath:        []string{"db1", "db2"},
			bucketToMove:         "sb3ToMove",
			incompatibleKeyInSrc: true,
			incompatibleKeyInDst: false,
			parentSrc:            true,
			parentDst:            false,
			expActErr:            errors.ErrIncompatibleValue,
		},
		{
			name:                 "bucketToMove key exist in dstBucket but no subBucket value",
			srcBucketPath:        []string{"sb1", "sb2", "sb3ToMove"},
			dstBucketPath:        []string{"db1", "db2"},
			bucketToMove:         "sb3ToMove",
			incompatibleKeyInSrc: false,
			incompatibleKeyInDst: true,
			parentSrc:            true,
			parentDst:            true,
			expActErr:            errors.ErrIncompatibleValue,
		},
		{
			name:                 "srcBucket is rootBucket",
			srcBucketPath:        []string{"", "sb3ToMove"},
			dstBucketPath:        []string{"db1", "db2"},
			bucketToMove:         "sb3ToMove",
			incompatibleKeyInSrc: false,
			incompatibleKeyInDst: false,
			parentSrc:            true,
			parentDst:            false,
			expActErr:            nil,
		},
		{
			name:                 "dstBucket is rootBucket",
			srcBucketPath:        []string{"sb1", "sb2", "sb3ToMove"},
			dstBucketPath:        []string{""},
			bucketToMove:         "sb3ToMove",
			incompatibleKeyInSrc: false,
			incompatibleKeyInDst: false,
			parentSrc:            true,
			parentDst:            false,
			expActErr:            nil,
		},
		{
			name:                 "srcBucket is rootBucket and dstBucket is rootBucket",
			srcBucketPath:        []string{"", "sb3ToMove"},
			dstBucketPath:        []string{""},
			bucketToMove:         "sb3ToMove",
			incompatibleKeyInSrc: false,
			incompatibleKeyInDst: false,
			parentSrc:            false,
			parentDst:            false,
			expActErr:            errors.ErrSameBuckets,
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(*testing.T) {
			db := btesting.MustCreateDBWithOption(t, &bbolt.Options{PageSize: pageSize})

			dumpBucketBeforeMoving := tempfile()
			dumpBucketAfterMoving := tempfile()

			// arrange
			if err := db.Update(func(tx *bbolt.Tx) error {
				srcBucket := openBuckets(t, tx, tc.incompatibleKeyInSrc, true, false, tc.srcBucketPath...)
				dstBucket := openBuckets(t, tx, tc.incompatibleKeyInDst, true, false, tc.dstBucketPath...)

				if tc.incompatibleKeyInSrc {
					if pErr := srcBucket.Put([]byte(tc.bucketToMove), []byte("0")); pErr != nil {
						t.Fatalf("error inserting key %v, and value %v in bucket %v: %v", tc.bucketToMove, "0", srcBucket, pErr)
					}
				}

				if tc.incompatibleKeyInDst {
					if pErr := dstBucket.Put([]byte(tc.bucketToMove), []byte("0")); pErr != nil {
						t.Fatalf("error inserting key %v, and value %v in bucket %v: %v", tc.bucketToMove, "0", dstBucket, pErr)
					}
				}

				return nil
			}); err != nil {
				t.Fatal(err)
			}
			db.MustCheck()

			// act
			if err := db.Update(func(tx *bbolt.Tx) error {
				srcBucket := openBuckets(t, tx, false, false, tc.parentSrc, tc.srcBucketPath...)
				dstBucket := openBuckets(t, tx, false, false, tc.parentDst, tc.dstBucketPath...)

				var bucketToMove *bbolt.Bucket
				if srcBucket != nil {
					bucketToMove = srcBucket.Bucket([]byte(tc.bucketToMove))
				} else {
					bucketToMove = tx.Bucket([]byte(tc.bucketToMove))
				}

				if tc.expActErr == nil && bucketToMove != nil {
					if wErr := dumpBucket([]byte(tc.bucketToMove), bucketToMove, dumpBucketBeforeMoving); wErr != nil {
						t.Fatalf("error dumping bucket %v to file %v: %v", bucketToMove.String(), dumpBucketBeforeMoving, wErr)
					}
				}

				mErr := tx.MoveBucket([]byte(tc.bucketToMove), srcBucket, dstBucket)
				require.ErrorIs(t, mErr, tc.expActErr)

				return nil
			}); err != nil {
				t.Fatal(err)
			}
			db.MustCheck()

			// skip assertion if failure expected
			if tc.expActErr != nil {
				return
			}

			// assert
			if err := db.Update(func(tx *bbolt.Tx) error {
				var movedBucket *bbolt.Bucket
				srcBucket := openBuckets(t, tx, false, false, tc.parentSrc, tc.srcBucketPath...)

				if srcBucket != nil {
					if movedBucket = srcBucket.Bucket([]byte(tc.bucketToMove)); movedBucket != nil {
						t.Fatalf("expected childBucket %v to be moved from srcBucket %v", tc.bucketToMove, srcBucket)
					}
				} else {
					if movedBucket = tx.Bucket([]byte(tc.bucketToMove)); movedBucket != nil {
						t.Fatalf("expected childBucket %v to be moved from root bucket %v", tc.bucketToMove, "root bucket")
					}
				}

				dstBucket := openBuckets(t, tx, false, false, tc.parentDst, tc.dstBucketPath...)
				if dstBucket != nil {
					if movedBucket = dstBucket.Bucket([]byte(tc.bucketToMove)); movedBucket == nil {
						t.Fatalf("expected childBucket %v to be child of dstBucket %v", tc.bucketToMove, dstBucket)
					}
				} else {
					if movedBucket = tx.Bucket([]byte(tc.bucketToMove)); movedBucket == nil {
						t.Fatalf("expected childBucket %v to be child of dstBucket %v", tc.bucketToMove, "root bucket")
					}
				}

				wErr := dumpBucket([]byte(tc.bucketToMove), movedBucket, dumpBucketAfterMoving)
				if wErr != nil {
					t.Fatalf("error dumping bucket %v to file %v", movedBucket.String(), dumpBucketAfterMoving)
				}

				beforeBucket := readBucketFromFile(t, dumpBucketBeforeMoving)
				afterBucket := readBucketFromFile(t, dumpBucketAfterMoving)

				if !bytes.Equal(beforeBucket, afterBucket) {
					t.Fatalf("bucket's content before moving is different than after moving")
				}

				return nil
			}); err != nil {
				t.Fatal(err)
			}
			db.MustCheck()
		})
	}
}

func openBuckets(t testing.TB, tx *bbolt.Tx, incompatibleKey bool, init bool, parent bool, paths ...string) *bbolt.Bucket {
	t.Helper()

	var bk *bbolt.Bucket
	var err error

	idx := len(paths) - 1
	for i, key := range paths {
		if len(key) == 0 {
			if !init {
				break
			}
			continue
		}
		if (incompatibleKey && i == idx) || (parent && i == idx) {
			continue
		}
		if bk == nil {
			bk, err = tx.CreateBucketIfNotExists([]byte(key))
		} else {
			bk, err = bk.CreateBucketIfNotExists([]byte(key))
		}
		if err != nil {
			t.Fatalf("error creating bucket %v: %v", key, err)
		}
		if init {
			insertRandKeysValuesBucket(t, bk, rand.Intn(4096))
		}
	}

	return bk
}

func readBucketFromFile(t testing.TB, tmpFile string) []byte {
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("error reading temp file %v", tmpFile)
	}

	return data
}

func insertRandKeysValuesBucket(t testing.TB, bk *bbolt.Bucket, n int) {
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

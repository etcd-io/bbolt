package main_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// Ensure the "keys" command can print a list of keys for a bucket.
func TestKeysCommand_Run(t *testing.T) {
	testCases := []struct {
		name       string
		printable  bool
		testBucket string
		expected   string
	}{
		{
			name:       "printable keys",
			printable:  true,
			testBucket: "foo",
			expected:   "foo-0\nfoo-1\nfoo-2\n",
		},
		{
			name:       "non printable keys",
			printable:  false,
			testBucket: "bar",
			expected:   convertInt64KeysIntoHexString(100001, 100002, 100003),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Creating test database for subtest '%s'", tc.name)
			db := btesting.MustCreateDB(t)
			err := db.Update(func(tx *bolt.Tx) error {
				t.Logf("creating test bucket %s", tc.testBucket)
				b, bErr := tx.CreateBucketIfNotExists([]byte(tc.testBucket))
				if bErr != nil {
					return fmt.Errorf("error creating test bucket %q: %v", tc.testBucket, bErr)
				}

				t.Logf("inserting test data into test bucket %s", tc.testBucket)
				if tc.printable {
					for i := 0; i < 3; i++ {
						key := fmt.Sprintf("%s-%d", tc.testBucket, i)
						if pErr := b.Put([]byte(key), []byte{0}); pErr != nil {
							return pErr
						}
					}
				} else {
					for i := 100001; i < 100004; i++ {
						k := convertInt64IntoBytes(int64(i))
						if pErr := b.Put(k, []byte{0}); pErr != nil {
							return pErr
						}
					}
				}
				return nil
			})
			require.NoError(t, err)
			db.Close()
			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			t.Log("Running Keys cmd")
			rootCmd := main.NewRootCommand()
			outputBuf := bytes.NewBufferString("")
			rootCmd.SetOut(outputBuf)
			rootCmd.SetArgs([]string{"keys", db.Path(), tc.testBucket})
			err = rootCmd.Execute()
			require.NoError(t, err)

			t.Log("Checking output")
			output, err := io.ReadAll(outputBuf)
			require.NoError(t, err)
			require.Equalf(t, tc.expected, string(output), "unexpected stdout:\n\n%s", string(output))
		})
	}
}

func TestKeyCommand_NoArgs(t *testing.T) {
	expErr := errors.New("requires at least 2 arg(s), only received 0")
	rootCmd := main.NewRootCommand()
	rootCmd.SetArgs([]string{"keys"})
	err := rootCmd.Execute()
	require.ErrorContains(t, err, expErr.Error())
}

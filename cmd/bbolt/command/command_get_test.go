package command_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/internal/btesting"
)

func TestGetCommand_Run(t *testing.T) {
	testCases := []struct {
		name          string
		printable     bool
		testBucket    string
		testKey       string
		expectedValue string
	}{
		{
			name:          "printable data",
			printable:     true,
			testBucket:    "foo",
			testKey:       "foo-1",
			expectedValue: "value-foo-1\n",
		},
		{
			name:          "non printable data",
			printable:     false,
			testBucket:    "bar",
			testKey:       "100001",
			expectedValue: hex.EncodeToString(convertInt64IntoBytes(100001)) + "\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Creating test database for subtest '%s'", tc.name)
			db := btesting.MustCreateDB(t)

			t.Log("Inserting test data")
			err := db.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte(tc.testBucket))
				if err != nil {
					return fmt.Errorf("create bucket %q: %w", tc.testBucket, err)
				}

				if tc.printable {
					return b.Put([]byte(tc.testKey), []byte("value-"+tc.testKey))
				}

				return b.Put([]byte(tc.testKey), convertInt64IntoBytes(100001))
			})
			require.NoError(t, err)
			db.Close()
			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			t.Log("Running get command")
			rootCmd := command.NewRootCommand()
			outputBuf := bytes.NewBufferString("")
			rootCmd.SetOut(outputBuf)
			rootCmd.SetArgs([]string{"get", db.Path(), tc.testBucket, tc.testKey})
			err = rootCmd.Execute()
			require.NoError(t, err)

			t.Log("Checking output")
			output, err := io.ReadAll(outputBuf)
			require.NoError(t, err)
			require.Equalf(t, tc.expectedValue, string(output), "unexpected stdout:\n\n%s", string(output))
		})
	}
}

func TestGetCommand_NoArgs(t *testing.T) {
	expErr := errors.New("requires at least 3 arg(s), only received 0")
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"get"})
	err := rootCmd.Execute()
	require.ErrorContains(t, err, expErr.Error())
}

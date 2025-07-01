package main_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// Ensure the "buckets" command can print a list of buckets.
func TestBucketsCommand_Run(t *testing.T) {

	testCases := []struct {
		name      string
		args      []string
		expErr    error
		expOutput string
	}{
		{
			name:      "buckets all buckets in bbolt database",
			args:      []string{"buckets", "path"},
			expErr:    nil,
			expOutput: "bar\nbaz\nfoo\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			t.Log("Creating sample DB")
			db := btesting.MustCreateDB(t)
			if err := db.Update(func(tx *bolt.Tx) error {
				for _, name := range []string{"foo", "bar", "baz"} {
					_, err := tx.CreateBucket([]byte(name))
					if err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			db.Close()
			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			t.Log("Running buckets cmd")
			rootCmd := main.NewRootCommand()
			outputBuf := bytes.NewBufferString("")
			rootCmd.SetOut(outputBuf)

			tc.args[1] = db.Path()
			rootCmd.SetArgs(tc.args)
			err := rootCmd.Execute()
			require.Equal(t, tc.expErr, err)

			t.Log("Checking output")
			output, err := io.ReadAll(outputBuf)
			require.NoError(t, err)
			require.Containsf(t, string(output), tc.expOutput, "unexpected stdout:\n\n%s", string(output))
		})
	}
}

package main_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// Ensure the "buckets" command can print a list of buckets.
func TestBuckets(t *testing.T) {
	// Create a test database and populate it with sample buckets.
	t.Log("Creating sample DB")
	db := btesting.MustCreateDB(t)
	t.Log("Creating sample Buckets")
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

	// setup the buckets command.
	t.Log("Running buckets command")
	rootCmd := main.NewRootCommand()
	var args []string = []string{"buckets", db.Path()}
	outputBuf := bytes.NewBufferString("")
	rootCmd.SetOut(outputBuf)
	rootCmd.SetErr(outputBuf)
	rootCmd.SetArgs(args)
	_, err := rootCmd.ExecuteC()
	actualOutput := outputBuf.String()
	require.NoError(t, err)
	t.Log("Verify result")
	expected := "bar\nbaz\nfoo\n"
	require.EqualValues(t, expected, actualOutput)
}

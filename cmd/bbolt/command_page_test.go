package main_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt"
	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// Ensure the "page" command neither panics nor changes the db file.
func TestPageCommand_Run(t *testing.T) {
	t.Log("Creating sample DB")
	// Create a test database
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bbolt.Tx) error {
		// Create buckets and insert sample data
		for _, name := range []string{"foo", "bar"} {
			b, err := tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			for i := 0; i < 3; i++ {
				key := fmt.Sprintf("%s-%d", name, i)
				val := fmt.Sprintf("val-%s-%d", name, i)
				if err := b.Put([]byte(key), []byte(val)); err != nil {
					return err
				}
			}
		}
		return nil
	})
	require.NoError(t, err)
	db.Close()
	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	// Running the page command
	t.Log("Running page cmd with a valid page ID")
	rootCmd := main.NewRootCommand()
	outputBuf := bytes.NewBufferString("")
	rootCmd.SetOut(outputBuf)

	rootCmd.SetArgs([]string{"page", db.Path(), "0"})
	err = rootCmd.Execute()
	require.NoError(t, err)

	// Check that the command does not panic and generates output
	t.Log("Checking output for page command")
	_, err = io.ReadAll(outputBuf)
	require.NoError(t, err)

	// Verify output includes expected headers
	require.Contains(t, outputBuf.String(), "Page ID    TYPE      ITEMS   OVRFLW")

	// Test Case 2: Handle missing page ID argument
	t.Log("Running page cmd with missing page ID argument")
	rootCmd.SetArgs([]string{"page", db.Path()})
	err = rootCmd.Execute()
	require.ErrorContains(t, err, "page ID(s) required")

	// Test Case 3: Handle invalid page ID argument (non-numeric)
	t.Log("Running page cmd with invalid page ID argument")
	rootCmd.SetArgs([]string{"page", db.Path(), "invalid"})
	err = rootCmd.Execute()
	require.ErrorContains(t, err, "invalid page ID")
}

// Ensure the "page" command handles missing database path gracefully.
func TestPageCommand_NoArgs(t *testing.T) {
	t.Log("Running page cmd with missing database path")
	rootCmd := main.NewRootCommand()
	expErr := errors.New("accepts 2 arg(s), received 1")
	rootCmd.SetArgs([]string{"page"})
	err := rootCmd.Execute()
	require.ErrorContains(t, err, expErr.Error())
}

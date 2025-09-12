package command_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/internal/btesting"
)

// Ensure the "stats" command executes correctly with an empty database.
func TestStatsCommand_Run_EmptyDatabase(t *testing.T) {
	// ignore
	if os.Getpagesize() != 4096 {
		t.Skip("system does not use 4KB page size")
	}

	t.Log("Creating sample DB")
	db := btesting.MustCreateDB(t)
	db.Close()
	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	// generate expected result.
	exp := "Aggregate statistics for 0 buckets\n\n" +
		"Page count statistics\n" +
		"\tNumber of logical branch pages: 0\n" +
		"\tNumber of physical branch overflow pages: 0\n" +
		"\tNumber of logical leaf pages: 0\n" +
		"\tNumber of physical leaf overflow pages: 0\n" +
		"Tree statistics\n" +
		"\tNumber of keys/value pairs: 0\n" +
		"\tNumber of levels in B+tree: 0\n" +
		"Page size utilization\n" +
		"\tBytes allocated for physical branch pages: 0\n" +
		"\tBytes actually used for branch data: 0 (0%)\n" +
		"\tBytes allocated for physical leaf pages: 0\n" +
		"\tBytes actually used for leaf data: 0 (0%)\n" +
		"Bucket statistics\n" +
		"\tTotal number of buckets: 0\n" +
		"\tTotal number on inlined buckets: 0 (0%)\n" +
		"\tBytes used for inlined buckets: 0 (0%)\n"

	t.Log("Running stats cmd")
	rootCmd := command.NewRootCommand()
	outputBuf := bytes.NewBufferString("")
	rootCmd.SetOut(outputBuf)

	rootCmd.SetArgs([]string{"stats", db.Path()})
	err := rootCmd.Execute()
	require.NoError(t, err)

	t.Log("Checking output")
	output, err := io.ReadAll(outputBuf)
	require.NoError(t, err)
	require.Exactlyf(t, exp, string(output), "unexpected stdout:\n\n%s", string(output))
}

// Ensure the "stats" command can execute correctly.
func TestStatsCommand_Run(t *testing.T) {
	// ignore
	if os.Getpagesize() != 4096 {
		t.Skip("system does not use 4KB page size")
	}

	t.Log("Creating sample DB")
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		// create "foo" bucket.
		b, err := tx.CreateBucket([]byte("foo"))
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			if err := b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))); err != nil {
				return err
			}
		}

		// create "bar" bucket.
		b, err = tx.CreateBucket([]byte("bar"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			if err := b.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i))); err != nil {
				return err
			}
		}

		// create "baz" bucket.
		b, err = tx.CreateBucket([]byte("baz"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("key"), []byte("value")); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	// generate expected result.
	exp := "Aggregate statistics for 3 buckets\n\n" +
		"Page count statistics\n" +
		"\tNumber of logical branch pages: 0\n" +
		"\tNumber of physical branch overflow pages: 0\n" +
		"\tNumber of logical leaf pages: 1\n" +
		"\tNumber of physical leaf overflow pages: 0\n" +
		"Tree statistics\n" +
		"\tNumber of keys/value pairs: 111\n" +
		"\tNumber of levels in B+tree: 1\n" +
		"Page size utilization\n" +
		"\tBytes allocated for physical branch pages: 0\n" +
		"\tBytes actually used for branch data: 0 (0%)\n" +
		"\tBytes allocated for physical leaf pages: 4096\n" +
		"\tBytes actually used for leaf data: 1996 (48%)\n" +
		"Bucket statistics\n" +
		"\tTotal number of buckets: 3\n" +
		"\tTotal number on inlined buckets: 2 (66%)\n" +
		"\tBytes used for inlined buckets: 236 (11%)\n"

	t.Log("Running stats cmd")
	rootCmd := command.NewRootCommand()
	outputBuf := bytes.NewBufferString("")
	rootCmd.SetOut(outputBuf)

	rootCmd.SetArgs([]string{"stats", db.Path()})
	err := rootCmd.Execute()
	require.NoError(t, err)

	t.Log("Checking output")
	output, err := io.ReadAll(outputBuf)
	require.NoError(t, err)
	require.Exactlyf(t, exp, string(output), "unexpected stdout:\n\n%s", string(output))
}

func TestStatsCommand_NoArgs(t *testing.T) {
	expErr := errors.New("accepts between 1 and 2 arg(s), received 0")
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"stats"})
	err := rootCmd.Execute()
	require.ErrorContains(t, err, expErr.Error())
}

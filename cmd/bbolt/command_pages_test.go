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

// Ensure the "pages" command neither panic, nor change the db file.
func TestPagesCommand_Run(t *testing.T) {
	t.Log("Creating sample DB")
	db := btesting.MustCreateDB(t)
	err := db.Update(func(tx *bolt.Tx) error {
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

	t.Log("Running pages cmd")
	rootCmd := main.NewRootCommand()
	outputBuf := bytes.NewBufferString("")
	rootCmd.SetOut(outputBuf)

	rootCmd.SetArgs([]string{"pages", db.Path()})
	err = rootCmd.Execute()
	require.NoError(t, err)

	t.Log("Checking output")
	_, err = io.ReadAll(outputBuf)
	require.NoError(t, err)
}

func TestPagesCommand_NoArgs(t *testing.T) {
	expErr := errors.New("accepts 1 arg(s), received 0")
	rootCmd := main.NewRootCommand()
	rootCmd.SetArgs([]string{"pages"})
	err := rootCmd.Execute()
	require.ErrorContains(t, err, expErr.Error())
}

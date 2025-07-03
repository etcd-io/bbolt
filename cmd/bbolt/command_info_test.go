package main_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// Ensure the "info" command can print information about a database.
func TestInfoCommand_Run(t *testing.T) {
	t.Log("Creating sample DB")
	db := btesting.MustCreateDB(t)
	db.Close()
	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	t.Log("Running info cmd")
	rootCmd := main.NewRootCommand()
	outputBuf := bytes.NewBufferString("")
	rootCmd.SetOut(outputBuf)

	rootCmd.SetArgs([]string{"info", db.Path()})
	err := rootCmd.Execute()
	require.NoError(t, err)

	t.Log("Checking output")
	_, err = io.ReadAll(outputBuf)
	require.NoError(t, err)
}

func TestInfoCommand_NoArgs(t *testing.T) {
	expErr := errors.New("accepts 1 arg(s), received 0")
	rootCmd := main.NewRootCommand()
	rootCmd.SetArgs([]string{"info"})
	err := rootCmd.Execute()
	require.ErrorContains(t, err, expErr.Error())
}

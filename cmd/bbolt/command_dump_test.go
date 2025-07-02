package main_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

func TestDumpCommand_Run(t *testing.T) {
	t.Log("Creating database")
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	require.NoError(t, db.Close())
	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	t.Log("Running dump command")
	rootCmd := main.NewRootCommand()
	outputBuf := bytes.NewBufferString("")
	rootCmd.SetOut(outputBuf)
	rootCmd.SetArgs([]string{"dump", db.Path(), "0"})
	err := rootCmd.Execute()
	require.NoError(t, err)

	t.Log("Checking output")
	exp := `0000010 edda 0ced 0200 0000 0010 0000 0000 0000`
	output, err := io.ReadAll(outputBuf)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(output), exp), "unexpected stdout:", string(output))
}

func TestDumpCommand_NoArgs(t *testing.T) {
	expErr := errors.New("requires at least 2 arg(s), only received 0")
	rootCmd := main.NewRootCommand()
	rootCmd.SetArgs([]string{"dump"})
	err := rootCmd.Execute()
	require.ErrorContains(t, err, expErr.Error())
}

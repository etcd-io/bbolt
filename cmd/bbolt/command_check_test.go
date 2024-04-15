package main_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

func TestCheckCommand_Run(t *testing.T) {
	db := btesting.MustCreateDB(t)
	db.Close()
	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	rootCmd := main.NewRootCommand()
	// capture output for assertion
	outputBuf := bytes.NewBufferString("")
	rootCmd.SetOut(outputBuf)

	rootCmd.SetArgs([]string{
		"check", db.Path(),
	})
	err := rootCmd.Execute()
	require.NoError(t, err)

	output, err := io.ReadAll(outputBuf)
	require.NoError(t, err)
	require.Equalf(t, "OK\n", string(output), "unexpected stdout:\n\n%s", string(output))
}

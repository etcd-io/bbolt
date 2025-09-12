package command_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/cmd/bbolt/command"
	"go.etcd.io/bbolt/internal/btesting"
)

func TestPageCommand_Run(t *testing.T) {
	t.Log("Creating a new database")
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	db.Close()

	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	exp := "Page ID:    0\n" +
		"Page Type:  meta\n" +
		"Total Size: 4096 bytes\n" +
		"Overflow pages: 0\n" +
		"Version:    2\n" +
		"Page Size:  4096 bytes\n" +
		"Flags:      00000000\n" +
		"Root:       <pgid=3>\n" +
		"Freelist:   <pgid=2>\n" +
		"HWM:        <pgid=4>\n" +
		"Txn ID:     0\n" +
		"Checksum:   07516e114689fdee\n\n"

	t.Log("Running page command")
	rootCmd := command.NewRootCommand()
	outBuf := &bytes.Buffer{}
	rootCmd.SetOut(outBuf)
	rootCmd.SetArgs([]string{"page", db.Path(), "0"})

	err := rootCmd.Execute()
	require.NoError(t, err)
	require.Equal(t, exp, outBuf.String(), "unexpected stdout")
}

func TestPageCommand_ExclusiveArgs(t *testing.T) {
	testCases := []struct {
		name    string
		pageIds string
		allFlag string
		expErr  error
	}{
		{
			name:    "flag only",
			pageIds: "",
			allFlag: "--all",
			expErr:  nil,
		},
		{
			name:    "pageIds only",
			pageIds: "0",
			allFlag: "",
			expErr:  nil,
		},
		{
			name:    "pageIds and flag",
			pageIds: "0",
			allFlag: "--all",
			expErr:  command.ErrInvalidPageArgs,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Log("Creating a new database")
			db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
			db.Close()

			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			t.Log("Running page command")
			rootCmd := command.NewRootCommand()
			outBuf := &bytes.Buffer{}
			rootCmd.SetOut(outBuf)
			rootCmd.SetArgs([]string{"page", db.Path(), tc.pageIds, tc.allFlag})

			err := rootCmd.Execute()
			require.Equal(t, tc.expErr, err)
		})
	}
}

func TestPageCommand_NoArgs(t *testing.T) {
	expErr := errors.New("requires at least 1 arg(s), only received 0")
	rootCmd := command.NewRootCommand()
	rootCmd.SetArgs([]string{"page"})
	err := rootCmd.Execute()
	require.ErrorContains(t, err, expErr.Error())
}

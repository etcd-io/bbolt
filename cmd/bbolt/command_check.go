package main

import (
	"fmt"

	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func newCheckCommand() *cobra.Command {
	checkCmd := &cobra.Command{
		Use:   "check <bbolt-file>",
		Short: "verify integrity of bbolt database data",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return checkFunc(cmd, args[0])
		},
	}

	return checkCmd
}

func checkFunc(cmd *cobra.Command, dbPath string) error {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}

	// Open database.
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Perform consistency check.
	return db.View(func(tx *bolt.Tx) error {
		var count int
		for err := range tx.Check(bolt.WithKVStringer(CmdKvStringer())) {
			fmt.Fprintln(cmd.OutOrStdout(), err)
			count++
		}

		// Print summary of errors.
		if count > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "%d errors found\n", count)
			return guts_cli.ErrCorrupt
		}

		// Notify user that database is valid.
		fmt.Fprintln(cmd.OutOrStdout(), "OK")
		return nil
	})
}

package main

import (
	"fmt"

	"github.com/spf13/cobra"
	bolt "go.etcd.io/bbolt"
)

func newBucketsCommand() *cobra.Command {
	var bucketsCmd = &cobra.Command{
		Use:   "buckets <path>",
		Short: "print a list of buckets",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return bucketsFunc(cmd, args[0])
		},
	}
	return bucketsCmd
}

func bucketsFunc(cmd *cobra.Command, srcDBPath string) error {
	// Required database path.
	if srcDBPath == "" {
		return ErrPathRequired
		// Verify if the specified database file exists.
	} else if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	// Open database.
	db, err := bolt.Open(srcDBPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print the list of buckets in the database.
	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			fmt.Fprintln(cmd.OutOrStdout(), string(name))
			return nil
		})
	})
}

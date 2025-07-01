package main

import (
	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
)

var optionsFormat *string

func newKeysCommand() *cobra.Command {
	var format string

	keysCmd := &cobra.Command{
		Use:   "keys <bbolt-file> <bucket>",
		Short: "print a list of keys in the given (sub)bucket in bbolt database",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			optionsFormat = &format
			if _, err := checkSourceDBPath(args[0]); err != nil {
				return err
			}

			if len(args[1:]) == 0 {
				return ErrBucketRequired
			}
			return keysFunc(cmd, args[0], args[1:]...)
		},
	}

	keysCmd.Flags().StringVarP(&format, "format", "f", "auto", "Output format one of: "+FORMAT_MODES)
	return keysCmd
}

func keysFunc(cmd *cobra.Command, dbPath string, buckets ...string) error {
	// Open database.
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print keys.
	return db.View(func(tx *bolt.Tx) error {
		// Find bucket.
		lastBucket, err := findLastBucket(tx, buckets)
		if err != nil {
			return err
		}

		// Iterate over each key.
		return lastBucket.ForEach(func(key, _ []byte) error {
			return writelnBytes(cmd.OutOrStdout(), key, *optionsFormat)
		})
	})
}

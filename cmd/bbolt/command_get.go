package main

import (
	"fmt"

	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
)

func newGetCommand() *cobra.Command {
	var parseFormat, format string

	cmd := &cobra.Command{
		Use:   "get PATH [BUCKET..] KEY",
		Short: "Get the value of a key from a (sub)bucket in a bbolt database",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Argument validation
			path := args[0]
			buckets := args[1 : len(args)-1]
			keyStr := args[len(args)-1]

			// Validate the input parameters
			if len(buckets) == 0 {
				return fmt.Errorf("bucket is required: %w", ErrBucketRequired)
			}
			key, err := parseBytes(keyStr, parseFormat)
			if err != nil {
				return err
			}
			if len(key) == 0 {
				return fmt.Errorf("key is required")
			}

			// Check if the source DB path is valid
			if _, err := checkSourceDBPath(path); err != nil {
				return err
			}

			// Open the database
			db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
			if err != nil {
				return err
			}
			defer db.Close()

			// Access the database and get the value
			return db.View(func(tx *bolt.Tx) error {
				lastBucket, err := findLastBucket(tx, buckets)
				if err != nil {
					return err
				}
				val := lastBucket.Get(key)
				if val == nil {
					return fmt.Errorf("Error %w for key: %q hex: \"%x\"", ErrKeyNotFound, key, key)
				}
				return writelnBytes(cmd.OutOrStdout(), val, format)
			})
		},
	}

	cmd.Flags().StringVar(&parseFormat, "parse-format", "ascii-encoded", "Input format. One of: ascii-encoded|hex")
	cmd.Flags().StringVar(&format, "format", "auto", "Output format. One of: ascii|hex|auto")

	return cmd
}

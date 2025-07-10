package main

import (
	"fmt"

	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
)

// Refactor the main logic into a separate function (getFunc).
func getFunc(cmd *cobra.Command, path string, buckets []string, keyStr string, parseFormat string, format string) error {
	// validate the input parameters
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

	// check if the source DB path is valid
	if _, err := checkSourceDBPath(path); err != nil {
		return err
	}

	// open the database
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	// access the database and get the value
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
}

func newGetCommand() *cobra.Command {
	var parseFormat, format string

	cmd := &cobra.Command{
		Use:   "get PATH [BUCKET..] KEY",
		Short: "get the value of a key from a (sub)bucket in a bbolt database",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			// argument parsing
			path := args[0]
			buckets := args[1 : len(args)-1]
			keyStr := args[len(args)-1]

			// call the refactored getFunc
			return getFunc(cmd, path, buckets, keyStr, parseFormat, format)
		},
	}

	cmd.Flags().StringVar(&parseFormat, "parse-format", "ascii-encoded", "Input format. One of: ascii-encoded|hex")
	cmd.Flags().StringVar(&format, "format", "auto", "Output format. One of: ascii|hex|auto")

	return cmd
}

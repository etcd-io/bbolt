package main

import (
	"fmt"

	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/errors"
)

type getOptions struct {
	parseFormat string
	format      string
}

func newGetCommand() *cobra.Command {
	var opts getOptions

	cmd := &cobra.Command{
		Use:   "get PATH [BUCKET..] KEY",
		Short: "get the value of a key from a (sub)bucket in a bbolt database",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			if path == "" {
				return ErrPathRequired
			}
			buckets := args[1 : len(args)-1]
			keyStr := args[len(args)-1]

			// validate input parameters
			if len(buckets) == 0 {
				return fmt.Errorf("bucket is required: %w", ErrBucketRequired)
			}

			key, err := parseBytes(keyStr, opts.parseFormat)
			if err != nil {
				return err
			}

			if len(key) == 0 {
				return fmt.Errorf("key is required: %w", errors.ErrKeyRequired)
			}

			return getFunc(cmd, path, buckets, key, opts)
		},
	}

	cmd.Flags().StringVar(&opts.parseFormat, "parse-format", "ascii-encoded", "Input format one of: ascii-encoded|hex")
	cmd.Flags().StringVar(&opts.format, "format", "auto", "Output format one of: "+FORMAT_MODES+" (default: auto)")

	return cmd
}

// getFunc opens the BoltDB and retrieves the key value from the bucket path.
func getFunc(cmd *cobra.Command, path string, buckets []string, key []byte, opts getOptions) error {
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
			return fmt.Errorf("Error %w for key: %q hex: \"%x\"", ErrKeyNotFound, key, string(key))
		}
		return writelnBytes(cmd.OutOrStdout(), val, opts.format)
	})
}

package command

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

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
			if err := opts.Validate(args); err != nil {
				return err
			}
			return opts.Run(cmd, args)
		},
	}
	opts.AddFlags(cmd.Flags())

	return cmd
}

func (o *getOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.parseFormat, "parse-format", "ascii-encoded", "Input format one of: ascii-encoded|hex")
	fs.StringVar(&o.format, "format", "auto", "Output format one of: "+FORMAT_MODES+" (default: auto)")
}

func (o *getOptions) Validate(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("requires at least 3 arguments")
	}

	path := args[0]
	if path == "" {
		return ErrPathRequired
	}

	buckets := args[1 : len(args)-1]
	if len(buckets) == 0 {
		return fmt.Errorf("bucket is required: %w", ErrBucketRequired)
	}

	keyStr := args[len(args)-1]
	key, err := parseBytes(keyStr, o.parseFormat)
	if err != nil {
		return err
	}

	if len(key) == 0 {
		return fmt.Errorf("key is required: %w", errors.ErrKeyRequired)
	}

	return nil
}

func (o *getOptions) Run(cmd *cobra.Command, args []string) error {
	path := args[0]
	buckets := args[1 : len(args)-1]
	keyStr := args[len(args)-1]

	key, err := parseBytes(keyStr, o.parseFormat)
	if err != nil {
		return err
	}

	return getFunc(cmd, path, buckets, key, *o)
}

// getFunc opens the given bbolt db file and retrieves the key value from the bucket path.
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

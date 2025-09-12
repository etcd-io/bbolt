package command

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
)

type keysOptions struct {
	format string
}

func (o *keysOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&o.format, "format", "f", "auto", "Output format one of: "+FORMAT_MODES)
}

func newKeysCommand() *cobra.Command {
	var o keysOptions

	keysCmd := &cobra.Command{
		Use:   "keys <bbolt-file> <buckets>",
		Short: "print a list of keys in the given (sub)bucket in bbolt database",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return keysFunc(cmd, o, args[0], args[1:]...)
		},
	}

	o.AddFlags(keysCmd.Flags())
	return keysCmd
}

func keysFunc(cmd *cobra.Command, cfg keysOptions, dbPath string, buckets ...string) error {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}
	// Open database.
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		ReadOnly: true,
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
			return writelnBytes(cmd.OutOrStdout(), key, cfg.format)
		})
	})
}

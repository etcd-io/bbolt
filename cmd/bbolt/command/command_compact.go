package command

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
)

type compactOptions struct {
	dstPath   string
	txMaxSize int64
	dstNoSync bool
}

func newCompactCommand() *cobra.Command {
	var o compactOptions
	var compactCmd = &cobra.Command{
		Use:   "compact [options] -o <dst-bbolt-file> <src-bbolt-file>",
		Short: "creates a compacted copy of the database from source path to the destination path, preserving the original.",
		Long: `compact opens a database at source path and walks it recursively, copying keys
as they are found from all buckets, to a newly created database at the destination path.
The original database is left untouched.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(args[0]); err != nil {
				return err
			}
			return o.Run(cmd, args[0])
		},
	}
	o.AddFlags(compactCmd.Flags())

	return compactCmd
}

func (o *compactOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&o.dstPath, "output", "o", "", "")
	fs.Int64Var(&o.txMaxSize, "tx-max-size", 65536, "")
	fs.BoolVar(&o.dstNoSync, "no-sync", false, "")
	_ = cobra.MarkFlagRequired(fs, "output")
}

func (o *compactOptions) Validate(srcPath string) (err error) {
	if o.dstPath == "" {
		return errors.New("output file required")
	}

	return
}

func (o *compactOptions) Run(cmd *cobra.Command, srcPath string) (err error) {

	// ensure source file exists.
	fi, err := checkSourceDBPath(srcPath)
	if err != nil {
		return err
	}
	initialSize := fi.Size()

	// open source database.
	src, err := bolt.Open(srcPath, 0400, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer src.Close()

	// open destination database.
	dst, err := bolt.Open(o.dstPath, fi.Mode(), &bolt.Options{NoSync: o.dstNoSync})
	if err != nil {
		return err
	}
	defer dst.Close()

	// run compaction.
	if err := bolt.Compact(dst, src, o.txMaxSize); err != nil {
		return err
	}

	// report stats on new size.
	fi, err = os.Stat(o.dstPath)
	if err != nil {
		return err
	} else if fi.Size() == 0 {
		return fmt.Errorf("zero db size")
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%d -> %d bytes (gain=%.2fx)\n", initialSize, fi.Size(), float64(initialSize)/float64(fi.Size()))

	return
}

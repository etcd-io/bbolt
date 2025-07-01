package main

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
)

func newStatsCommand() *cobra.Command {
	statsCmd := &cobra.Command{
		Use:   "stats <bbolt-file>",
		Short: "print stats of bbolt database",
		Long: strings.TrimLeft(`
usage: bolt stats PATH

Stats performs an extensive search of the database to track every page
reference. It starts at the current meta page and recursively iterates
through every accessible bucket.

The following errors can be reported:

    already freed
        The page is referenced more than once in the freelist.

    unreachable unfreed
        The page is not referenced by a bucket or in the freelist.

    reachable freed
        The page is referenced by a bucket but is also in the freelist.

    out of bounds
        A page is referenced that is above the high water mark.

    multiple references
        A page is referenced by more than one other page.

    invalid type
        The page type is not "meta", "leaf", "branch", or "freelist".

No errors should occur in your database. However, if for some reason you
experience corruption, please submit a ticket to the etcd-io/bbolt project page:

  https://github.com/etcd-io/bbolt/issues
`, "\n"),
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			prefix := ""
			if len(args) > 1 {
				prefix = args[1]
			}

			return statsFunc(cmd, args[0], prefix)
		},
	}

	return statsCmd
}

func statsFunc(cmd *cobra.Command, dbPath string, prefix string) error {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}

	// open database.
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		var s bolt.BucketStats
		var count int
		if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if bytes.HasPrefix(name, []byte(prefix)) {
				s.Add(b.Stats())
				count += 1
			}
			return nil
		}); err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStdout(), "Aggregate statistics for %d buckets\n\n", count)

		fmt.Fprintln(cmd.OutOrStdout(), "Page count statistics")
		fmt.Fprintf(cmd.OutOrStdout(), "\tNumber of logical branch pages: %d\n", s.BranchPageN)
		fmt.Fprintf(cmd.OutOrStdout(), "\tNumber of physical branch overflow pages: %d\n", s.BranchOverflowN)
		fmt.Fprintf(cmd.OutOrStdout(), "\tNumber of logical leaf pages: %d\n", s.LeafPageN)
		fmt.Fprintf(cmd.OutOrStdout(), "\tNumber of physical leaf overflow pages: %d\n", s.LeafOverflowN)

		fmt.Fprintln(cmd.OutOrStdout(), "Tree statistics")
		fmt.Fprintf(cmd.OutOrStdout(), "\tNumber of keys/value pairs: %d\n", s.KeyN)
		fmt.Fprintf(cmd.OutOrStdout(), "\tNumber of levels in B+tree: %d\n", s.Depth)

		fmt.Fprintln(cmd.OutOrStdout(), "Page size utilization")
		fmt.Fprintf(cmd.OutOrStdout(), "\tBytes allocated for physical branch pages: %d\n", s.BranchAlloc)
		var percentage int
		if s.BranchAlloc != 0 {
			percentage = int(float32(s.BranchInuse) * 100.0 / float32(s.BranchAlloc))
		}
		fmt.Fprintf(cmd.OutOrStdout(), "\tBytes actually used for branch data: %d (%d%%)\n", s.BranchInuse, percentage)
		fmt.Fprintf(cmd.OutOrStdout(), "\tBytes allocated for physical leaf pages: %d\n", s.LeafAlloc)
		percentage = 0
		if s.LeafAlloc != 0 {
			percentage = int(float32(s.LeafInuse) * 100.0 / float32(s.LeafAlloc))
		}
		fmt.Fprintf(cmd.OutOrStdout(), "\tBytes actually used for leaf data: %d (%d%%)\n", s.LeafInuse, percentage)

		fmt.Fprintln(cmd.OutOrStdout(), "Bucket statistics")
		fmt.Fprintf(cmd.OutOrStdout(), "\tTotal number of buckets: %d\n", s.BucketN)
		percentage = 0
		if s.BucketN != 0 {
			percentage = int(float32(s.InlineBucketN) * 100.0 / float32(s.BucketN))
		}
		fmt.Fprintf(cmd.OutOrStdout(), "\tTotal number on inlined buckets: %d (%d%%)\n", s.InlineBucketN, percentage)
		percentage = 0
		if s.LeafInuse != 0 {
			percentage = int(float32(s.InlineBucketInuse) * 100.0 / float32(s.LeafInuse))
		}
		fmt.Fprintf(cmd.OutOrStdout(), "\tBytes used for inlined buckets: %d (%d%%)\n", s.InlineBucketInuse, percentage)

		return nil
	})
}

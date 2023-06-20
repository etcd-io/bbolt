package main

import (
	"flag"
	"fmt"
	"github.com/spf13/cobra"
	bolt "go.etcd.io/bbolt"
	"os"
)

func newCheckCobraCommand() *cobra.Command {
	checkCmd := &cobra.Command{
		Use:   "bolt info PATH",
		Short: "Info prints basic information about the Bolt database at PATH.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return PrintDbInfo(args)
		},
	}
	return checkCmd
}

func PrintDbInfo(args []string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(os.Stderr)
		return ErrUsage
	}

	path := fs.Arg(0)
	if path == "" {
		return ErrPathRequired
	} else if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	// Open the database.
	db, err := bolt.Open(path, 0666, &bolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer db.Close()

	// Print basic database info.
	info := db.Info()
	fmt.Fprintf(os.Stdout, "Page size: %d\n", info.PageSize)
	return nil
}

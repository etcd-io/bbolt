package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
)

func newInfoCobraCommand() *cobra.Command {
	infoCmd := &cobra.Command{
		Use:   "info PATH",
		Short: "Info prints basic information about the Bolt database at PATH.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("db file path not provided")
			}
			if len(args) > 1 {
				return errors.New("too many arguments, only accept db file path")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
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
		},
	}

	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	fs.Bool("h", false, "")

	infoCmd.Flags().AddFlagSet(fs)

	return infoCmd
}

package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
)

type PageError struct {
	ID  int
	Err error
}

func (e *PageError) Error() string {
	return fmt.Sprintf("page error: id=%d, err=%s", e.ID, e.Err)
}

func newPagesCommand() *cobra.Command {
	pagesCmd := &cobra.Command{
		Use:   "pages <bbolt-file>",
		Short: "print a list of pages in bbolt database",
		Long: strings.TrimLeft(`
Pages prints a table of pages with their type (meta, leaf, branch, freelist).
Leaf and branch pages will show a key count in the "items" column while the
freelist will show the number of free pages in the "items" column.

The "overflow" column shows the number of blocks that the page spills over
into. Normally there is no overflow but large keys and values can cause
a single page to take up multiple blocks.
`, "\n"),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return pagesFunc(cmd, args[0])
		},
	}

	return pagesCmd
}

func pagesFunc(cmd *cobra.Command, dbPath string) error {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}

	// Open database.
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Write header.
	fmt.Fprintln(cmd.OutOrStdout(), "ID       TYPE       ITEMS  OVRFLW")
	fmt.Fprintln(cmd.OutOrStdout(), "======== ========== ====== ======")

	return db.View(func(tx *bolt.Tx) error {
		var id int
		for {
			p, err := tx.Page(id)
			if err != nil {
				return &PageError{ID: id, Err: err}
			} else if p == nil {
				break
			}

			// Only display count and overflow if this is a non-free page.
			var count, overflow string
			if p.Type != "free" {
				count = strconv.Itoa(p.Count)
				if p.OverflowCount > 0 {
					overflow = strconv.Itoa(p.OverflowCount)
				}
			}

			// Print table row.
			fmt.Fprintf(cmd.OutOrStdout(), "%-8d %-10s %-6s %-6s\n", p.ID, p.Type, count, overflow)

			// Move to the next non-overflow page.
			id += 1
			if p.Type != "free" {
				id += p.OverflowCount
			}
		}
		return nil
	})
}

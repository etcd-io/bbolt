package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

// NewPageCommand creates a new Cobra command for the "page" subcommand.
func NewPageCommand() *cobra.Command {
	var all bool
	var formatValue string

	// Create a new Cobra command for "page".
	cmd := &cobra.Command{
		Use:   "page PATH pageid [pageid...]",
		Short: "Print page(s) from a Bolt DB",
		Long:  "Print one or more pages from the specified Bolt DB.",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate that PATH is provided.
			if len(args) < 1 {
				return fmt.Errorf("path to the database is required")
			}

			path := args[0]
			if _, err := os.Stat(path); os.IsNotExist(err) {
				return fmt.Errorf("file not found: %s", path)
			}

			// Handle the case when `--all` flag is passed.
			if all {
				return printAllPages(path, formatValue)
			}

			// Get page IDs from the arguments.
			pageIDs, err := stringToPages(args[1:])
			if err != nil {
				return err
			} else if len(pageIDs) == 0 {
				return fmt.Errorf("page ID(s) required")
			}

			return printPages(pageIDs, path, formatValue)
		},
	}

	// Define flags for the command.
	cmd.Flags().BoolVar(&all, "all", false, "list all pages (only skips pages that were considered successful overflow pages)")
	cmd.Flags().StringVar(&formatValue, "format-value", "auto", "One of: auto, hex, utf8 . Applies to values on the leaf page.")

	return cmd
}

func printPages(pageIDs []uint64, path string, formatValue string) error {
	// Print each page listed.
	for i, pageID := range pageIDs {
		// Print a separator.
		if i > 0 {
			fmt.Println("===============================================")
		}
		_, err := printPage(path, pageID, formatValue)
		if err != nil {
			fmt.Printf("Printing page %d failed: %s. Continuing...\n", pageID, err)
		}
	}
	return nil
}

func printAllPages(path string, formatValue string) error {
	_, hwm, err := guts_cli.ReadPageAndHWMSize(path)
	if err != nil {
		return fmt.Errorf("cannot read number of pages: %v", err)
	}

	// Print each page listed.
	for pageID := uint64(0); pageID < uint64(hwm); pageID++ {
		// Print a separator.
		if pageID > 0 {
			fmt.Println("===============================================")
		}
		overflow, err := printPage(path, pageID, formatValue)
		if err != nil {
			fmt.Printf("Printing page %d failed: %s. Continuing...\n", pageID, err)
		} else {
			pageID += uint64(overflow) + 1
		}
	}
	return nil
}

func printPage(path string, pageID uint64, formatValue string) (numPages uint32, reterr error) {
	defer func() {
		if err := recover(); err != nil {
			reterr = fmt.Errorf("%s", err)
		}
	}()

	// Retrieve page info and page size.
	p, buf, err := guts_cli.ReadPage(path, pageID)
	if err != nil {
		return 0, err
	}

	// Print basic page info.
	fmt.Printf("Page ID:    %d\n", p.Id())
	fmt.Printf("Page Type:  %s\n", p.Typ())
	fmt.Printf("Total Size: %d bytes\n", len(buf))
	fmt.Printf("Overflow pages: %d\n", p.Overflow())

	// Print type-specific data.
	switch p.Typ() {
	case "meta":
		err = PrintMeta(buf)
	case "leaf":
		err = PrintLeaf(buf, formatValue)
	case "branch":
		err = PrintBranch(buf)
	case "freelist":
		err = PrintFreelist(buf)
	}
	if err != nil {
		return 0, err
	}
	return p.Overflow(), nil
}

func PrintMeta(buf []byte) error {
	m := common.LoadPageMeta(buf)
	m.Print(os.Stdout)
	return nil
}

func PrintLeaf(buf []byte, formatValue string) error {
	p := common.LoadPage(buf)

	// Print number of items.
	fmt.Printf("Item Count: %d\n", p.Count())
	fmt.Println()

	// Print each key/value.
	for i := uint16(0); i < p.Count(); i++ {
		e := p.LeafPageElement(i)

		// Format key as string.
		var k string
		if isPrintable(string(e.Key())) {
			k = fmt.Sprintf("%q", string(e.Key()))
		} else {
			k = fmt.Sprintf("%x", string(e.Key()))
		}

		// Format value as string.
		var v string
		if e.IsBucketEntry() {
			b := e.Bucket()
			v = b.String()
		} else {
			var err error
			v, err = formatBytes(e.Value(), formatValue)
			if err != nil {
				return err
			}
		}

		fmt.Printf("%s: %s\n", k, v)
	}
	fmt.Println()
	return nil
}

func PrintBranch(buf []byte) error {
	p := common.LoadPage(buf)

	// Print number of items.
	fmt.Printf("Item Count: %d\n", p.Count())
	fmt.Println()

	// Print each key/value.
	for i := uint16(0); i < p.Count(); i++ {
		e := p.BranchPageElement(i)

		// Format key as string.
		var k string
		if isPrintable(string(e.Key())) {
			k = fmt.Sprintf("%q", string(e.Key()))
		} else {
			k = fmt.Sprintf("%x", string(e.Key()))
		}

		fmt.Printf("%s: <pgid=%d>\n", k, e.Pgid())
	}
	fmt.Println()
	return nil
}

func PrintFreelist(buf []byte) error {
	p := common.LoadPage(buf)

	// Print number of items.
	_, cnt := p.FreelistPageCount()
	fmt.Printf("Item Count: %d\n", cnt)
	fmt.Printf("Overflow: %d\n", p.Overflow())

	fmt.Println()

	// Print each page in the freelist.
	ids := p.FreelistPageIds()
	for _, id := range ids {
		fmt.Printf("%d\n", id)
	}
	fmt.Println()
	return nil
}

// Helper functions to parse pages and format values (unchanged)

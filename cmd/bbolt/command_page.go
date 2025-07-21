package main

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

type getPageOptions struct {
	all    bool
	format string
}

func newPageCommand() *cobra.Command {
	var opt getPageOptions
	pageCmd := &cobra.Command{
		Use:   "page <bbolt-file> [pageid...]",
		Short: "page prints one or more pages in human readable format.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dbPath := args[0]
			pageIDs, err := stringToPages(args[1:])
			if err != nil {
				return err
			}
			if len(pageIDs) == 0 && !opt.all {
				return ErrPageIDRequired
			}
			return pageFunc(cmd, opt, dbPath, pageIDs)
		},
	}
	opt.AddFlags(pageCmd.Flags())

	return pageCmd
}

func (o *getPageOptions) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.all, "all", false, "List all pages.")
	fs.StringVar(&o.format, "format-value", "auto", "Output format one of: "+FORMAT_MODES+". Applies to values on the leaf page.")
}

func pageFunc(cmd *cobra.Command, cfg getPageOptions, dbPath string, pageIDs []uint64) (err error) {
	if cfg.all && len(pageIDs) != 0 {
		return ErrInvalidPageArgs
	}

	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}

	if cfg.all {
		printAllPages(cmd, dbPath, cfg.format)
	} else {
		printPages(cmd, pageIDs, dbPath, cfg.format)
	}

	return
}

func printPages(cmd *cobra.Command, pageIDs []uint64, path string, formatValue string) {
	// print each page listed.
	for i, pageID := range pageIDs {
		// print a separator.
		if i > 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "===============================================")
		}
		_, pErr := printPage(cmd, path, pageID, formatValue)
		if pErr != nil {
			fmt.Fprintf(cmd.OutOrStdout(), "Prining page %d failed: %s. Continuing...\n", pageID, pErr)
		}
	}
}

// printPage prints given page to cmd.Stdout and returns error or number of interpreted pages.
func printPage(cmd *cobra.Command, path string, pageID uint64, formatValue string) (numPages uint32, reterr error) {
	defer func() {
		if err := recover(); err != nil {
			reterr = fmt.Errorf("%s", err)
		}
	}()

	// retrieve page info and page size.
	p, buf, err := guts_cli.ReadPage(path, pageID)
	if err != nil {
		return 0, err
	}

	// print basic page info.
	stdout := cmd.OutOrStdout()
	fmt.Fprintf(stdout, "Page ID:    %d\n", p.Id())
	fmt.Fprintf(stdout, "Page Type:  %s\n", p.Typ())
	fmt.Fprintf(stdout, "Total Size: %d bytes\n", len(buf))
	fmt.Fprintf(stdout, "Overflow pages: %d\n", p.Overflow())

	// print type-specific data.
	switch p.Typ() {
	case "meta":
		err = pagePrintMeta(stdout, buf)
	case "leaf":
		err = pagePrintLeaf(stdout, buf, formatValue)
	case "branch":
		err = pagePrintBranch(stdout, buf)
	case "freelist":
		err = pagePrintFreelist(stdout, buf)
	}
	if err != nil {
		return 0, err
	}
	return p.Overflow(), nil
}

func printAllPages(cmd *cobra.Command, path string, formatValue string) {
	_, hwm, err := guts_cli.ReadPageAndHWMSize(path)
	if err != nil {
		fmt.Fprintf(cmd.OutOrStdout(), "cannot read number of pages: %v", err)
	}

	// print each page listed.
	for pageID := uint64(0); pageID < uint64(hwm); {
		// print a separator.
		if pageID > 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "===============================================")
		}
		overflow, pErr := printPage(cmd, path, pageID, formatValue)
		if pErr != nil {
			fmt.Fprintf(cmd.OutOrStdout(), "Prining page %d failed: %s. Continuing...\n", pageID, pErr)
			pageID++
		} else {
			pageID += uint64(overflow) + 1
		}
	}
}

// pagePrintMeta prints the data from the meta page.
func pagePrintMeta(w io.Writer, buf []byte) error {
	m := common.LoadPageMeta(buf)
	m.Print(w)
	return nil
}

// pagePrintLeaf prints the data for a leaf page.
func pagePrintLeaf(w io.Writer, buf []byte, formatValue string) error {
	p := common.LoadPage(buf)

	// print number of items.
	fmt.Fprintf(w, "Item Count: %d\n", p.Count())
	fmt.Fprintf(w, "\n")

	// print each key/value.
	for i := uint16(0); i < p.Count(); i++ {
		e := p.LeafPageElement(i)

		// format key as string.
		var k string
		if isPrintable(string(e.Key())) {
			k = fmt.Sprintf("%q", string(e.Key()))
		} else {
			k = fmt.Sprintf("%x", string(e.Key()))
		}

		// format value as string.
		var v string
		var err error
		if e.IsBucketEntry() {
			b := e.Bucket()
			v = b.String()
		} else {
			v, err = formatBytes(e.Value(), formatValue)
			if err != nil {
				return err
			}
		}

		fmt.Fprintf(w, "%s: %s\n", k, v)
	}
	fmt.Fprintf(w, "\n")
	return nil
}

// pagePrintBranch prints the data for a leaf page.
func pagePrintBranch(w io.Writer, buf []byte) error {
	p := common.LoadPage(buf)

	// print number of items.
	fmt.Fprintf(w, "Item Count: %d\n", p.Count())
	fmt.Fprintf(w, "\n")

	// print each key/value.
	for i := uint16(0); i < p.Count(); i++ {
		e := p.BranchPageElement(i)

		// format key as string.
		var k string
		if isPrintable(string(e.Key())) {
			k = fmt.Sprintf("%q", string(e.Key()))
		} else {
			k = fmt.Sprintf("%x", string(e.Key()))
		}

		fmt.Fprintf(w, "%s: <pgid=%d>\n", k, e.Pgid())
	}
	fmt.Fprintf(w, "\n")
	return nil
}

// pagePrintFreelist prints the data for a freelist page.
func pagePrintFreelist(w io.Writer, buf []byte) error {
	p := common.LoadPage(buf)

	// print number of items.
	_, cnt := p.FreelistPageCount()
	fmt.Fprintf(w, "Item Count: %d\n", cnt)
	fmt.Fprintf(w, "Overflow: %d\n", p.Overflow())

	fmt.Fprintf(w, "\n")

	// print each page in the freelist.
	ids := p.FreelistPageIds()
	for _, ids := range ids {
		fmt.Fprintf(w, "%d\n", ids)
	}
	fmt.Fprintf(w, "\n")
	return nil
}

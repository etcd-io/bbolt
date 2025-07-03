package main

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

type pageItemOptions struct {
	keyOnly   bool
	valueOnly bool
	format    string
}

func newPageItemCommand() *cobra.Command {
	var opt pageItemOptions
	pageItemCmd := &cobra.Command{
		Use:   "page-item [options] <bbolt-file> pageid itemid",
		Short: "print a page item key and value in bbolt database",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return opt.Run(cmd, args)
		},
	}
	opt.AddFlags(pageItemCmd.Flags())

	return pageItemCmd
}

func (o *pageItemOptions) AddFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&o.keyOnly, "key-only", false, "Print only the key")
	fs.BoolVar(&o.valueOnly, "value-only", false, "Print only the value")
	fs.StringVar(&o.format, "format", "auto", "Output format one of: "+FORMAT_MODES)
}

func (o *pageItemOptions) Run(cmd *cobra.Command, args []string) (err error) {

	if o.keyOnly && o.valueOnly {
		return errors.New("the --key-only or --value-only flag may be set, but not both")
	}

	// Require database path and page id.
	path := args[0]
	if _, err := checkSourceDBPath(path); err != nil {
		return err
	}

	// Read page id.
	pageID, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return err
	}

	// Read item id.
	itemID, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		return err
	}

	// Open database.
	db, err := bolt.Open(path, 0600, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	// Retrieve page info and page size.
	_, buf, err := guts_cli.ReadPage(path, pageID)
	if err != nil {
		return err
	}

	if !o.valueOnly {
		err := pageItemPrintLeafItemKey(cmd.OutOrStdout(), buf, uint16(itemID), o.format)
		if err != nil {
			return err
		}
	}
	if !o.keyOnly {
		err := pageItemPrintLeafItemValue(cmd.OutOrStdout(), buf, uint16(itemID), o.format)
		if err != nil {
			return err
		}
	}

	return
}

func pageItemPrintLeafItemKey(w io.Writer, pageBytes []byte, index uint16, format string) error {
	k, _, err := pageItemLeafPageElement(pageBytes, index)
	if err != nil {
		return err
	}

	return writelnBytes(w, k, format)
}

func pageItemPrintLeafItemValue(w io.Writer, pageBytes []byte, index uint16, format string) error {
	_, v, err := pageItemLeafPageElement(pageBytes, index)
	if err != nil {
		return err
	}
	return writelnBytes(w, v, format)
}

func pageItemLeafPageElement(pageBytes []byte, index uint16) ([]byte, []byte, error) {
	p := common.LoadPage(pageBytes)
	if index >= p.Count() {
		return nil, nil, fmt.Errorf("leafPageElement: expected item index less than %d, but got %d", p.Count(), index)
	}
	if p.Typ() != "leaf" {
		return nil, nil, fmt.Errorf("leafPageElement: expected page type of 'leaf', but got '%s'", p.Typ())
	}

	e := p.LeafPageElement(index)
	return e.Key(), e.Value(), nil
}

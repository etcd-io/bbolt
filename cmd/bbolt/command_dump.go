package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"go.etcd.io/bbolt/internal/guts_cli"
)

func newDumpCommand() *cobra.Command {
	dumpCmd := &cobra.Command{
		Use:   "dump <bbolt-file> pageid [pageid...]",
		Short: "prints a hexadecimal dump of one or more pages of bbolt database.",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return dumpFunc(cmd, args)
		},
	}

	return dumpCmd
}

func dumpFunc(cmd *cobra.Command, args []string) (err error) {

	// Require database path and page id.
	path := args[0]
	if _, err := checkSourceDBPath(path); err != nil {
		return err
	}

	// Read page ids.
	pageIDs, err := stringToPages(args[1:])
	if err != nil {
		return err
	} else if len(pageIDs) == 0 {
		return ErrPageIDRequired
	}

	// Open database to retrieve page size.
	pageSize, _, err := guts_cli.ReadPageAndHWMSize(path)
	if err != nil {
		return err
	}

	// Open database file handler.
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Print each page listed.
	for i, pageID := range pageIDs {
		// Print a separator.
		if i > 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "===============================================")
		}

		// Print page to stdout.
		if err := dumpPrintPage(cmd.OutOrStdout(), f, pageID, uint64(pageSize)); err != nil {
			return err
		}
	}

	return
}

func dumpPrintPage(w io.Writer, r io.ReaderAt, pageID uint64, pageSize uint64) error {

	const bytesPerLineN = 16

	// Read page into buffer.
	buf := make([]byte, pageSize)
	addr := pageID * uint64(pageSize)
	if n, err := r.ReadAt(buf, int64(addr)); err != nil {
		return err
	} else if uint64(n) != pageSize {
		return io.ErrUnexpectedEOF
	}

	// Write out to writer in 16-byte lines.
	var prev []byte
	var skipped bool
	for offset := uint64(0); offset < pageSize; offset += bytesPerLineN {
		// Retrieve current 16-byte line.
		line := buf[offset : offset+bytesPerLineN]
		isLastLine := (offset == (pageSize - bytesPerLineN))

		// If it's the same as the previous line then print a skip.
		if bytes.Equal(line, prev) && !isLastLine {
			if !skipped {
				fmt.Fprintf(w, "%07x *\n", addr+offset)
				skipped = true
			}
		} else {
			// Print line as hexadecimal in 2-byte groups.
			fmt.Fprintf(w, "%07x %04x %04x %04x %04x %04x %04x %04x %04x\n", addr+offset,
				line[0:2], line[2:4], line[4:6], line[6:8],
				line[8:10], line[10:12], line[12:14], line[14:16],
			)

			skipped = false
		}

		// Save the previous line.
		prev = line
	}
	fmt.Fprint(w, "\n")

	return nil
}

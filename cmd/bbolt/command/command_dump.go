package command

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
			dbPath := args[0]
			pageIDs, err := stringToPages(args[1:])
			if err != nil {
				return err
			} else if len(pageIDs) == 0 {
				return ErrPageIDRequired
			}
			return dumpFunc(cmd, dbPath, pageIDs)
		},
	}

	return dumpCmd
}

func dumpFunc(cmd *cobra.Command, dbPath string, pageIDs []uint64) (err error) {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}

	// open database to retrieve page size.
	pageSize, _, err := guts_cli.ReadPageAndHWMSize(dbPath)
	if err != nil {
		return err
	}

	// open database file handler.
	f, err := os.Open(dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// print each page listed.
	for i, pageID := range pageIDs {
		// print a separator.
		if i > 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "===============================================")
		}

		// print page to stdout.
		if err := dumpPage(cmd.OutOrStdout(), f, pageID, uint64(pageSize)); err != nil {
			return err
		}
	}

	return
}

func dumpPage(w io.Writer, r io.ReaderAt, pageID uint64, pageSize uint64) error {
	const bytesPerLineN = 16

	// read page into buffer.
	buf := make([]byte, pageSize)
	addr := pageID * uint64(pageSize)
	if n, err := r.ReadAt(buf, int64(addr)); err != nil {
		return err
	} else if uint64(n) != pageSize {
		return io.ErrUnexpectedEOF
	}

	// write out to writer in 16-byte lines.
	var prev []byte
	var skipped bool
	for offset := uint64(0); offset < pageSize; offset += bytesPerLineN {
		// retrieve current 16-byte line.
		line := buf[offset : offset+bytesPerLineN]
		isLastLine := (offset == (pageSize - bytesPerLineN))

		// if it's the same as the previous line then print a skip.
		if bytes.Equal(line, prev) && !isLastLine {
			if !skipped {
				fmt.Fprintf(w, "%07x *\n", addr+offset)
				skipped = true
			}
		} else {
			// print line as hexadecimal in 2-byte groups.
			fmt.Fprintf(w, "%07x %04x %04x %04x %04x %04x %04x %04x %04x\n", addr+offset,
				line[0:2], line[2:4], line[4:6], line[6:8],
				line[8:10], line[10:12], line[12:14], line[14:16],
			)

			skipped = false
		}

		// save the previous line.
		prev = line
	}
	fmt.Fprint(w, "\n")

	return nil
}

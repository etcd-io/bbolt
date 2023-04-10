package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
	"go.etcd.io/bbolt/internal/surgeon"
)

var (
	ErrSurgeryFreelistAlreadyExist = errors.New("the file already has freelist, please consider to abandon the freelist to forcibly rebuild it")
)

var (
	surgeryTargetDBFilePath string
	surgeryPageId           uint64
	surgeryStartElementIdx  int
	surgeryEndElementIdx    int
)

func newSurgeryCobraCommand() *cobra.Command {
	surgeryCmd := &cobra.Command{
		Use:   "surgery <subcommand>",
		Short: "surgery related commands",
	}

	surgeryCmd.AddCommand(newSurgeryClearPageElementsCommand())
	surgeryCmd.AddCommand(newSurgeryFreelistCommand())

	return surgeryCmd
}

func newSurgeryClearPageElementsCommand() *cobra.Command {
	clearElementCmd := &cobra.Command{
		Use:   "clear-page-elements <bbolt-file> [options]",
		Short: "Clears elements from the given page, which can be a branch or leaf page",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("db file path not provided")
			}
			if len(args) > 1 {
				return errors.New("too many arguments")
			}
			return nil
		},
		RunE: surgeryClearPageElementFunc,
	}

	clearElementCmd.Flags().StringVar(&surgeryTargetDBFilePath, "output", "", "path to the target db file")
	clearElementCmd.Flags().Uint64VarP(&surgeryPageId, "pageId", "", 0, "page id")
	clearElementCmd.Flags().IntVarP(&surgeryStartElementIdx, "from-index", "", 0, "start element index (included) to clear, starting from 0")
	clearElementCmd.Flags().IntVarP(&surgeryEndElementIdx, "to-index", "", 0, "end element index (excluded) to clear, starting from 0, -1 means to the end of page")

	return clearElementCmd
}

func surgeryClearPageElementFunc(cmd *cobra.Command, args []string) error {
	srcDBPath := args[0]

	if err := common.CopyFile(srcDBPath, surgeryTargetDBFilePath); err != nil {
		return fmt.Errorf("[clear-page-element] copy file failed: %w", err)
	}

	if surgeryPageId < 2 {
		return fmt.Errorf("the pageId must be at least 2, but got %d", surgeryPageId)
	}

	needAbandonFreelist, err := surgeon.ClearPageElements(surgeryTargetDBFilePath, common.Pgid(surgeryPageId), surgeryStartElementIdx, surgeryEndElementIdx, false)
	if err != nil {
		return fmt.Errorf("clear-page-element command failed: %w", err)
	}

	if needAbandonFreelist {
		fmt.Fprintf(os.Stdout, "WARNING: The clearing has abandoned some pages that are not yet referenced from free list.\n")
		fmt.Fprintf(os.Stdout, "Please consider executing `./bbolt surgery abandon-freelist ...`\n")
	}

	fmt.Fprintf(os.Stdout, "All elements in [%d, %d) in page %d were cleared\n", surgeryStartElementIdx, surgeryEndElementIdx, surgeryPageId)
	return nil
}

// TODO(ahrtr): add `bbolt surgery freelist rebuild/check ...` commands,
// and move all `surgery freelist` commands into a separate file,
// e.g command_surgery_freelist.go.
func newSurgeryFreelistCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "freelist <subcommand>",
		Short: "freelist related surgery commands",
	}

	cmd.AddCommand(newSurgeryFreelistAbandonCommand())
	cmd.AddCommand(newSurgeryFreelistRebuildCommand())

	return cmd
}

func newSurgeryFreelistAbandonCommand() *cobra.Command {
	abandonFreelistCmd := &cobra.Command{
		Use:   "abandon <bbolt-file> [options]",
		Short: "Abandon the freelist from both meta pages",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("db file path not provided")
			}
			if len(args) > 1 {
				return errors.New("too many arguments")
			}
			return nil
		},
		RunE: surgeryFreelistAbandonFunc,
	}

	abandonFreelistCmd.Flags().StringVar(&surgeryTargetDBFilePath, "output", "", "path to the target db file")

	return abandonFreelistCmd
}

func surgeryFreelistAbandonFunc(cmd *cobra.Command, args []string) error {
	srcDBPath := args[0]

	if err := common.CopyFile(srcDBPath, surgeryTargetDBFilePath); err != nil {
		return fmt.Errorf("[freelist abandon] copy file failed: %w", err)
	}

	if err := surgeon.ClearFreelist(surgeryTargetDBFilePath); err != nil {
		return fmt.Errorf("abandom-freelist command failed: %w", err)
	}

	fmt.Fprintf(os.Stdout, "The freelist was abandoned in both meta pages.\nIt may cause some delay on next startup because bbolt needs to scan the whole db to reconstruct the free list.\n")
	return nil
}

func newSurgeryFreelistRebuildCommand() *cobra.Command {
	rebuildFreelistCmd := &cobra.Command{
		Use:   "rebuild <bbolt-file> [options]",
		Short: "Rebuild the freelist",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("db file path not provided")
			}
			if len(args) > 1 {
				return errors.New("too many arguments")
			}
			return nil
		},
		RunE: surgeryFreelistRebuildFunc,
	}

	rebuildFreelistCmd.Flags().StringVar(&surgeryTargetDBFilePath, "output", "", "path to the target db file")

	return rebuildFreelistCmd
}

func surgeryFreelistRebuildFunc(cmd *cobra.Command, args []string) error {
	srcDBPath := args[0]

	// Ensure source file exists.
	fi, err := os.Stat(srcDBPath)
	if os.IsNotExist(err) {
		return fmt.Errorf("source database file %q doesn't exist", srcDBPath)
	} else if err != nil {
		return fmt.Errorf("failed to open source database file %q: %v", srcDBPath, err)
	}

	if surgeryTargetDBFilePath == "" {
		return fmt.Errorf("output database path wasn't given, specify output database file path with --output option")
	}

	// make sure the freelist isn't present in the file.
	meta, err := readMetaPage(srcDBPath)
	if err != nil {
		return err
	}
	if meta.Freelist() != common.PgidNoFreelist {
		return ErrSurgeryFreelistAlreadyExist
	}

	if err := common.CopyFile(srcDBPath, surgeryTargetDBFilePath); err != nil {
		return fmt.Errorf("[freelist rebuild] copy file failed: %w", err)
	}

	// bboltDB automatically reconstruct & sync freelist in write mode.
	db, err := bolt.Open(surgeryTargetDBFilePath, fi.Mode(), &bolt.Options{NoFreelistSync: false})
	if err != nil {
		return fmt.Errorf("[freelist rebuild] open db file failed: %w", err)
	}
	err = db.Close()
	if err != nil {
		return fmt.Errorf("[freelist rebuild] close db file failed: %w", err)
	}

	fmt.Fprintf(os.Stdout, "The freelist was successfully rebuilt.\n")
	return nil
}

func readMetaPage(path string) (*common.Meta, error) {
	_, activeMetaPageId, err := guts_cli.GetRootPage(path)
	if err != nil {
		return nil, fmt.Errorf("read root page failed: %w", err)
	}
	_, buf, err := guts_cli.ReadPage(path, uint64(activeMetaPageId))
	if err != nil {
		return nil, fmt.Errorf("read active mage page failed: %w", err)
	}
	return common.LoadPageMeta(buf), nil
}

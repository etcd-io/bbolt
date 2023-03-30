package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/surgeon"
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

	if err := copyFile(srcDBPath, surgeryTargetDBFilePath); err != nil {
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

	if err := copyFile(srcDBPath, surgeryTargetDBFilePath); err != nil {
		return fmt.Errorf("[abandon-freelist] copy file failed: %w", err)
	}

	if err := surgeon.ClearFreelist(surgeryTargetDBFilePath); err != nil {
		return fmt.Errorf("abandom-freelist command failed: %w", err)
	}

	fmt.Fprintf(os.Stdout, "The freelist was abandoned in both meta pages.\nIt may cause some delay on next startup because bbolt needs to scan the whole db to reconstruct the free list.\n")
	return nil
}

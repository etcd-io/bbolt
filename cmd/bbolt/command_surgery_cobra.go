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
	cmd := &cobra.Command{
		Use:   "surgery <subcommand>",
		Short: "surgery related commands",
	}

	cmd.AddCommand(newSurgeryClearPageElementsCommand())

	return cmd
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
	clearElementCmd.Flags().IntVarP(&surgeryStartElementIdx, "from", "", 0, "start element index (included) to clear, starting from 0")
	clearElementCmd.Flags().IntVarP(&surgeryEndElementIdx, "to", "", 0, "end element index (excluded) to clear, starting from 0, -1 means to the end of page")

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

	if err := surgeon.ClearPageElements(surgeryTargetDBFilePath, common.Pgid(surgeryPageId), surgeryStartElementIdx, surgeryEndElementIdx); err != nil {
		return fmt.Errorf("clear-page-element command failed: %w", err)
	}

	fmt.Fprintf(os.Stdout, "All elements in [%d, %d) in page %d were cleared\n", surgeryStartElementIdx, surgeryEndElementIdx, surgeryPageId)
	return nil
}

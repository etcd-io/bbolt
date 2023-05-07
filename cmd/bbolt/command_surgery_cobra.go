package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
	"go.etcd.io/bbolt/internal/surgeon"
)

var (
	ErrSurgeryFreelistAlreadyExist = errors.New("the file already has freelist, please consider to abandon the freelist to forcibly rebuild it")
)

func newSurgeryCobraCommand() *cobra.Command {
	surgeryCmd := &cobra.Command{
		Use:   "surgery <subcommand>",
		Short: "surgery related commands",
	}

	surgeryCmd.AddCommand(newSurgeryRevertMetaPageCommand())
	surgeryCmd.AddCommand(newSurgeryCopyPageCommand())
	surgeryCmd.AddCommand(newSurgeryClearPageElementsCommand())
	surgeryCmd.AddCommand(newSurgeryFreelistCommand())

	return surgeryCmd
}

type surgeryBaseOptions struct {
	outputDBFilePath string
}

func (o *surgeryBaseOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.outputDBFilePath, "output", o.outputDBFilePath, "path to the filePath db file")
}

func (o *surgeryBaseOptions) Validate() error {
	if o.outputDBFilePath == "" {
		return fmt.Errorf("output database path wasn't given, specify output database file path with --output option")
	}
	return nil
}

func newSurgeryRevertMetaPageCommand() *cobra.Command {
	var o surgeryBaseOptions
	revertMetaPageCmd := &cobra.Command{
		Use:   "revert-meta-page <bbolt-file> [options]",
		Short: "Revert the meta page to revert the changes performed by the latest transaction",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("db file path not provided")
			}
			if len(args) > 1 {
				return errors.New("too many arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryRevertMetaPageFunc(args[0], o)
		},
	}
	o.AddFlags(revertMetaPageCmd.Flags())
	return revertMetaPageCmd
}

func surgeryRevertMetaPageFunc(srcDBPath string, cfg surgeryBaseOptions) error {
	if _, err := checkSourceDBPath(srcDBPath); err != nil {
		return err
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[revert-meta-page] copy file failed: %w", err)
	}

	if err := surgeon.RevertMetaPage(cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("revert-meta-page command failed: %w", err)
	}

	fmt.Fprintln(os.Stdout, "The meta page is reverted.")

	return nil
}

type surgeryCopyPageOptions struct {
	surgeryBaseOptions
	sourcePageId      uint64
	destinationPageId uint64
}

func (o *surgeryCopyPageOptions) AddFlags(fs *pflag.FlagSet) {
	o.surgeryBaseOptions.AddFlags(fs)
	fs.Uint64VarP(&o.sourcePageId, "from-page", "", o.sourcePageId, "source page Id")
	fs.Uint64VarP(&o.destinationPageId, "to-page", "", o.destinationPageId, "destination page Id")
}

func (o *surgeryCopyPageOptions) Validate() error {
	if err := o.surgeryBaseOptions.Validate(); err != nil {
		return err
	}
	if o.sourcePageId == o.destinationPageId {
		return fmt.Errorf("'--from-page' and '--to-page' have the same value: %d", o.sourcePageId)
	}
	return nil
}

func newSurgeryCopyPageCommand() *cobra.Command {
	var o surgeryCopyPageOptions
	copyPageCmd := &cobra.Command{
		Use:   "copy-page <bbolt-file> [options]",
		Short: "Copy page from the source page Id to the destination page Id",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return errors.New("db file path not provided")
			}
			if len(args) > 1 {
				return errors.New("too many arguments")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryCopyPageFunc(args[0], o)
		},
	}
	o.AddFlags(copyPageCmd.Flags())
	return copyPageCmd
}

func surgeryCopyPageFunc(srcDBPath string, cfg surgeryCopyPageOptions) error {
	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[copy-page] copy file failed: %w", err)
	}

	if err := surgeon.CopyPage(cfg.outputDBFilePath, common.Pgid(cfg.sourcePageId), common.Pgid(cfg.destinationPageId)); err != nil {
		return fmt.Errorf("copy-page command failed: %w", err)
	}

	meta, err := readMetaPage(srcDBPath)
	if err != nil {
		return err
	}
	if meta.IsFreelistPersisted() {
		fmt.Fprintf(os.Stdout, "WARNING: the free list might have changed.\n")
		fmt.Fprintf(os.Stdout, "Please consider executing `./bbolt surgery abandon-freelist ...`\n")
	}

	fmt.Fprintf(os.Stdout, "The page %d was successfully copied to page %d\n", cfg.sourcePageId, cfg.destinationPageId)
	return nil
}

type surgeryClearPageElementsOptions struct {
	surgeryBaseOptions
	pageId          uint64
	startElementIdx int
	endElementIdx   int
}

func (o *surgeryClearPageElementsOptions) AddFlags(fs *pflag.FlagSet) {
	o.surgeryBaseOptions.AddFlags(fs)
	fs.Uint64VarP(&o.pageId, "pageId", "", o.pageId, "page id")
	fs.IntVarP(&o.startElementIdx, "from-index", "", o.startElementIdx, "start element index (included) to clear, starting from 0")
	fs.IntVarP(&o.endElementIdx, "to-index", "", o.endElementIdx, "end element index (excluded) to clear, starting from 0, -1 means to the end of page")
}

func (o *surgeryClearPageElementsOptions) Validate() error {
	if err := o.surgeryBaseOptions.Validate(); err != nil {
		return err
	}
	if o.pageId < 2 {
		return fmt.Errorf("the pageId must be at least 2, but got %d", o.pageId)
	}
	return nil
}

func newSurgeryClearPageElementsCommand() *cobra.Command {
	var o surgeryClearPageElementsOptions
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
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryClearPageElementFunc(args[0], o)
		},
	}
	o.AddFlags(clearElementCmd.Flags())
	return clearElementCmd
}

func surgeryClearPageElementFunc(srcDBPath string, cfg surgeryClearPageElementsOptions) error {
	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[clear-page-element] copy file failed: %w", err)
	}

	needAbandonFreelist, err := surgeon.ClearPageElements(cfg.outputDBFilePath, common.Pgid(cfg.pageId), cfg.startElementIdx, cfg.endElementIdx, false)
	if err != nil {
		return fmt.Errorf("clear-page-element command failed: %w", err)
	}

	if needAbandonFreelist {
		fmt.Fprintf(os.Stdout, "WARNING: The clearing has abandoned some pages that are not yet referenced from free list.\n")
		fmt.Fprintf(os.Stdout, "Please consider executing `./bbolt surgery abandon-freelist ...`\n")
	}

	fmt.Fprintf(os.Stdout, "All elements in [%d, %d) in page %d were cleared\n", cfg.startElementIdx, cfg.endElementIdx, cfg.pageId)
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
	var o surgeryBaseOptions
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
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryFreelistAbandonFunc(args[0], o)
		},
	}
	o.AddFlags(abandonFreelistCmd.Flags())

	return abandonFreelistCmd
}

func surgeryFreelistAbandonFunc(srcDBPath string, cfg surgeryBaseOptions) error {
	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[freelist abandon] copy file failed: %w", err)
	}

	if err := surgeon.ClearFreelist(cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("abandom-freelist command failed: %w", err)
	}

	fmt.Fprintf(os.Stdout, "The freelist was abandoned in both meta pages.\nIt may cause some delay on next startup because bbolt needs to scan the whole db to reconstruct the free list.\n")
	return nil
}

func newSurgeryFreelistRebuildCommand() *cobra.Command {
	var o surgeryBaseOptions
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
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return surgeryFreelistRebuildFunc(args[0], o)
		},
	}
	o.AddFlags(rebuildFreelistCmd.Flags())

	return rebuildFreelistCmd
}

func surgeryFreelistRebuildFunc(srcDBPath string, cfg surgeryBaseOptions) error {
	// Ensure source file exists.
	fi, err := checkSourceDBPath(srcDBPath)
	if err != nil {
		return err
	}

	// make sure the freelist isn't present in the file.
	meta, err := readMetaPage(srcDBPath)
	if err != nil {
		return err
	}
	if meta.IsFreelistPersisted() {
		return ErrSurgeryFreelistAlreadyExist
	}

	if err := common.CopyFile(srcDBPath, cfg.outputDBFilePath); err != nil {
		return fmt.Errorf("[freelist rebuild] copy file failed: %w", err)
	}

	// bboltDB automatically reconstruct & sync freelist in write mode.
	db, err := bolt.Open(cfg.outputDBFilePath, fi.Mode(), &bolt.Options{NoFreelistSync: false})
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

func checkSourceDBPath(srcPath string) (os.FileInfo, error) {
	fi, err := os.Stat(srcPath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("source database file %q doesn't exist", srcPath)
	} else if err != nil {
		return nil, fmt.Errorf("failed to open source database file %q: %v", srcPath, err)
	}
	return fi, nil
}

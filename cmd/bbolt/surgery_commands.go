package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/surgeon"
)

// surgeryCommand represents the "surgery" command execution.
type surgeryCommand struct {
	baseCommand

	srcPath string
	dstPath string
}

// newSurgeryCommand returns a SurgeryCommand.
func newSurgeryCommand(m *Main) *surgeryCommand {
	c := &surgeryCommand{}
	c.baseCommand = m.baseCommand
	return c
}

// Run executes the `surgery` program.
func (cmd *surgeryCommand) Run(args ...string) error {
	// Require a command at the beginning.
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Execute command.
	switch args[0] {
	case "help":
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	case "copy-page":
		return newCopyPageCommand(cmd).Run(args[1:]...)
	case "clear-page":
		return newClearPageCommand(cmd).Run(args[1:]...)
	default:
		return ErrUnknownCommand
	}
}

func (cmd *surgeryCommand) parsePathsAndCopyFile(fs *flag.FlagSet) error {
	// Require database paths.
	cmd.srcPath = fs.Arg(0)
	if cmd.srcPath == "" {
		return ErrPathRequired
	}

	cmd.dstPath = fs.Arg(1)
	if cmd.dstPath == "" {
		return errors.New("output file required")
	}

	// Copy database from SrcPath to DstPath
	if err := common.CopyFile(cmd.srcPath, cmd.dstPath); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	return nil
}

// Usage returns the help message.
func (cmd *surgeryCommand) Usage() string {
	return strings.TrimLeft(`
Surgery is a command for performing low level update on bbolt databases.

Usage:

	bbolt surgery command [arguments]

The commands are:
    help                   print this screen
    clear-page             clear all elements at the given pageId
    copy-page              copy page from source pageId to target pageId
    revert-meta-page       revert the meta page change made by the last transaction

Use "bbolt surgery [command] -h" for more information about a command.
`, "\n")
}

// copyPageCommand represents the "surgery copy-page" command execution.
type copyPageCommand struct {
	*surgeryCommand
}

// newCopyPageCommand returns a copyPageCommand.
func newCopyPageCommand(m *surgeryCommand) *copyPageCommand {
	c := &copyPageCommand{}
	c.surgeryCommand = m
	return c
}

// Run executes the command.
func (cmd *copyPageCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	if err := cmd.parsePathsAndCopyFile(fs); err != nil {
		return fmt.Errorf("copyPageCommand failed to parse paths and copy file: %w", err)
	}

	// Read page id.
	srcPageId, err := strconv.ParseUint(fs.Arg(2), 10, 64)
	if err != nil {
		return err
	}
	dstPageId, err := strconv.ParseUint(fs.Arg(3), 10, 64)
	if err != nil {
		return err
	}

	// copy the page
	if err := surgeon.CopyPage(cmd.dstPath, common.Pgid(srcPageId), common.Pgid(dstPageId)); err != nil {
		return fmt.Errorf("copyPageCommand failed: %w", err)
	}

	fmt.Fprintf(cmd.Stdout, "The page %d was copied to page %d\n", srcPageId, dstPageId)
	return nil
}

// Usage returns the help message.
func (cmd *copyPageCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt surgery copy-page SRC DST srcPageId dstPageid

CopyPage copies the database file at SRC to a newly created database
file at DST. Afterwards, it copies the page at srcPageId to the page
at dstPageId in DST.

The original database is left untouched.
`, "\n")
}

// clearPageCommand represents the "surgery clear-page" command execution.
type clearPageCommand struct {
	*surgeryCommand
}

// newClearPageCommand returns a clearPageCommand.
func newClearPageCommand(m *surgeryCommand) *clearPageCommand {
	c := &clearPageCommand{}
	c.surgeryCommand = m
	return c
}

// Run executes the command.
func (cmd *clearPageCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	if err := cmd.parsePathsAndCopyFile(fs); err != nil {
		return fmt.Errorf("clearPageCommand failed to parse paths and copy file: %w", err)
	}

	// Read page id.
	pageId, err := strconv.ParseUint(fs.Arg(2), 10, 64)
	if err != nil {
		return err
	}

	needAbandonFreelist, err := surgeon.ClearPage(cmd.dstPath, common.Pgid(pageId))
	if err != nil {
		return fmt.Errorf("clearPageCommand failed: %w", err)
	}

	if needAbandonFreelist {
		fmt.Fprintf(os.Stdout, "WARNING: The clearing has abandoned some pages that are not yet referenced from free list.\n")
		fmt.Fprintf(os.Stdout, "Please consider executing `./bbolt surgery abandon-freelist ...`\n")
	}

	fmt.Fprintf(cmd.Stdout, "Page (%d) was cleared\n", pageId)
	return nil
}

// Usage returns the help message.
func (cmd *clearPageCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt surgery clear-page SRC DST pageId

ClearPage copies the database file at SRC to a newly created database
file at DST. Afterwards, it clears all elements in the page at pageId
in DST.

The original database is left untouched.
`, "\n")
}

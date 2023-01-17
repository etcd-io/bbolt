package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

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
	case "revert-meta-page":
		return newRevertMetaPageCommand(cmd).Run(args[1:]...)
	default:
		return ErrUnknownCommand
	}
}

// Usage returns the help message.
func (cmd *surgeryCommand) Usage() string {
	return strings.TrimLeft(`
Surgery is a command for performing low level update on bbolt databases.

Usage:

	bbolt surgery command [arguments]

The commands are:

    help                   print this screen
    revert-meta-page       revert the meta page change made by the last transaction

Use "bbolt surgery [command] -h" for more information about a command.
`, "\n")
}

// revertMetaPageCommand represents the "surgery revert-meta-page" command execution.
type revertMetaPageCommand struct {
	surgeryCommand
}

// newRevertMetaPageCommand returns a revertMetaPageCommand.
func newRevertMetaPageCommand(m *surgeryCommand) *revertMetaPageCommand {
	c := &revertMetaPageCommand{}
	c.surgeryCommand = *m
	return c
}

// Run executes the command.
func (cmd *revertMetaPageCommand) Run(args ...string) error {
	// Parse flags.
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	help := fs.Bool("h", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	} else if *help {
		fmt.Fprintln(cmd.Stderr, cmd.Usage())
		return ErrUsage
	}

	// Require database paths.
	cmd.srcPath = fs.Arg(0)
	if cmd.srcPath == "" {
		return ErrPathRequired
	}

	cmd.dstPath = fs.Arg(1)
	if cmd.dstPath == "" {
		return errors.New("output file required")
	}

	// Ensure source file exists.
	_, err := os.Stat(cmd.srcPath)
	if os.IsNotExist(err) {
		return ErrFileNotFound
	} else if err != nil {
		return err
	}

	// Ensure output file not exist.
	_, err = os.Stat(cmd.dstPath)
	if err == nil {
		return fmt.Errorf("output file %q already exists", cmd.dstPath)
	} else if !os.IsNotExist(err) {
		return err
	}

	// Copy database from SrcPath to DstPath
	if err := copyFile(cmd.srcPath, cmd.dstPath); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	// revert the meta page
	if err = surgeon.RevertMetaPage(cmd.dstPath); err != nil {
		return err
	}

	fmt.Fprintln(cmd.Stdout, "The meta page is reverted.")
	return nil
}

func copyFile(srcPath, dstPath string) error {
	srcDB, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source file %q: %w", srcPath, err)
	}
	defer srcDB.Close()
	dstDB, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %q: %w", dstPath, err)
	}
	defer dstDB.Close()
	written, err := io.Copy(dstDB, srcDB)
	if err != nil {
		return fmt.Errorf("failed to copy database file from %q to %q: %w", srcPath, dstPath, err)
	}

	srcFi, err := srcDB.Stat()
	if err != nil {
		return fmt.Errorf("failed to get source file info %q: %w", srcPath, err)
	}
	initialSize := srcFi.Size()
	if initialSize != written {
		return fmt.Errorf("the byte copied (%q: %d) isn't equal to the initial db size (%q: %d)", dstPath, written, srcPath, initialSize)
	}

	return nil
}

// Usage returns the help message.
func (cmd *revertMetaPageCommand) Usage() string {
	return strings.TrimLeft(`
usage: bolt surgery revert-meta-page -o DST SRC

RevertMetaPage copies the database file at SRC to a newly created database
file at DST. Afterwards, it reverts the meta page on the newly created
database at DST.

The original database is left untouched.
`, "\n")
}

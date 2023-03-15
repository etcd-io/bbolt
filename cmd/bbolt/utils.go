package main

import (
	"fmt"
	"io"
	"os"
)

func copyFile(srcPath, dstPath string) error {
	// Ensure source file exists.
	_, err := os.Stat(srcPath)
	if os.IsNotExist(err) {
		return ErrFileNotFound
	} else if err != nil {
		return err
	}

	// Ensure output file not exist.
	_, err = os.Stat(dstPath)
	if err == nil {
		return fmt.Errorf("output file %q already exists", dstPath)
	} else if !os.IsNotExist(err) {
		return err
	}

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

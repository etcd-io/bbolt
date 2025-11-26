package main

import (
	"fmt"
	"os"

	"go.etcd.io/bbolt/cmd/bbolt/command"
)

func main() {
	rootCmd := command.NewRootCommand()
	if err := rootCmd.Execute(); err != nil {
		if rootCmd.SilenceErrors {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
		os.Exit(1)
	}
}

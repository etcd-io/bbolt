package main

import (
	"fmt"
	"os"

	"go.etcd.io/bbolt/cmd/cli"
)

func main() {
	m := cli.NewMain()
	if err := m.Run(os.Args[1:]...); err == cli.ErrUsage {
		os.Exit(2)
	} else if err == cli.ErrUnknownCommand {
		cobraExecute()
	} else if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func cobraExecute() {
	rootCmd := cli.NewRootCommand()
	if err := rootCmd.Execute(); err != nil {
		if rootCmd.SilenceErrors {
			fmt.Fprintln(os.Stderr, "Error:", err)
			os.Exit(1)
		} else {
			os.Exit(1)
		}
	}
}

package command

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"

	"go.etcd.io/bbolt/version"
)

func versionOutput() string {
	out := fmt.Sprintf("bbolt Version: %s\n", version.Version)
	if version.Branch != "" {
		out += fmt.Sprintf("Git Branch: %s\n", version.Branch)
	}
	if version.Commit != "" {
		out += fmt.Sprintf("Git Commit: %s\n", version.Commit)
	}
	out += fmt.Sprintf("Go Version: %s\nGo OS/Arch: %s/%s\n",
		runtime.Version(), runtime.GOOS, runtime.GOARCH)
	return out
}

func newVersionCommand() *cobra.Command {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "print the current version of bbolt",
		Long:  "print the current version of bbolt",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Print(versionOutput())
		},
	}

	return versionCmd
}

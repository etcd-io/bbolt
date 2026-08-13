package version

var (
	// Version shows the last bbolt binary version released.
	Version = "1.5.0"

	// Branch and Commit are set via -ldflags at build time to identify
	// the git branch and commit the binary was built from.
	Branch = ""
	Commit = ""
)

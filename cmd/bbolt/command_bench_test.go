package main_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	main "go.etcd.io/bbolt/cmd/bbolt"
)

// Ensure the "bench" command runs and exits without errors
func TestBenchCommand_Run(t *testing.T) {
	tests := map[string]struct {
		args []string
	}{
		"no-args":    {},
		"100k count": {[]string{"--count", "100000"}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Run the command.
			buf := new(bytes.Buffer)
			rootCmd := main.NewRootCommand()
			rootCmd.SetArgs(append([]string{"bench"}, test.args...))
			rootCmd.SetOut(buf)
			if err := rootCmd.Execute(); err != nil {
				t.Fatal(err)
			}

			output, err := io.ReadAll(buf)
			if err != nil {
				t.Fatal(err)
			}
			stderr := string(output)

			if !strings.Contains(stderr, "starting write benchmark.") || !strings.Contains(stderr, "starting read benchmark.") {
				t.Fatal(fmt.Errorf("benchmark result does not contain read/write start output:\n%s", stderr))
			}

			if strings.Contains(stderr, "iter mismatch") {
				t.Fatal(fmt.Errorf("found iter mismatch in stdout:\n%s", stderr))
			}

			if !strings.Contains(stderr, "# Write") || !strings.Contains(stderr, "# Read") {
				t.Fatal(fmt.Errorf("benchmark result does not contain read/write output:\n%s", stderr))
			}
		})
	}
}

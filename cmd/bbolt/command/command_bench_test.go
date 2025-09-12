package command_test

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/cmd/bbolt/command"
)

type safeWriter struct {
	buf *bytes.Buffer
	mu  sync.Mutex
}

func (w *safeWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *safeWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func newSafeWriter() *safeWriter {
	return &safeWriter{buf: bytes.NewBufferString("")}
}

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
			rootCmd := command.NewRootCommand()

			outputWriter := newSafeWriter()
			rootCmd.SetOut(outputWriter)

			errorWriter := newSafeWriter()
			rootCmd.SetErr(errorWriter)

			args := append([]string{"bench"}, test.args...)
			rootCmd.SetArgs(args)

			err := rootCmd.Execute()
			require.NoError(t, err)

			outStr := outputWriter.String()
			errStr := errorWriter.String()

			if !strings.Contains(errStr, "starting write benchmark.") || !strings.Contains(errStr, "starting read benchmark.") {
				t.Fatal(fmt.Errorf("benchmark result does not contain read/write start output:\n%s", outStr))
			}

			if strings.Contains(errStr, "iter mismatch") {
				t.Fatal(fmt.Errorf("found iter mismatch in stdout:\n%s", outStr))
			}

			if !strings.Contains(outStr, "# Write") || !strings.Contains(outStr, "# Read") {
				t.Fatal(fmt.Errorf("benchmark result does not contain read/write output:\n%s", outStr))
			}
		})
	}
}

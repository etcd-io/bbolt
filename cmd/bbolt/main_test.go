package main_test

import (
	"bytes"
	crypto "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

func TestPageCommand_Run(t *testing.T) {
	db := btesting.MustCreateDBWithOption(t, &bolt.Options{PageSize: 4096})
	db.Close()

	defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

	exp := "Page ID:    0\n" +
		"Page Type:  meta\n" +
		"Total Size: 4096 bytes\n" +
		"Overflow pages: 0\n" +
		"Version:    2\n" +
		"Page Size:  4096 bytes\n" +
		"Flags:      00000000\n" +
		"Root:       <pgid=3>\n" +
		"Freelist:   <pgid=2>\n" +
		"HWM:        <pgid=4>\n" +
		"Txn ID:     0\n" +
		"Checksum:   07516e114689fdee\n\n"

	m := NewMain()
	err := m.Run("page", db.Path(), "0")
	require.NoError(t, err)
	if m.Stdout.String() != exp {
		t.Fatalf("unexpected stdout:\n%s\n%s", m.Stdout.String(), exp)
	}
}

// Ensure the "get" command can print the value of a key in a bucket.
func TestGetCommand_Run(t *testing.T) {
	testCases := []struct {
		name          string
		printable     bool
		testBucket    string
		testKey       string
		expectedValue string
	}{
		{
			name:          "printable data",
			printable:     true,
			testBucket:    "foo",
			testKey:       "foo-1",
			expectedValue: "val-foo-1\n",
		},
		{
			name:          "non printable data",
			printable:     false,
			testBucket:    "bar",
			testKey:       "100001",
			expectedValue: hex.EncodeToString(convertInt64IntoBytes(100001)) + "\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := btesting.MustCreateDB(t)

			if err := db.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucket([]byte(tc.testBucket))
				if err != nil {
					return err
				}
				if tc.printable {
					val := fmt.Sprintf("val-%s", tc.testKey)
					if err := b.Put([]byte(tc.testKey), []byte(val)); err != nil {
						return err
					}
				} else {
					if err := b.Put([]byte(tc.testKey), convertInt64IntoBytes(100001)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			db.Close()

			defer requireDBNoChange(t, dbData(t, db.Path()), db.Path())

			// Run the command.
			m := NewMain()
			if err := m.Run("get", db.Path(), tc.testBucket, tc.testKey); err != nil {
				t.Fatal(err)
			}
			actual := m.Stdout.String()
			assert.Equal(t, tc.expectedValue, actual)
		})
	}
}

// Ensure the "bench" command runs and exits without errors
func TestBenchCommand_Run(t *testing.T) {
	tests := map[string]struct {
		args []string
	}{
		"no-args":    {},
		"100k count": {[]string{"-count", "100000"}},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Run the command.
			m := NewMain()
			args := append([]string{"bench"}, test.args...)
			if err := m.Run(args...); err != nil {
				t.Fatal(err)
			}

			stderr := m.Stderr.String()
			stdout := m.Stdout.String()
			if !strings.Contains(stderr, "starting write benchmark.") || !strings.Contains(stderr, "starting read benchmark.") {
				t.Fatal(fmt.Errorf("benchmark result does not contain read/write start output:\n%s", stderr))
			}

			if strings.Contains(stderr, "iter mismatch") {
				t.Fatal(fmt.Errorf("found iter mismatch in stdout:\n%s", stderr))
			}

			if !strings.Contains(stdout, "# Write") || !strings.Contains(stdout, "# Read") {
				t.Fatal(fmt.Errorf("benchmark result does not contain read/write output:\n%s", stdout))
			}
		})
	}
}

type ConcurrentBuffer struct {
	m   sync.Mutex
	buf bytes.Buffer
}

func (b *ConcurrentBuffer) Read(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()

	return b.buf.Read(p)
}

func (b *ConcurrentBuffer) Write(p []byte) (n int, err error) {
	b.m.Lock()
	defer b.m.Unlock()

	return b.buf.Write(p)
}

func (b *ConcurrentBuffer) String() string {
	b.m.Lock()
	defer b.m.Unlock()

	return b.buf.String()
}

// Main represents a test wrapper for main.Main that records output.
type Main struct {
	*main.Main
	Stdin  ConcurrentBuffer
	Stdout ConcurrentBuffer
	Stderr ConcurrentBuffer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	m := &Main{Main: main.NewMain()}
	m.Main.Stdin = &m.Stdin
	m.Main.Stdout = &m.Stdout
	m.Main.Stderr = &m.Stderr
	return m
}

func TestCommands_Run_NoArgs(t *testing.T) {
	testCases := []struct {
		name   string
		cmd    string
		expErr error
	}{
		{
			name:   "get",
			cmd:    "get",
			expErr: main.ErrNotEnoughArgs,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMain()
			err := m.Run(tc.cmd)
			require.ErrorIs(t, err, main.ErrNotEnoughArgs)
		})
	}
}

func fillBucket(b *bolt.Bucket, prefix []byte) error {
	n := 10 + rand.Intn(50)
	for i := 0; i < n; i++ {
		v := make([]byte, 10*(1+rand.Intn(4)))
		_, err := crypto.Read(v)
		if err != nil {
			return err
		}
		k := append(prefix, []byte(fmt.Sprintf("k%d", i))...)
		if err := b.Put(k, v); err != nil {
			return err
		}
	}
	// limit depth of subbuckets
	s := 2 + rand.Intn(4)
	if len(prefix) > (2*s + 1) {
		return nil
	}
	n = 1 + rand.Intn(3)
	for i := 0; i < n; i++ {
		k := append(prefix, []byte(fmt.Sprintf("b%d", i))...)
		sb, err := b.CreateBucket(k)
		if err != nil {
			return err
		}
		if err := fillBucket(sb, append(k, '.')); err != nil {
			return err
		}
	}
	return nil
}

func chkdb(path string) ([]byte, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var buf bytes.Buffer
	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return walkBucket(b, name, nil, &buf)
		})
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func walkBucket(parent *bolt.Bucket, k []byte, v []byte, w io.Writer) error {
	if _, err := fmt.Fprintf(w, "%d:%x=%x\n", parent.Sequence(), k, v); err != nil {
		return err
	}

	// not a bucket, exit.
	if v != nil {
		return nil
	}
	return parent.ForEach(func(k, v []byte) error {
		if v == nil {
			return walkBucket(parent.Bucket(k), k, nil, w)
		}
		return walkBucket(parent, k, v, w)
	})
}

func dbData(t *testing.T, filePath string) []byte {
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)
	return data
}

func requireDBNoChange(t *testing.T, oldData []byte, filePath string) {
	newData, err := os.ReadFile(filePath)
	require.NoError(t, err)

	noChange := bytes.Equal(oldData, newData)
	require.True(t, noChange)
}

func convertInt64IntoBytes(num int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, num)
	return buf[:n]
}

func convertInt64KeysIntoHexString(nums ...int64) string {
	var res []string
	for _, num := range nums {
		res = append(res, hex.EncodeToString(convertInt64IntoBytes(num)))
	}
	return strings.Join(res, "\n") + "\n" // last newline char
}

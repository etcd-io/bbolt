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

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
)

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

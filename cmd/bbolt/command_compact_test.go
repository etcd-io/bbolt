package main_test

import (
	crypto "crypto/rand"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	bolt "go.etcd.io/bbolt"
	main "go.etcd.io/bbolt/cmd/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
)

// Ensure the "compact" command can print a list of buckets.
func TestCompactCommand_Run(t *testing.T) {
	dstdb := btesting.MustCreateDB(t)
	dstdb.Close()

	t.Log("Creating sample DB")
	db := btesting.MustCreateDB(t)
	if err := db.Update(func(tx *bolt.Tx) error {
		n := 2 + rand.Intn(5)
		for i := 0; i < n; i++ {
			k := []byte(fmt.Sprintf("b%d", i))
			b, err := tx.CreateBucketIfNotExists(k)
			if err != nil {
				return err
			}
			if err := b.SetSequence(uint64(i)); err != nil {
				return err
			}
			if err := fillBucket(b, append(k, '.')); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// make the db grow by adding large values, and delete them.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("large_vals"))
		if err != nil {
			return err
		}
		n := 5 + rand.Intn(5)
		for i := 0; i < n; i++ {
			v := make([]byte, 1000*1000*(1+rand.Intn(5)))
			_, err := crypto.Read(v)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(fmt.Sprintf("l%d", i)), v); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("large_vals")).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return tx.DeleteBucket([]byte("large_vals"))
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	dbChk, err := chkdb(db.Path())
	require.NoError(t, err)

	t.Log("Running compact cmd")
	rootCmd := main.NewRootCommand()
	rootCmd.SetArgs([]string{"compact", "-o", dstdb.Path(), db.Path()})
	err = rootCmd.Execute()
	require.NoError(t, err)

	t.Log("Checking output")
	dbChkAfterCompact, err := chkdb(db.Path())
	require.NoError(t, err)
	dstdbChk, err := chkdb(dstdb.Path())
	require.NoError(t, err)
	require.Equal(t, dbChk, dbChkAfterCompact, "the original db has been touched")
	require.Equal(t, dbChk, dstdbChk, "the compacted db data isn't the same than the original db")
}

func TestCompactCommand_NoArgs(t *testing.T) {
	expErr := errors.New("requires at least 1 arg(s), only received 0")
	rootCmd := main.NewRootCommand()
	rootCmd.SetArgs([]string{"compact"})
	err := rootCmd.Execute()
	require.ErrorContains(t, err, expErr.Error())
}

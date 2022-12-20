package tests_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/guts_cli"
	"go.etcd.io/bbolt/internal/surgeon"
	"testing"
)

func TestTx_RecursivelyCheckPages_MisplacedPage(t *testing.T) {
	db := btesting.MustCreateDB(t)
	assert.NoError(t,
		db.Fill([]byte("data"), 1, 10000,
			func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
			func(tx int, k int) []byte { return make([]byte, 100) },
		))
	assert.NoError(t, db.Close())

	navigator := surgeon.NewXRay(db.Path())

	path1, err := navigator.FindPathToPagesWithKey([]byte("0451"))
	assert.NoError(t, err, "Cannot find page that contains key:'0451'")
	assert.Len(t, path1, 1, "Expected only one page that contains key:'0451'")

	path2, err := navigator.FindPathToPagesWithKey([]byte("7563"))
	assert.NoError(t, err, "Cannot find page that contains key:'7563'")
	assert.Len(t, path2, 1, "Expected only one page that contains key:'7563'")

	srcPage := path1[0][len(path1[0])-1]
	targetPage := path2[0][len(path2[0])-1]
	assert.NoError(t, surgeon.CopyPage(db.Path(), srcPage, targetPage))

	db.MustReopen()
	assert.NoError(t, db.Update(func(tx *bolt.Tx) error {
		// Collect all the errors.
		var errors []error
		for err := range tx.Check(bolt.HexKeyValueStringer()) {
			errors = append(errors, err)
		}
		assert.Len(t, errors, 1)
		assert.ErrorContains(t, errors[0], fmt.Sprintf("leaf page(%v) needs to be >= to the key in the ancestor", targetPage))
		return nil
	}))
	assert.NoError(t, db.Close())
}

func TestTx_RecursivelyCheckPages_CorruptedLeaf(t *testing.T) {
	db := btesting.MustCreateDB(t)
	assert.NoError(t,
		db.Fill([]byte("data"), 1, 10000,
			func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
			func(tx int, k int) []byte { return make([]byte, 100) },
		))
	assert.NoError(t, db.Close())

	navigator := surgeon.NewXRay(db.Path())

	path1, err := navigator.FindPathToPagesWithKey([]byte("0451"))
	assert.NoError(t, err, "Cannot find page that contains key:'0451'")
	assert.Len(t, path1, 1, "Expected only one page that contains key:'0451'")

	srcPage := path1[0][len(path1[0])-1]
	p, pbuf, err := guts_cli.ReadPage(db.Path(), uint64(srcPage))
	assert.NoError(t, err)
	assert.Positive(t, p.Count(), "page must be not empty")
	p.LeafPageElement(p.Count() / 2).Key()[0] = 'z'
	assert.NoError(t, surgeon.WritePage(db.Path(), pbuf))

	db.MustReopen()
	assert.NoError(t, db.Update(func(tx *bolt.Tx) error {
		// Collect all the errors.
		var errors []error
		for err := range tx.Check(bolt.HexKeyValueStringer()) {
			errors = append(errors, err)
		}
		assert.Len(t, errors, 2)
		assert.ErrorContains(t, errors[0], fmt.Sprintf("leaf page(%v) needs to be < than key of the next element in ancestor", srcPage))
		assert.ErrorContains(t, errors[1], fmt.Sprintf("leaf page(%v) needs to be > (found <) than previous element", srcPage))
		return nil
	}))
	assert.NoError(t, db.Close())
}

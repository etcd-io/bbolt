package surgeon_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.etcd.io/bbolt/internal/btesting"
	"go.etcd.io/bbolt/internal/guts_cli"
	"go.etcd.io/bbolt/internal/surgeon"
)

func TestNavigator_FindPathToPagesWithKey(t *testing.T) {
	db := btesting.MustCreateDB(t)
	assert.NoError(t,
		db.Fill([]byte("data"), 1, 500,
			func(tx int, k int) []byte { return []byte(fmt.Sprintf("%04d", k)) },
			func(tx int, k int) []byte { return make([]byte, 100) },
		))
	assert.NoError(t, db.Close())

	navigator := surgeon.NewXRay(db.Path())
	path1, err := navigator.FindPathToPagesWithKey([]byte("0451"))
	assert.NoError(t, err)
	assert.NotEmpty(t, path1)

	page := path1[0][len(path1[0])-1]
	p, _, err := guts_cli.ReadPage(db.Path(), uint64(page))
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, []byte("0451"), p.LeafPageElement(0).Key())
	assert.LessOrEqual(t, []byte("0451"), p.LeafPageElement(p.Count()-1).Key())
}

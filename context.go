package bbolt

import (
	"go.etcd.io/bbolt/internal/common"
)

var (
	commitCtx = &commitContext{}
)

type commitContext struct {
	freePages []common.Pgid
}

func (c *commitContext) addFreePage(pgid common.Pgid) {
	c.freePages = append(c.freePages, pgid)
}

func (c *commitContext) commitFreePages(tx *Tx) {
	for _, pgid := range c.freePages {
		tx.db.freelist.Free(tx.meta.Txid(), tx.page(pgid))
	}

	c.freePages = c.freePages[:0]
}

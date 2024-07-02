package bbolt

import (
	"testing"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/freelist"
)

func TestTx_allocatePageStats(t *testing.T) {
	for n, f := range map[string]freelist.Interface{"hashmap": freelist.NewHashMapFreelist(), "array": freelist.NewArrayFreelist()} {
		t.Run(n, func(t *testing.T) {
			ids := []common.Pgid{2, 3}
			f.Init(ids)

			tx := &Tx{
				db: &DB{
					freelist: f,
					pageSize: common.DefaultPageSize,
				},
				meta:  &common.Meta{},
				pages: make(map[common.Pgid]*common.Page),
			}

			txStats := tx.Stats()
			prePageCnt := txStats.GetPageCount()
			allocateCnt := f.FreeCount()

			if _, err := tx.allocate(allocateCnt); err != nil {
				t.Fatal(err)
			}

			txStats = tx.Stats()
			if txStats.GetPageCount() != prePageCnt+int64(allocateCnt) {
				t.Errorf("Allocated %d but got %d page in stats", allocateCnt, txStats.GetPageCount())
			}
		})
	}
}

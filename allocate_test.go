package bbolt

import (
	"testing"
)

func TestTx_allocatePageStats(t *testing.T) {
	f := newTestFreelist()
	ids := []pgid{2, 3}
	f.readIDs(ids)

	tx := &Tx{
		db: &DB{
			freelist: f,
			pageSize: defaultPageSize,
		},
		meta:  &meta{},
		pages: make(map[pgid]*page),
	}

	txStats := tx.Stats()
	prePageCnt := txStats.GetPageCount()
	allocateCnt := f.free_count()

	if _, err := tx.allocate(allocateCnt); err != nil {
		t.Fatal(err)
	}

	txStats = tx.Stats()
	if txStats.GetPageCount() != prePageCnt+int64(allocateCnt) {
		t.Errorf("Allocated %d but got %d page in stats", allocateCnt, txStats.GetPageCount())
	}
}

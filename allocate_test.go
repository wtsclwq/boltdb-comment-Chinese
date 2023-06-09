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

	prePageCnt := tx.Stats().PageCount
	allocateCnt := f.freeCount()

	if _, err := tx.allocate(allocateCnt); err != nil {
		t.Fatal(err)
	}

	if tx.Stats().PageCount != prePageCnt+allocateCnt {
		t.Errorf("Allocated %d but got %d page in stats", allocateCnt, tx.Stats().PageCount)
	}
}

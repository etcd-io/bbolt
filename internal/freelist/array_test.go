package freelist

import (
	"reflect"
	"testing"

	"go.etcd.io/bbolt/internal/common"
)

// Ensure that a freelist can find contiguous blocks of pages.
func TestFreelistArray_allocate(t *testing.T) {
	f := NewArrayFreelist()
	ids := []common.Pgid{3, 4, 5, 6, 7, 9, 12, 13, 18}
	f.Init(ids)
	if id := int(f.Allocate(1, 3)); id != 3 {
		t.Fatalf("exp=3; got=%v", id)
	}
	if id := int(f.Allocate(1, 1)); id != 6 {
		t.Fatalf("exp=6; got=%v", id)
	}
	if id := int(f.Allocate(1, 3)); id != 0 {
		t.Fatalf("exp=0; got=%v", id)
	}
	if id := int(f.Allocate(1, 2)); id != 12 {
		t.Fatalf("exp=12; got=%v", id)
	}
	if id := int(f.Allocate(1, 1)); id != 7 {
		t.Fatalf("exp=7; got=%v", id)
	}
	if id := int(f.Allocate(1, 0)); id != 0 {
		t.Fatalf("exp=0; got=%v", id)
	}
	if id := int(f.Allocate(1, 0)); id != 0 {
		t.Fatalf("exp=0; got=%v", id)
	}
	if exp := common.Pgids([]common.Pgid{9, 18}); !reflect.DeepEqual(exp, f.freePageIds()) {
		t.Fatalf("exp=%v; got=%v", exp, f.freePageIds())
	}

	if id := int(f.Allocate(1, 1)); id != 9 {
		t.Fatalf("exp=9; got=%v", id)
	}
	if id := int(f.Allocate(1, 1)); id != 18 {
		t.Fatalf("exp=18; got=%v", id)
	}
	if id := int(f.Allocate(1, 1)); id != 0 {
		t.Fatalf("exp=0; got=%v", id)
	}
	if exp := common.Pgids([]common.Pgid{}); !reflect.DeepEqual(exp, f.freePageIds()) {
		t.Fatalf("exp=%v; got=%v", exp, f.freePageIds())
	}
}

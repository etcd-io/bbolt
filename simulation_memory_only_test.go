package bbolt_test

import (
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestSimulateMemoryOnly_1op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 1, 1)
}
func TestSimulateMemoryOnly_10op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 10, 1)
}
func TestSimulateMemoryOnly_100op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 100, 1)
}
func TestSimulateMemoryOnly_1000op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 1000, 1)
}
func TestSimulateMemoryOnly_10000op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 10000, 1)
}
func TestSimulateMemoryOnly_10op_10p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 10, 10)
}
func TestSimulateMemoryOnly_100op_10p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 100, 10)
}
func TestSimulateMemoryOnly_1000op_10p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 1000, 10)
}
func TestSimulateMemoryOnly_10000op_10p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 10000, 10)
}
func TestSimulateMemoryOnly_100op_100p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 100, 100)
}
func TestSimulateMemoryOnly_1000op_100p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 1000, 100)
}
func TestSimulateMemoryOnly_10000op_100p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 10000, 100)
}
func TestSimulateMemoryOnly_10000op_1000p(t *testing.T) {
	testSimulate(t, &bolt.Options{MemOnly: true}, 8, 10000, 1000)
}

// MemoryOnly + NoFreeListSync
func TestSimulateMemoryOnlyAndNoFreeListSync_1op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 1, 1)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_10op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 10, 1)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_100op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 100, 1)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_1000op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 1000, 1)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_10000op_1p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 10000, 1)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_10op_10p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 10, 10)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_100op_10p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 100, 10)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_1000op_10p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 1000, 10)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_10000op_10p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 10000, 10)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_100op_100p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 100, 100)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_1000op_100p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 1000, 100)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_10000op_100p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 10000, 100)
}
func TestSimulateMemoryOnlyAndNoFreeListSync_10000op_1000p(t *testing.T) {
	testSimulate(t, &bolt.Options{NoFreelistSync: true, MemOnly: true}, 8, 10000, 1000)
}
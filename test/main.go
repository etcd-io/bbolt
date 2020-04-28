package main

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

type freelist struct {
	forwardMap     map[pgid]uint64             // key is start pgid, value is its span size
}

type pgid uint64

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }


func (f *freelist) free_count() int {
	// use the forwardmap to get the total count
	count := 0
	for _, size := range f.forwardMap {
		count += int(size)
	}
	return count
}

func (f *freelist) hashmapGetFreePageIDs() []pgid {
	count := f.free_count()
	if count == 0 {
		return nil
	}

	m := make([]pgid, 0, count)
	for start, size := range f.forwardMap {
		//fmt.Printf("(%v, %v)\n", start, size)
		for i := 0; i < int(size); i++ {
			m = append(m, start+pgid(i))
		}
	}
	sort.Sort(pgids(m))

	return m
}

func (f *freelist) newHashmapGetFreePageIDs() []pgid {
	count := f.free_count()
	if count == 0 {
		return nil
	}

	m := make([]pgid, 0, count)

	keys := make([]pgid, 0, len(f.forwardMap))

	for k, _ := range f.forwardMap {
		keys = append(keys, k)
	}

	//fmt.Printf("keys:%v, len(keys)=%v\n", keys, len(keys))

	sort.Sort(pgids(keys))

	for _, start := range keys {
		size, ok := f.forwardMap[start]
		if (ok) {
			for i := 0; i < int(size); i++ {
				m = append(m, start+pgid(i))
			}
		}
	}

	return m
}

func test1() {
	N := int32(100)
	fm := make(map[pgid]uint64)
	i := int32(0)
	val := int32(0)
	for i = 0; i < N;  {
		val = rand.Int31n(1000)
		fm[pgid(i)] = uint64(val)
		i += val
	}

	f := freelist{
		forwardMap: fm,
	}
	start := time.Now()
	f.hashmapGetFreePageIDs()
	end := time.Now()
	fmt.Printf("origin time:%v\n", end.Sub(start))

	start = time.Now()
	res := f.newHashmapGetFreePageIDs()
	end = time.Now()
	fmt.Printf("new time:%v\n", end.Sub(start))


	if !sort.SliceIsSorted(res, func(i, j int) bool { return res[i] < res[j] }) {
		panic("pgids not sorted")
	}
}

func test2() {
	N := int32(10000)
	fm := make(map[pgid]uint64)
	i := int32(0)
	val := int32(10)
	for i = 0; i < N ;  {
		//val = rand.Int31n(1000)
		fm[pgid(i)] = uint64(val)
		i += val
	}

	f := freelist{
		forwardMap: fm,
	}
	start := time.Now()
	f.hashmapGetFreePageIDs()
	end := time.Now()
	fmt.Printf("origin time:%v\n", end.Sub(start))

	start = time.Now()
	res := f.newHashmapGetFreePageIDs()
	end = time.Now()
	fmt.Printf("new time:%v\n", end.Sub(start))


	if !sort.SliceIsSorted(res, func(i, j int) bool { return res[i] < res[j] }) {
		panic("pgids not sorted")
	}
}

func main() {
	test2()
}
package bbolt

import (
	"bytes"
	"fmt"
	"math/bits"
)

type bitmap struct {
	words  []uint64
	length int
}

func newBitmap() *bitmap {
	return &bitmap{}
}
func (bitmap *bitmap) Has(num int) bool {
	word, bit := num/64, uint(num%64)
	return word < len(bitmap.words) && (bitmap.words[word]&(1<<bit)) != 0
}

func (bitmap *bitmap) Add(num int) {
	word, bit := num/64, uint(num%64)
	for word >= len(bitmap.words) {
		bitmap.words = append(bitmap.words, 0)
	}
	// test if num is alreay in the bitmap
	if bitmap.words[word]&(1<<bit) == 0 {
		bitmap.words[word] |= 1 << bit
		bitmap.length++
	}
}

func (bitmap *bitmap) Len() int {
	return bitmap.length
}

// get the smallest number in this bitmap
// return -1 means not found
func (bitmap *bitmap) Get() int {
	for i, v := range bitmap.words {
		if v == 0 {
			continue
		}
		if j := getRightMostSetBit(v); j != -1 {
			return 64*i + int(j)
		}

	}

	return -1
}

// return -1 means not found
func getRightMostSetBit(n uint64) int {
	if x := bits.TrailingZeros64(n); x != 64 {
		return x
	}

	return -1
}

// Del a number in this map
func (bitmap *bitmap) Del(num int) {
	word, bit := num/64, uint(num%64)
	if word >= len(bitmap.words) {
		// do not exist for sure
		return
	}

	if bitmap.words[word]&(1<<bit) != 0 {
		bitmap.words[word] &= ^(1 << bit)
		bitmap.length--
	}

}

func (bitmap *bitmap) ToIndices() []int {
	var indices []int
	for i, v := range bitmap.words {
		if v == 0 {
			continue
		}
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				indices = append(indices, 64*i+int(j))
			}
		}
	}
	return indices
}

func (bitmap *bitmap) String() string {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, v := range bitmap.words {
		if v == 0 {
			continue
		}
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				if buf.Len() > len("{") {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%d", 64*uint(i)+j)
			}
		}
	}
	buf.WriteByte('}')
	fmt.Fprintf(&buf, "\nLength: %d\n", bitmap.length)
	return buf.String()
}

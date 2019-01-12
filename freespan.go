package bbolt

import (
	"fmt"
	"sort"
	"strconv"
)

const (
	freespanSizeBits  = 16
	freespanStartBits = 64 - freespanSizeBits

	freespanStartShift = freespanSizeBits
	freespanSizeMask   = 1<<freespanSizeBits - 1

	freespanMaxSize  = freespanSizeMask
	freespanMaxStart = 1<<freespanStartBits - 1

	freespanZero = freespan(0)
)

// A freespan represents a contiguous run of free pages.
// It is represented as a start pgid and a size.
// Start and size are packed into a single uint64 for space efficiency.
// Start is stored in the upper bits to enable searching and sorting
// to treat freespans are regular uint64s.
// Freespans with size 0 are allowed, although they are eliminated before storing them.
// Freespans with size > freespanMaxSize are represented as multiple spans.
// However, if multiple freespans are used to represent a single span,
// all but the last freespan must have size == freespanMaxSize.
// freespan(0) is never a legitimate freespan, since pgid 0 is a meta page,
// so it is sometimes used as a sentinel value to mean "no span".
type freespan uint64

// makeFreespan combines start and size into a freespan.
// The pgid/uint64 helps prevent accidental reversal of the arguments.
func makeFreespan(start pgid, size uint64) freespan {
	if start > freespanMaxStart {
		panic("makeFreespan start too large")
	}
	if size > freespanMaxSize {
		panic("makeFreespan size too large " + strconv.Itoa(int(start)) + " " + strconv.Itoa(int(size)))
	}
	return freespan(start)<<freespanStartShift + freespan(size)
}

// start returns the first pgid in s.
func (s freespan) start() pgid {
	return pgid(s >> freespanStartShift)
}

// size returns the number of pgids in s.
func (s freespan) size() uint64 {
	return uint64(s & freespanSizeMask)
}

// end returns the last pgid in s.
// It panics if s has size 0.
func (s freespan) end() pgid {
	sz := pgid(s.size())
	if sz == 0 {
		panic("size 0 spans have no end")
	}
	return s.start() + sz - 1
}

// next returns the pgid after the last pgid in s.
// Unlike end, it is well-defined for spans of size 0.
func (s freespan) next() pgid {
	return s.start() + pgid(s.size())
}

// contains reports whether s contains pg.
func (s freespan) contains(pg pgid) bool {
	return s.size() != 0 && uint64(pg-s.start()) < s.size()
}

// overlaps reports whether s overlaps t.
func (s freespan) overlaps(t freespan) bool {
	if t.start() < s.start() {
		s, t = t, s
	}
	return s.contains(t.start())
}

func (s freespan) String() string {
	if s.size() == 0 {
		return fmt.Sprintf("[%d,]", s.start())
	}
	return fmt.Sprintf("[%d,%d]", s.start(), s.end())
}

// append combines s with t.
// s must start before t, and they must not overlap.
// This can result in 0, 1, or 2 freespans.
// For example, if both s and t have length 0, then it will result in 0 freespans.
// If s and t are contiguous, or other has length 0, then it will usually result in 1 freespan.
// (If s and t are contiguous, but their combined length is > freespanMaxSize, it will result in 2 freespans.)
// If s and t have non-zero length, and are non-contiguous, it will result in 2 freespans.
// The lack of a freespan will be represented with a return value of 0.
// If there is exactly 1 freespan returned, u will be non-0 and v will be 0.
func (s freespan) append(t freespan) (u, v freespan) {
	if t.start() < s.start() {
		panic("freespan append out of order " + s.String() + " " + t.String())
	}
	if s.overlaps(t) {
		panic("freespan append overlaps " + s.String() + " " + t.String())
	}
	if s.size() == 0 {
		s = 0
	}
	if t.size() == 0 {
		t = 0
	}
	if s == 0 {
		return t, 0
	}
	if t == 0 {
		return s, 0
	}
	if s.end()+1 != t.start() {
		// Not contiguous.
		return s, t
	}
	// Contiguous.
	sz := s.size() + t.size()
	if sz > freespanMaxSize {
		// Size too large. Max out first retval, place remainder in second.
		first := makeFreespan(s.start(), freespanMaxSize)
		second := makeFreespan(s.start()+freespanMaxSize, sz-freespanMaxSize)
		return first, second
	}
	// Size fits. Collapse to single span.
	return makeFreespan(s.start(), sz), 0
}

// appendAll appends every individual pgid in s to ids and returns the result.
// It is expensive and should only be used in performance-insensitive code.
func (s freespan) appendAll(ids []pgid) []pgid {
	for i := s.start(); i < s.next(); i++ {
		ids = append(ids, i)
	}
	return ids
}

type freespans []freespan

func (a freespans) Len() int           { return len(a) }
func (a freespans) Less(i, j int) bool { return a[i] < a[j] }
func (a freespans) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (s freespans) contains(pg pgid) bool {
	pspan := makeFreespan(pg, 0)
	n := sort.Search(len(s), func(i int) bool { return s[i] > pspan })
	if n == len(s) {
		return false
	}
	return s[n].contains(pg)
}

// filter out zero size freespan
func filterZeroFreespan(spans []freespan) (dst []freespan) {
	dst = make([]freespan, 0, len(spans))
	for _, span := range spans {
		if span.size() != 0 {
			dst = append(dst, span)
		}
	}

	return dst
}

// filter out zero size freespan in place
func filterZeroFreespanInPlace(spans []freespan) []freespan {
	index := 0

	for _, span := range spans {
		if span.size() != 0 {
			spans[index] = span
			index++
		}
	}

	spans = spans[0:index]
	return spans
}

// mergenorm must be inpalce merge !!!
// which means dst can not be reassigned to other slice or freelist write will fail
func mergenorm(dst []freespan, all [][]freespan) []freespan {
	if dst == nil {
		n := 0
		for _, spans := range all {
			n += len(spans)
		}
		dst = make([]freespan, 0, n)
	}

	if len(all) == 0 {
		dst = filterZeroFreespanInPlace(dst)
		return dst
	}
	if !sort.SliceIsSorted(dst, func(i, j int) bool { return dst[i] < dst[j] }) {
		fmt.Println(dst)
		panic("dst not working")
	}
	//fmt.Println("dst is", dst)
	//fmt.Println("all is", all)

	for i := 0; i < len(all); i++ {
		// fmt.Println("all[i]", all[i])
		if !sort.SliceIsSorted(all[i], func(x, y int) bool { return all[i][x] < all[i][y] }) {
			fmt.Println(all[i])
			panic("all[i] not working")
		}
		dst = mergeTwoSortedSpan(dst, all[i])
	}
	//sort.Slice(dst, func(i, j int) bool { return dst[i] < dst[j] })
	if !sort.SliceIsSorted(dst, func(i, j int) bool { return dst[i] < dst[j] }) {
		panic("not working")
	}
	// Walk dst and merge the continous page.
	out := 0
	for i := 1; i < len(dst); i++ {
		u, v := dst[out].append(dst[i])
		if u == 0 {
			continue
		}
		if v == 0 {
			dst[out] = u
			continue
		}
		dst[out] = u
		dst[out+1] = v
		out++
	}
	if out < len(dst) && dst[out].size() != 0 {
		out++
	}
	dst = dst[:out]

	return dst
}

func mergeTwoSortedSpan(dst, b []freespan) []freespan {
	a := filterZeroFreespan(dst)
	b = filterZeroFreespan(b)
	dst = dst[:0]

	if len(a) == 0 {
		dst = append(dst, b...)
		return dst
	}
	if len(b) == 0 {
		dst = append(dst, a...)
		return dst
	}

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0].start() < a[0].start() {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i].start() > follow[0].start() })
		if len(dst) > 0 && lead[0].start() == dst[len(dst)-1].next() {
			// Combine spans.
			if lead[0].size()+dst[len(dst)-1].size() <= freespanMaxSize {
				dst[len(dst)-1] = makeFreespan(dst[len(dst)-1].start(), lead[0].size()+dst[len(dst)-1].size())
				lead = lead[1:]
				n--
			}
			// too large to merge, so do not merge
		}
		dst = append(dst, lead[:n]...)

		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	if follow[0].start() == dst[len(dst)-1].next() {
		// Combine spans.
		if follow[0].size()+dst[len(dst)-1].size() <= freespanMaxSize {
			dst[len(dst)-1] = makeFreespan(dst[len(dst)-1].start(), follow[0].size()+dst[len(dst)-1].size())
			follow = follow[1:]
		}

	}
	dst = append(dst, follow...)

	return dst
}

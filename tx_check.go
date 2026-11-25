package bbolt

import (
	"encoding/hex"
	"fmt"

	"go.etcd.io/bbolt/internal/common"
)

// Check performs several consistency checks on the database for this transaction.
// An error is returned if any inconsistency is found.
//
// It can be safely run concurrently on a writable transaction. However, this
// incurs a high cost for large databases and databases with a lot of subbuckets
// because of caching. This overhead can be removed if running on a read-only
// transaction, however, it is not safe to execute other writer transactions at
// the same time.
//
// It also allows users to provide a customized `KVStringer` implementation,
// so that bolt can generate human-readable diagnostic messages.
func (tx *Tx) Check(options ...CheckOption) <-chan error {
	chkConfig := checkConfig{
		kvStringer: HexKVStringer(),
	}
	for _, op := range options {
		op(&chkConfig)
	}

	ch := make(chan error)
	go func() {
		// Close the channel to signal completion.
		defer close(ch)
		tx.check(chkConfig, ch)
	}()
	return ch
}

func (tx *Tx) check(cfg checkConfig, ch chan error) {
	defer func() {
		if r := recover(); r != nil {
			ch <- panicked{r}
		}
	}()
	// Force loading free list if opened in ReadOnly mode.
	tx.db.loadFreelist()

	// Check if any pages are double freed.
	freed := make(map[common.Pgid]bool)
	all := make([]common.Pgid, tx.db.freelist.Count())
	tx.db.freelist.Copyall(all)
	for _, id := range all {
		if freed[id] {
			ch <- fmt.Errorf("page %d: already freed", id)
		}
		freed[id] = true
	}

	// Track every reachable page.
	reachable := make(map[common.Pgid]*common.Page)
	reachable[0] = tx.page(0) // meta0
	reachable[1] = tx.page(1) // meta1
	if tx.meta.Freelist() != common.PgidNoFreelist {
		for i := uint32(0); i <= tx.page(tx.meta.Freelist()).Overflow(); i++ {
			reachable[tx.meta.Freelist()+common.Pgid(i)] = tx.page(tx.meta.Freelist())
		}
	}

	if cfg.pageId == 0 {
		// Check the whole db file, starting from the root bucket and
		// recursively check all child buckets.
		tx.recursivelyCheckBucket(tx.root, reachable, freed, cfg.kvStringer, ch)

		// Ensure all pages below high water mark are either reachable or freed.
		for i := common.Pgid(0); i < tx.meta.Pgid(); i++ {
			_, isReachable := reachable[i]
			if !isReachable && !freed[i] {
				ch <- fmt.Errorf("page %d: unreachable unfreed", int(i))
			}
		}
	} else {
		// Check the db file starting from a specified pageId.
		if cfg.pageId < 2 || cfg.pageId >= uint64(tx.meta.Pgid()) {
			ch <- fmt.Errorf("page ID (%d) out of range [%d, %d)", cfg.pageId, 2, tx.meta.Pgid())
			return
		}

		tx.recursivelyCheckPage(common.Pgid(cfg.pageId), reachable, freed, cfg.kvStringer, ch)
	}
}

func (tx *Tx) recursivelyCheckPage(pageId common.Pgid, reachable map[common.Pgid]*common.Page, freed map[common.Pgid]bool,
	kvStringer KVStringer, ch chan error) {
	// Check the page and everything below it as if it was the root page
	// of a bucket, which is all that is needed to look up the buckets
	// that are found below it.
	inBkt := common.NewInBucket(pageId, 0)
	tmpBucket := &Bucket{
		InBucket:    &inBkt,
		FillPercent: DefaultFillPercent,
		tx:          tx,
	}
	tx.recursivelyCheckBucket(tmpBucket, reachable, freed, kvStringer, ch)
}

// recursivelyCheckBucket verifies the invariant properties of the given
// bucket and of all buckets nested within it. In a read-only transaction, it
// reads every page only once.
func (tx *Tx) recursivelyCheckBucket(b *Bucket, reachable map[common.Pgid]*common.Page, freed map[common.Pgid]bool,
	kvStringer KVStringer, ch chan error) {
	// Ignore inline buckets.
	if b.RootPage() == 0 {
		return
	}

	tx.recursivelyCheckBucketPage(b, b.RootPage(), nil, nil, nil, reachable, freed, kvStringer, ch)

	// The pages of a writable transaction may be out of date, a bucket
	// that was moved isn't found in the pages of its new parent yet. Look
	// up the nested buckets through the in-memory nodes instead.
	if tx.writable {
		_ = b.ForEachBucket(func(k []byte) error {
			if child := b.Bucket(k); child != nil {
				tx.recursivelyCheckBucket(child, reachable, freed, kvStringer, ch)
			}
			return nil
		})
	}
}

func verifyPageReachable(p *common.Page, hwm common.Pgid, stack []common.Pgid, reachable map[common.Pgid]*common.Page, freed map[common.Pgid]bool, ch chan error) {
	if p.Id() > hwm {
		ch <- fmt.Errorf("page %d: out of bounds: %d (stack: %v)", int(p.Id()), int(hwm), stack)
	}

	// Ensure each page is only referenced once.
	for i := common.Pgid(0); i <= common.Pgid(p.Overflow()); i++ {
		var id = p.Id() + i
		if _, ok := reachable[id]; ok {
			ch <- fmt.Errorf("page %d: multiple references (stack: %v)", int(id), stack)
		}
		reachable[id] = p
	}

	// We should only encounter un-freed leaf and branch pages.
	if freed[p.Id()] {
		ch <- fmt.Errorf("page %d: reachable freed", int(p.Id()))
	} else if !p.IsBranchPage() && !p.IsLeafPage() {
		ch <- fmt.Errorf("page %d: invalid type: %s (stack: %v)", int(p.Id()), p.Typ(), stack)
	}
}

// recursivelyCheckBucketPage verifies the subtree rooted at `pgId`, which is
// part of the bucket `b`:
//   - every page is reachable only once and is not freed.
//   - keys on pages must be sorted.
//   - keys on children pages are between 2 consecutive keys on the parent's branch page.
//   - all keys are >=`minKeyClosed` (can be nil) and <`maxKeyOpen` (can be nil).
//   - the same holds for every bucket nested within the subtree.
//
// `pagesStack` is expected to contain IDs of pages from the bucket's root to
// the parent of `pgId` for the clean debugging message.
func (tx *Tx) recursivelyCheckBucketPage(
	b *Bucket, pgId common.Pgid, minKeyClosed, maxKeyOpen []byte, pagesStack []common.Pgid,
	reachable map[common.Pgid]*common.Page, freed map[common.Pgid]bool,
	kvStringer KVStringer, ch chan error) (maxKeyInSubtree []byte) {

	p := tx.page(pgId)
	pagesStack = append(pagesStack, pgId)
	verifyPageReachable(p, tx.meta.Pgid(), pagesStack, reachable, freed, ch)

	keyToString := kvStringer.KeyToString
	switch {
	case p.IsBranchPage():
		// For branch page we navigate ranges of all subpages.
		runningMin := minKeyClosed
		for i := range p.BranchPageElements() {
			elem := p.BranchPageElement(uint16(i))
			verifyKeyOrder(elem.Pgid(), "branch", i, elem.Key(), runningMin, maxKeyOpen, ch, keyToString, pagesStack)

			maxKey := maxKeyOpen
			if i < len(p.BranchPageElements())-1 {
				maxKey = p.BranchPageElement(uint16(i + 1)).Key()
			}
			maxKeyInSubtree = tx.recursivelyCheckBucketPage(b, elem.Pgid(), elem.Key(), maxKey, pagesStack, reachable, freed, kvStringer, ch)
			runningMin = maxKeyInSubtree
		}
		return maxKeyInSubtree
	case p.IsLeafPage():
		runningMin := minKeyClosed
		for i := range p.LeafPageElements() {
			elem := p.LeafPageElement(uint16(i))
			verifyKeyOrder(pgId, "leaf", i, elem.Key(), runningMin, maxKeyOpen, ch, keyToString, pagesStack)
			runningMin = elem.Key()

			// Check the bucket nested under this key. In a writable
			// transaction, nested buckets are looked up by the caller.
			if elem.IsBucketEntry() && !tx.writable {
				if child := b.Bucket(elem.Key()); child != nil {
					tx.recursivelyCheckBucket(child, reachable, freed, kvStringer, ch)
				}
			}
		}
		if p.Count() > 0 {
			return p.LeafPageElement(p.Count() - 1).Key()
		}
	default:
		ch <- fmt.Errorf("unexpected page type (flags: %x) for pgId:%d", p.Flags(), pgId)
	}
	return maxKeyInSubtree
}

/***
 * verifyKeyOrder checks whether an entry with given #index on pgId (pageType: "branch|leaf") that has given "key",
 * is within range determined by (previousKey..maxKeyOpen) and reports found violations to the channel (ch).
 */
func verifyKeyOrder(pgId common.Pgid, pageType string, index int, key []byte, previousKey []byte, maxKeyOpen []byte, ch chan error, keyToString func([]byte) string, pagesStack []common.Pgid) {
	if index == 0 && previousKey != nil && compareKeys(previousKey, key) > 0 {
		ch <- fmt.Errorf("the first key[%d]=(hex)%s on %s page(%d) needs to be >= the key in the ancestor (%s). Stack: %v",
			index, keyToString(key), pageType, pgId, keyToString(previousKey), pagesStack)
	}
	if index > 0 {
		cmpRet := compareKeys(previousKey, key)
		if cmpRet > 0 {
			ch <- fmt.Errorf("key[%d]=(hex)%s on %s page(%d) needs to be > (found <) than previous element (hex)%s. Stack: %v",
				index, keyToString(key), pageType, pgId, keyToString(previousKey), pagesStack)
		}
		if cmpRet == 0 {
			ch <- fmt.Errorf("key[%d]=(hex)%s on %s page(%d) needs to be > (found =) than previous element (hex)%s. Stack: %v",
				index, keyToString(key), pageType, pgId, keyToString(previousKey), pagesStack)
		}
	}
	if maxKeyOpen != nil && compareKeys(key, maxKeyOpen) >= 0 {
		ch <- fmt.Errorf("key[%d]=(hex)%s on %s page(%d) needs to be < than key of the next element in ancestor (hex)%s. Pages stack: %v",
			index, keyToString(key), pageType, pgId, keyToString(previousKey), pagesStack)
	}
}

// ===========================================================================================

type checkConfig struct {
	kvStringer KVStringer
	pageId     uint64
}

type CheckOption func(options *checkConfig)

func WithKVStringer(kvStringer KVStringer) CheckOption {
	return func(c *checkConfig) {
		c.kvStringer = kvStringer
	}
}

// WithPageId sets a page ID from which the check command starts to check
func WithPageId(pageId uint64) CheckOption {
	return func(c *checkConfig) {
		c.pageId = pageId
	}
}

// KVStringer allows to prepare human-readable diagnostic messages.
type KVStringer interface {
	KeyToString([]byte) string
	ValueToString([]byte) string
}

// HexKVStringer serializes both key & value to hex representation.
func HexKVStringer() KVStringer {
	return hexKvStringer{}
}

type hexKvStringer struct{}

func (hexKvStringer) KeyToString(key []byte) string {
	return hex.EncodeToString(key)
}

func (hexKvStringer) ValueToString(value []byte) string {
	return hex.EncodeToString(value)
}

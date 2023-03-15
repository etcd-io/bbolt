package surgeon

import (
	"fmt"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func CopyPage(path string, srcPage common.Pgid, target common.Pgid) error {
	p1, d1, err1 := guts_cli.ReadPage(path, uint64(srcPage))
	if err1 != nil {
		return err1
	}
	p1.SetId(target)
	return guts_cli.WritePage(path, d1)
}

func ClearPage(path string, pgId common.Pgid) error {
	return ClearPageElements(path, pgId, 0, -1)
}

func ClearPageElements(path string, pgId common.Pgid, start, end int) error {
	// Read the page
	p, buf, err := guts_cli.ReadPage(path, uint64(pgId))
	if err != nil {
		return fmt.Errorf("ReadPage failed: %w", err)
	}

	if !p.IsLeafPage() && !p.IsBranchPage() {
		return fmt.Errorf("can't clear elements in %q page", p.Typ())
	}

	elementCnt := int(p.Count())

	if elementCnt == 0 {
		return nil
	}

	if start < 0 || start >= elementCnt {
		return fmt.Errorf("the start index (%d) is out of range [0, %d)", start, elementCnt)
	}

	if (end < 0 || end > elementCnt) && end != -1 {
		return fmt.Errorf("the end index (%d) is out of range [0, %d]", end, elementCnt)
	}

	if start > end && end != -1 {
		return fmt.Errorf("the start index (%d) is bigger than the end index (%d)", start, end)
	}

	if start == end {
		return fmt.Errorf("invalid: the start index (%d) is equal to the end index (%d)", start, end)
	}

	preOverflow := p.Overflow()

	if end == int(p.Count()) || end == -1 {
		p.SetCount(uint16(start))
		p.SetOverflow(0)
		if preOverflow != 0 || p.IsBranchPage() {
			if err := clearFreelist(path); err != nil {
				return err
			}
		}
	} else {
		inodes := common.ReadInodeFromPage(p)
		inodes = append(inodes[:start], inodes[end:]...)

		p.SetCount(uint16(len(inodes)))
		dataWritten := common.WriteInodeToPage(inodes, p)

		pageSize, _, err := guts_cli.ReadPageAndHWMSize(path)
		if err != nil {
			return fmt.Errorf("ReadPageAndHWMSize failed: %w", err)
		}
		if dataWritten%uint32(pageSize) == 0 {
			p.SetOverflow(dataWritten/uint32(pageSize) - 1)
		} else {
			p.SetOverflow(dataWritten / uint32(pageSize))
		}
	}

	if err := guts_cli.WritePage(path, buf); err != nil {
		return fmt.Errorf("WritePage failed: %w", err)
	}

	if preOverflow != p.Overflow() || p.IsBranchPage() {
		return clearFreelist(path)
	}

	return nil
}

func clearFreelist(path string) error {
	if err := clearFreelistAt(path, 0); err != nil {
		return fmt.Errorf("clearFreelist on meta page 0 failed: %w", err)
	}
	if err := clearFreelistAt(path, 1); err != nil {
		return fmt.Errorf("clearFreelist on meta page 1 failed: %w", err)
	}
	return nil
}

func clearFreelistAt(path string, pageId uint64) error {
	_, buf, err := guts_cli.ReadPage(path, pageId)
	if err != nil {
		return fmt.Errorf("ReadPage %d failed: %w", pageId, err)
	}

	meta := common.LoadPageMeta(buf)
	meta.SetFreelist(common.PgidNoFreelist)
	meta.SetChecksum(meta.Sum64())

	if err := guts_cli.WritePage(path, buf); err != nil {
		return fmt.Errorf("WritePage %d failed: %w", pageId, err)
	}

	return nil
}

// RevertMetaPage replaces the newer metadata page with the older.
// It usually means that one transaction is being lost. But frequently
// data corruption happens on the last transaction pages and the
// previous state is consistent.
func RevertMetaPage(path string) error {
	_, activeMetaPage, err := guts_cli.GetRootPage(path)
	if err != nil {
		return err
	}
	if activeMetaPage == 0 {
		return CopyPage(path, 1, 0)
	} else {
		return CopyPage(path, 0, 1)
	}
}

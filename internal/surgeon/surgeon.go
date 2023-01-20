package surgeon

import (
	"fmt"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func CopyPage(path string, srcPage guts_cli.Pgid, target guts_cli.Pgid) error {
	p1, d1, err1 := guts_cli.ReadPage(path, uint64(srcPage))
	if err1 != nil {
		return err1
	}
	p1.SetId(target)
	return guts_cli.WritePage(path, d1)
}

func ClearPage(path string, pgId guts_cli.Pgid) error {
	// Read the page
	p, buf, err := guts_cli.ReadPage(path, uint64(pgId))
	if err != nil {
		return fmt.Errorf("ReadPage failed: %w", err)
	}

	// Update and rewrite the page
	p.SetCount(0)
	p.SetOverflow(0)
	if err := guts_cli.WritePage(path, buf); err != nil {
		return fmt.Errorf("WritePage failed: %w", err)
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

package surgeon

import (
	"fmt"
	"os"

	"go.etcd.io/bbolt/internal/guts_cli"
)

func CopyPage(path string, srcPage guts_cli.Pgid, target guts_cli.Pgid) error {
	p1, d1, err1 := guts_cli.ReadPage(path, uint64(srcPage))
	if err1 != nil {
		return err1
	}
	p1.SetId(target)
	return WritePage(path, d1)
}

func WritePage(path string, pageBuf []byte) error {
	page := guts_cli.LoadPage(pageBuf)
	pageSize, _, err := guts_cli.ReadPageAndHWMSize(path)
	if err != nil {
		return err
	}
	if pageSize != uint64(len(pageBuf)) {
		return fmt.Errorf("WritePage: len(buf)=%d != pageSize=%d", len(pageBuf), pageSize)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteAt(pageBuf, int64(page.Id())*int64(pageSize))
	return err
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

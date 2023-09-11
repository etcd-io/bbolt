package main_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/common"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func readMetaPage(t *testing.T, path string) *common.Meta {
	_, activeMetaPageId, err := guts_cli.GetRootPage(path)
	require.NoError(t, err)
	_, buf, err := guts_cli.ReadPage(path, uint64(activeMetaPageId))
	require.NoError(t, err)
	return common.LoadPageMeta(buf)
}

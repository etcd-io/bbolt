package common_test

import (
	"math/rand/v2"
	"testing"

	"go.etcd.io/bbolt/internal/common"
)

func BenchmarkFastCheck(b *testing.B) {
	var (
		id    = common.Pgid(rand.Uint64())                    // Important, constant ID optimizes some code out.
		flags = uint16(common.BranchPageFlag) << rand.IntN(3) // From 0x01 to 0x04.
		p     = common.NewPage(id, flags, 0, 0)
	)
	for b.Loop() {
		p.FastCheck(id)
	}
}

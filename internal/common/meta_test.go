package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/errors"
)

func TestMeta_Validate(t *testing.T) {
	newMeta := func(pageSize uint32) *Meta {
		m := &Meta{}
		m.SetMagic(Magic)
		m.SetVersion(Version)
		m.SetPageSize(pageSize)
		m.SetChecksum(m.Sum64())
		return m
	}

	require.NoError(t, newMeta(4096).Validate())

	// A zero page size is rejected even when the checksum matches.
	require.ErrorIs(t, newMeta(0).Validate(), errors.ErrInvalid)
}

//go:build test
// +build test

package isaacstates

import (
	"github.com/imfact-labs/mitum2/base"
	isaacblock "github.com/imfact-labs/mitum2/isaac/block"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/valuehash"
)

func newTestBlockMap(
	height base.Height,
	previous, previousSuffrage util.Hash,
	local base.LocalNode,
	networkID base.NetworkID,
) (m isaacblock.BlockMap, _ error) {
	m = isaacblock.NewBlockMap()

	for _, i := range []base.BlockItemType{
		base.BlockItemProposal,
		base.BlockItemOperations,
		base.BlockItemOperationsTree,
		base.BlockItemStates,
		base.BlockItemStatesTree,
		base.BlockItemVoteproofs,
	} {
		if err := m.SetItem(newTestBlockMapItem(i)); err != nil {
			return m, err
		}
	}

	if previous == nil {
		previous = valuehash.RandomSHA256()
	}
	if height != base.GenesisHeight && previousSuffrage == nil {
		previousSuffrage = valuehash.RandomSHA256()
	}

	manifest := base.NewDummyManifest(height, valuehash.RandomSHA256())
	manifest.SetPrevious(previous)
	manifest.SetSuffrage(previousSuffrage)

	m.SetManifest(manifest)
	err := m.Sign(local.Address(), local.Privatekey(), networkID)

	return m, err
}

func newTestBlockMapItem(t base.BlockItemType) isaacblock.BlockMapItem {
	return isaacblock.NewBlockMapItem(t, util.UUID().String())
}

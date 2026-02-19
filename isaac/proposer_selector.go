package isaac

import (
	"context"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/util"
	"github.com/pkg/errors"
)

// ProposerSelectFunc selects proposer between suffrage nodes. If failed to
// request proposal from remotes, local will be proposer.
type ProposerSelectFunc func(context.Context, base.Point, []base.Node, util.Hash) (base.Node, error)

type FuncProposerSelector struct {
	selectfunc func(base.Point, []base.Node, util.Hash) (base.Node, error)
}

func (p FuncProposerSelector) Select(
	_ context.Context, point base.Point, nodes []base.Node, previousBlock util.Hash,
) (base.Node, error) {
	return p.selectfunc(point, nodes, previousBlock)
}

type BlockBasedProposerSelector struct{}

func NewBlockBasedProposerSelector() BlockBasedProposerSelector {
	return BlockBasedProposerSelector{}
}

func (BlockBasedProposerSelector) Select(
	_ context.Context, point base.Point, nodes []base.Node, previousBlock util.Hash,
) (base.Node, error) {
	switch n := len(nodes); {
	case n < 1:
		return nil, errors.Errorf("empty suffrage nodes")
	case n < 2:
		return nodes[0], nil
	}

	var sum uint64

	if previousBlock == nil {
		return nil, errors.Errorf("empty previous block")
	}

	for _, b := range previousBlock.Bytes() {
		sum += uint64(b)
	}

	sum += uint64(point.Height().Int64()) + point.Round().Uint64()

	return nodes[int(sum%uint64(len(nodes)))], nil
}

func NewFixedProposerSelector(
	selectfunc func(base.Point, []base.Node, util.Hash) (base.Node, error),
) FuncProposerSelector {
	return FuncProposerSelector{selectfunc: selectfunc}
}

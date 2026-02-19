package isaac

import (
	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/util/hint"
)

var NodeHint = hint.MustNewHint("node-v0.0.1")

func NewNode(pub base.Publickey, addr base.Address) base.BaseNode {
	return base.NewBaseNode(NodeHint, pub, addr)
}

type LocalNode struct {
	base.BaseLocalNode
}

func NewLocalNode(priv base.Privatekey, addr base.Address) LocalNode {
	return LocalNode{
		BaseLocalNode: base.NewBaseLocalNode(NodeHint, priv, addr),
	}
}

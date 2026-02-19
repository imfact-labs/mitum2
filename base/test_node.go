//go:build test
// +build test

package base

import "github.com/imfact-labs/mitum2/util/hint"

var DummyNodeHint = hint.MustNewHint("dummy-node-v0.0.1")

func RandomNode() BaseNode {
	return NewBaseNode(DummyNodeHint, NewMPrivatekey().Publickey(), RandomAddress("node-"))
}

func RandomLocalNode() BaseLocalNode {
	return NewBaseLocalNode(DummyNodeHint, NewMPrivatekey(), RandomAddress("local-"))
}

package isaacnetwork

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacstates "github.com/imfact-labs/mitum2/isaac/states"
	"github.com/imfact-labs/mitum2/network/quicmemberlist"
	"github.com/imfact-labs/mitum2/network/quicstream"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	jsonenc "github.com/imfact-labs/mitum2/util/encoder/json"
	"github.com/imfact-labs/mitum2/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testNodeInfo struct {
	suite.Suite
}

func TestNodeInfo(t *testing.T) {
	suite.Run(t, new(testNodeInfo))
}

func TestNodeInfoEncode(tt *testing.T) {
	t := new(encoder.BaseTestEncode)
	t.SetT(tt)

	enc := jsonenc.NewEncoder()

	hints := []encoder.DecodeDetail{
		{Hint: base.StringAddressHint, Instance: base.StringAddress{}},
		{Hint: base.MPublickeyHint, Instance: &base.MPublickey{}},
		{Hint: base.DummyNodeHint, Instance: base.BaseNode{}},
		{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}},
		{Hint: isaac.FixedSuffrageCandidateLimiterRuleHint, Instance: isaac.FixedSuffrageCandidateLimiterRule{}},
		{Hint: isaac.NetworkPolicyHint, Instance: isaac.NetworkPolicy{}},
		{Hint: isaac.ParamsHint, Instance: &isaac.Params{}},
		{Hint: isaac.MiscParamsHint, Instance: &isaac.MISCParams{}},
		{Hint: quicmemberlist.ParamsHint, Instance: &quicmemberlist.MemberlistParams{}},
		{Hint: NodeInfoHint, Instance: NodeInfo{}},
	}
	for i := range hints {
		t.NoError(enc.Add(hints[i]))
	}

	networkID := util.UUID().Bytes()

	t.Encode = func() (interface{}, []byte) {
		info := NewNodeInfoUpdater(networkID, base.RandomNode(), util.MustNewVersion("v1.2.3"))
		info.n.startedAt = time.Now().Add(((time.Hour + time.Nanosecond*33) * -1))
		info.SetLastManifest(base.NewDummyManifest(base.Height(33), valuehash.RandomSHA256()))
		info.SetSuffrageHeight(base.Height(44))
		info.SetNetworkPolicy(isaac.DefaultNetworkPolicy())
		info.SetIsaacParams(isaac.DefaultParams(networkID))
		info.SetMemberlistParams(quicmemberlist.DefaultMemberlistParams())
		info.SetMISCParams(isaac.DefaultMISCParams())

		ci, err := quicstream.NewConnInfoFromStringAddr("1.2.3.4:4321", true)
		t.NoError(err)
		info.SetConnInfo(ci.String())

		info.SetConsensusNodes([]base.Node{
			base.RandomNode(),
			base.RandomNode(),
			base.RandomNode(),
		})
		info.SetConsensusState(isaacstates.StateBroken)
		info.SetLastVote(base.NewStagePoint(base.RawPoint(33, 3), base.StageACCEPT), base.VoteResultMajority)

		n := info.NodeInfo()
		n.SetNetworkParams(json.RawMessage(`{"timeout_request":"11s"}`))

		b, err := enc.Marshal(n)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return n, b
	}
	t.Decode = func(b []byte) interface{} {
		hinter, err := enc.Decode(b)
		t.NoError(err)

		i, ok := hinter.(NodeInfo)
		t.True(ok)

		return i
	}
	t.Compare = func(a, b interface{}) {
		ah, ok := a.(NodeInfo)
		t.True(ok)
		bh, ok := b.(NodeInfo)
		t.True(ok)

		t.NoError(bh.IsValid(networkID))

		t.True(ah.Hint().Equal(bh.Hint()))
		t.True(ah.address.Equal(bh.address))
		t.True(ah.publickey.Equal(bh.publickey))
		t.Equal(ah.consensusState, bh.consensusState)
		base.EqualManifest(t.Assert(), ah.lastManifest, bh.lastManifest)
		t.Equal(ah.suffrageHeight, bh.suffrageHeight)
		base.EqualNetworkPolicy(t.Assert(), ah.networkPolicy, bh.networkPolicy)
		isaac.EqualLocalParams(t.Assert(), ah.isaacParams, bh.isaacParams)
		t.Equal(ah.connInfo, bh.connInfo)
		t.Equal(len(ah.consensusNodes), len(bh.consensusNodes))
		for i := range ah.consensusNodes {
			ac := ah.consensusNodes[i]
			bc := bh.consensusNodes[i]
			base.IsEqualNode(ac, bc)
		}

		t.Equal(ah.version, bh.version)
		t.True(util.TimeEqual(ah.startedAt, bh.startedAt))
		t.Equal(ah.lastVote, bh.lastVote)
		t.JSONEq(string(ah.NetworkParams()), string(bh.NetworkParams()))
	}

	suite.Run(tt, t)
}

package isaacstates

import (
	"testing"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testProposalUnavailableBallotbox struct {
	baseTestBallotbox
}

func (t *testProposalUnavailableBallotbox) previousACCEPTVoteproof(
	point base.Point,
	prev util.Hash,
	nodes []base.LocalNode,
) base.ACCEPTVoteproof {
	afact := isaac.NewACCEPTBallotFact(point.PrevHeight(), valuehash.RandomSHA256(), prev, nil)

	asfs := make([]base.BallotSignFact, len(nodes))
	for i := range nodes {
		sf := isaac.NewACCEPTBallotSignFact(afact)
		t.NoError(sf.NodeSign(nodes[i].Privatekey(), t.networkID, nodes[i].Address()))
		asfs[i] = sf
	}

	avp := isaac.NewACCEPTVoteproof(afact.Point().Point)
	avp.
		SetMajority(afact).
		SetSignFacts(asfs).
		SetThreshold(base.Threshold(100)).
		Finish()

	return avp
}

func (t *testProposalUnavailableBallotbox) initBallotWithFact(
	node base.LocalNode,
	fact base.INITBallotFact,
	vp base.Voteproof,
) isaac.INITBallot {
	sf := isaac.NewINITBallotSignFact(fact)
	t.NoError(sf.NodeSign(node.Privatekey(), t.networkID, node.Address()))

	return isaac.NewINITBallot(vp, sf, nil)
}

func (t *testProposalUnavailableBallotbox) newBox(suf base.Suffrage, th base.Threshold) *Ballotbox {
	return NewBallotbox(
		base.RandomAddress(""),
		func() base.Threshold { return th },
		func(base.Height) (base.Suffrage, bool, error) {
			return suf, true, nil
		},
	)
}

func (t *testProposalUnavailableBallotbox) voteFacts(
	point base.Point,
	prev util.Hash,
	nodes []base.LocalNode,
	facts []base.INITBallotFact,
) base.Voteproof {
	sufnodes := make([]base.Node, len(nodes))
	for i := range nodes {
		sufnodes[i] = nodes[i]
	}

	suf, err := isaac.NewSuffrage(sufnodes)
	t.NoError(err)

	th := base.DefaultThreshold
	previous := t.previousACCEPTVoteproof(point, prev, nodes)
	box := t.newBox(suf, th)
	box.SetLastPoint(mustNewLastPoint(previous.Point(), true, false))

	var out base.Voteproof
	for i := range facts {
		bl := t.initBallotWithFact(nodes[i], facts[i], previous)
		t.NoError(bl.IsValid(t.networkID))

		voted, vps, err := box.voteAndWait(bl)
		t.NoError(err)

		if len(vps) > 0 {
			out = vps[len(vps)-1]
		}
		if !voted {
			t.NotNil(out)

			continue
		}
	}

	t.NotNil(out)
	t.NoError(out.IsValid(t.networkID))
	t.Equal(point, out.Point().Point)
	t.Equal(th, out.Threshold())

	return out
}

func (t *testProposalUnavailableBallotbox) TestProposalUnavailableScatterDrawNoACCEPT() {
	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	_, nodes := isaac.NewTestSuffrage(4)

	facts := make([]base.INITBallotFact, len(nodes))
	for i := range nodes {
		facts[i] = isaac.NewProposalUnavailableINITBallotFact(point, prev)
	}

	vp := t.voteFacts(point, prev, nodes, facts)

	t.Equal(base.StageINIT, vp.Point().Stage())
	t.Equal(base.VoteResultDraw, vp.Result())
	t.Nil(vp.Majority())
	t.Nil(vp.(base.INITVoteproof).BallotMajority())
}

func (t *testProposalUnavailableBallotbox) TestProposalUnavailableMixedNormalMajorityWins() {
	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	proposal := valuehash.RandomSHA256()
	_, nodes := isaac.NewTestSuffrage(4)

	normal := isaac.NewINITBallotFact(point, prev, proposal, nil)
	facts := []base.INITBallotFact{
		normal,
		normal,
		normal,
		isaac.NewProposalUnavailableINITBallotFact(point, prev),
	}

	vp := t.voteFacts(point, prev, nodes, facts)

	t.Equal(base.VoteResultMajority, vp.Result())
	t.NotNil(vp.Majority())
	base.EqualBallotFact(t.Assert(), normal, vp.Majority())
	t.True(vp.Majority().(base.INITBallotFact).Proposal().Equal(proposal))
}

func (t *testProposalUnavailableBallotbox) TestProposalUnavailableMixedNormalNoQuorumDraw() {
	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	_, nodes := isaac.NewTestSuffrage(4)

	normal := isaac.NewINITBallotFact(point, prev, valuehash.RandomSHA256(), nil)
	facts := []base.INITBallotFact{
		normal,
		normal,
		isaac.NewProposalUnavailableINITBallotFact(point, prev),
		isaac.NewProposalUnavailableINITBallotFact(point, prev),
	}

	vp := t.voteFacts(point, prev, nodes, facts)

	t.Equal(base.VoteResultDraw, vp.Result())
	t.Nil(vp.Majority())
	t.Nil(vp.(base.INITVoteproof).BallotMajority())
}

func (t *testProposalUnavailableBallotbox) TestProposalUnavailableMixedEmptyProposalDraw() {
	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	proposal := valuehash.RandomSHA256()
	_, nodes := isaac.NewTestSuffrage(4)

	facts := []base.INITBallotFact{
		isaac.NewEmptyProposalINITBallotFact(point, prev, proposal),
		isaac.NewEmptyProposalINITBallotFact(point, prev, proposal),
		isaac.NewProposalUnavailableINITBallotFact(point, prev),
		isaac.NewProposalUnavailableINITBallotFact(point, prev),
	}

	vp := t.voteFacts(point, prev, nodes, facts)

	t.Equal(base.VoteResultDraw, vp.Result())
	t.Nil(vp.Majority())
	t.Nil(vp.(base.INITVoteproof).BallotMajority())
}

func (t *testProposalUnavailableBallotbox) TestMimicProposalUnavailableINITBallotRegeneratesScatter() {
	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	_, nodes := isaac.NewTestSuffrage(4)

	original := isaac.NewProposalUnavailableINITBallotFact(point, prev)
	previous := t.previousACCEPTVoteproof(point, prev, nodes)

	bl, err := mimicBallot(t.networkID, nodes[0], original, nil, previous)
	t.NoError(err)

	mimicked := bl.SignFact().Fact().(isaac.ProposalUnavailableINITBallotFact)
	t.False(original.Hash().Equal(mimicked.Hash()))
	t.True(original.Proposal().Equal(mimicked.Proposal()))
	t.True(original.PreviousBlock().Equal(mimicked.PreviousBlock()))
}

func TestProposalUnavailableBallotbox(t *testing.T) {
	suite.Run(t, new(testProposalUnavailableBallotbox))
}

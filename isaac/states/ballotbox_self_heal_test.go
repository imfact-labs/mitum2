package isaacstates

import (
	"testing"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	"github.com/imfact-labs/mitum2/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBallotboxSelfHeal struct {
	baseTestBallotbox
}

func (t *testBallotboxSelfHeal) TestDeferredINITBallotsRecountAfterSuffrageAppears() {
	suf, nodes := isaac.NewTestSuffrage(4)
	th := base.DefaultThreshold

	var suffrageReady bool

	box := NewBallotbox(
		base.RandomAddress(""),
		func() base.Threshold { return th },
		func(base.Height) (base.Suffrage, bool, error) {
			if !suffrageReady {
				return nil, false, nil
			}

			return suf, true, nil
		},
	)

	point := base.RawPoint(33, 0)
	prev := valuehash.RandomSHA256()
	proposal := valuehash.RandomSHA256()
	previous := t.initBallot(nodes[0], nodes, point, prev, proposal, nil, nil).Voteproof()
	box.SetLastPoint(mustNewLastPoint(previous.Point(), true, false))

	stagepoint := base.NewStagePoint(point, base.StageINIT)
	addresses := make([]base.Address, len(nodes))
	for i := range nodes {
		addresses[i] = nodes[i].Address()
	}

	var majorityFact base.BallotFact

	for i := range nodes[:3] {
		bl := t.initBallot(nodes[i], nodes, point, prev, proposal, nil, previous)
		t.NoError(bl.IsValid(t.networkID))
		if i == 0 {
			majorityFact = bl.SignFact().Fact().(base.BallotFact) //nolint:forcetypeassert // test fixture
		}

		voted, vps, err := box.voteAndWait(bl)
		t.NoError(err)
		t.True(voted)
		t.Empty(vps)
	}

	t.False(box.Count())
	t.Empty(box.Voted(stagepoint, addresses))

	select {
	case vp := <-box.Voteproof():
		t.Failf("unexpected voteproof before suffrage appears", "%v", vp)
	default:
	}

	suffrageReady = true

	t.True(box.Count())

	voted := box.Voted(stagepoint, addresses)
	t.Len(voted, 3)

	select {
	case vp := <-box.Voteproof():
		t.NoError(vp.IsValid(t.networkID))
		t.Equal(point, vp.Point().Point)
		t.Equal(base.StageINIT, vp.Point().Stage())
		t.Equal(base.VoteResultMajority, vp.Result())
		t.Equal(th, vp.Threshold())
		t.Len(vp.SignFacts(), 3)
		base.EqualBallotFact(t.Assert(), majorityFact, vp.Majority())
	case <-time.After(time.Second):
		t.Fail("failed to wait voteproof after suffrage appears")
	}
}

func TestBallotboxSelfHeal(t *testing.T) {
	suite.Run(t, new(testBallotboxSelfHeal))
}

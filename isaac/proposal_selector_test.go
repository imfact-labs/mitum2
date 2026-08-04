package isaac

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluele/gcache"
	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/util"
	jsonenc "github.com/imfact-labs/mitum2/util/encoder/json"
	"github.com/imfact-labs/mitum2/util/valuehash"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type dummyProposalPool struct {
	sync.RWMutex
	facthashs gcache.Cache
	points    gcache.Cache
}

func newDummyProposalPool(size int) *dummyProposalPool {
	return &dummyProposalPool{
		facthashs: gcache.New(size).LRU().Build(),
		points:    gcache.New(size).LRU().Build(),
	}
}

func (p *dummyProposalPool) Proposal(facthash util.Hash) (base.ProposalSignFact, bool, error) {
	p.RLock()
	defer p.RUnlock()

	return p.get(facthash.String())
}

func (p *dummyProposalPool) ProposalBytes(facthash util.Hash) (string, []byte, []byte, bool, error) {
	p.RLock()
	defer p.RUnlock()

	pr, found, err := p.get(facthash.String())
	if err != nil || !found {
		return "", nil, nil, false, err
	}

	b, err := util.MarshalJSON(pr)
	if err != nil {
		return "", nil, nil, false, err
	}

	return jsonenc.JSONEncoderHint.String(), nil, b, true, nil
}

func (p *dummyProposalPool) ProposalByPoint(point base.Point, proposer base.Address, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
	p.RLock()
	defer p.RUnlock()

	switch i, err := p.points.Get(p.pointkey(point, proposer, previousBlock)); {
	case errors.Is(err, gcache.KeyNotFoundError):
		return nil, false, nil
	case err != nil:
		return nil, false, nil
	case i == nil:
		return nil, false, nil
	default:
		return p.get(i.(string))
	}
}

func (p *dummyProposalPool) SetProposal(pr base.ProposalSignFact) (bool, error) {
	p.Lock()
	defer p.Unlock()

	facthash := pr.Fact().Hash().String()
	if p.facthashs.Has(facthash) {
		return false, nil
	}

	_ = p.facthashs.Set(facthash, pr)
	_ = p.points.Set(p.pointkey(pr.Point(), pr.ProposalFact().Proposer(), pr.ProposalFact().PreviousBlock()), facthash)

	return true, nil
}

func (p *dummyProposalPool) get(facthash string) (base.ProposalSignFact, bool, error) {
	switch i, err := p.facthashs.Get(facthash); {
	case errors.Is(err, gcache.KeyNotFoundError):
		return nil, false, nil
	case err != nil:
		return nil, false, nil
	case i == nil:
		return nil, false, nil
	default:
		return i.(base.ProposalSignFact), true, nil
	}
}

func (p *dummyProposalPool) pointkey(point base.Point, proposer base.Address, previousBlock util.Hash) string {
	return fmt.Sprintf("%d-%d-%s-%s",
		point.Height(),
		point.Round(),
		proposer.String(),
		previousBlock.String(),
	)
}

type testBaseProposalSelector struct {
	BaseTestBallots
}

func (t *testBaseProposalSelector) SetupTest() {
	t.BaseTestBallots.SetupTest()
}

func (t *testBaseProposalSelector) newNodes(n int, extra ...base.Node) []base.Node {
	nodes := make([]base.Node, n+1)

	for i := range nodes {
		nodes[i] = base.RandomLocalNode()
	}

	for i := range extra {
		nodes[n+i] = extra[i]
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Address().String() < nodes[j].Address().String()
	})

	return nodes
}

func (t *testBaseProposalSelector) newargs(nodes []base.Node) *BaseProposalSelectorArgs {
	args := NewBaseProposalSelectorArgs()
	args.Pool = newDummyProposalPool(10)
	proposalMaker := NewProposalMaker(
		t.Local,
		t.LocalParams.NetworkID(),
		func(context.Context, base.Height) ([][2]util.Hash, error) { return nil, nil },
		args.Pool,
		nil,
	)

	args.ProposerSelectFunc = NewBlockBasedProposerSelector().Select
	args.Maker = proposalMaker
	args.GetNodesFunc = func(base.Height) ([]base.Node, bool, error) {
		return nodes, len(nodes) > 0, nil
	}
	args.RequestFunc = func(ctx context.Context, point base.Point, proposer base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
		for i := range nodes {
			n := nodes[i]
			if !n.Address().Equal(proposer.Address()) {
				continue
			}

			pr, err := NewProposalMaker(
				n.(base.LocalNode),
				t.LocalParams.NetworkID(),
				func(context.Context, base.Height) ([][2]util.Hash, error) { return nil, nil },
				args.Pool,
				nil,
			).Make(ctx, point, previousBlock)

			return pr, err == nil, err
		}

		return nil, false, errors.Errorf("proposer not found in suffrage")
	}
	args.MinProposerWait = func() time.Duration {
		return time.Second * 3
	}

	return args
}

func (t *testBaseProposalSelector) TestNew() {
	nodes := t.newNodes(99, t.Local)

	args := t.newargs(nodes)

	p := NewBaseProposalSelector(t.Local, args)

	t.T().Logf("available nodes: %d", len(nodes))

	prev := valuehash.NewBytesFromString("5pjdLQojuwtQAN1FdEL1V1uvDdZi2koPJWLqJzo27yzBqn8WkNpXgypsF4VUBHwCtgduQw14N3sg7UHSjc6K2B25")
	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point, prev, 0)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.LocalParams.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())

	t.Run("different previous block", func() {
		prev = valuehash.NewBytesFromString("7bdd6043068e7026a1fd8592d5f457ffba1ae3bd0869d6f797db16548bc6bc87")
		apr, err := p.Select(context.Background(), point, prev, 0)
		t.NoError(err)
		t.NotNil(pr)

		t.NoError(pr.IsValid(t.LocalParams.NetworkID()))

		t.Equal(point, apr.Point())

		t.T().Logf("selected proposer: %q", apr.ProposalFact().Proposer())
		t.False(pr.ProposalFact().Proposer().Equal(apr.ProposalFact().Proposer()))
	})
}

func (t *testBaseProposalSelector) TestOneNode() {
	nodes := []base.Node{t.Local}

	args := t.newargs(nodes)
	p := NewBaseProposalSelector(t.Local, args)

	args.ProposerSelectFunc = NewBlockBasedProposerSelector().Select
	args.RequestFunc = func(_ context.Context, point base.Point, proposer base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
		return nil, false, errors.Errorf("proposer not found in suffrage")
	}

	t.T().Logf("available nodes: %d", len(nodes))

	prev := valuehash.RandomSHA512()
	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point, prev, 0)
	t.NoError(err)
	t.NotNil(pr)

	t.NoError(pr.IsValid(t.LocalParams.NetworkID()))

	t.Equal(point, pr.Point())

	t.T().Logf("selected proposer: %q", pr.ProposalFact().Proposer())
	t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
}

func (t *testBaseProposalSelector) TestUnknownSuffrage() {
	args := t.newargs(nil)

	p := NewBaseProposalSelector(t.Local, args)

	args.ProposerSelectFunc = NewBlockBasedProposerSelector().Select

	prev := valuehash.RandomSHA512()
	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point, prev, 0)
	t.Error(err)
	t.Nil(pr)
	t.ErrorContains(err, "nodes not found for height")
}

func (t *testBaseProposalSelector) TestFailedToReqeustByContext() {
	nodes := t.newNodes(3)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Address().String() < nodes[j].Address().String()
	})

	var requested []string

	args := t.newargs(nodes)

	p := NewBaseProposalSelector(t.Local, args)

	args.ProposerSelectFunc = NewFixedProposerSelector(
		func(base.Point, []base.Node, util.Hash) (base.Node, error) {
			return nodes[2], nil
		},
	).Select
	args.RequestFunc = func(ctx context.Context, point base.Point, proposer base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
		requested = append(requested, proposer.Address().String())

		if proposer.Address().Equal(nodes[2].Address()) {
			return nil, false, context.Canceled
		}

		for i := range nodes {
			n := nodes[i]
			if !n.Address().Equal(proposer.Address()) {
				continue
			}

			pr, err := NewProposalMaker(
				n.(base.LocalNode),
				t.LocalParams.NetworkID(),
				func(context.Context, base.Height) ([][2]util.Hash, error) { return nil, nil },
				args.Pool,
				nil,
			).Make(ctx, point, previousBlock)

			return pr, err == nil, err
		}

		return nil, false, errors.Errorf("proposer not found in suffrage")
	}

	t.T().Logf("available nodes: %d", len(nodes))
	for i := range nodes {
		t.T().Logf("available node: %d, %v", i, nodes[i].Address())
	}

	prev := valuehash.RandomSHA512()
	point := base.RawPoint(66, 11)
	pr, err := p.Select(context.Background(), point, prev, 0)
	t.Error(err)
	t.Nil(pr)
	t.ErrorIs(err, ErrProposalSelectionFailed)
	t.NotEmpty(requested)
	for i := range requested {
		t.Equal(nodes[2].Address().String(), requested[i])
	}
}

func (t *testBaseProposalSelector) TestAllFailedToReqeust() {
	nodes := t.newNodes(3)

	args := t.newargs(nodes)

	p := NewBaseProposalSelector(t.Local, args)

	args.ProposerSelectFunc = NewBlockBasedProposerSelector().Select
	args.RequestFunc = func(_ context.Context, point base.Point, proposer base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
		return nil, false, errors.Errorf("proposer not found in suffrage")
	}

	t.T().Logf("available nodes: %d", len(nodes))

	prev := valuehash.RandomSHA512()
	point := base.RawPoint(66, 11)

	t.Run("internal", func() {
		pr, err := p.selectInternal(context.Background(), point, prev, 0)
		t.Error(err)
		t.Nil(pr)
		t.ErrorIs(err, ErrProposalSelectionFailed)
	})

	t.Run("Select; no local fallback", func() {
		pr, err := p.Select(context.Background(), point, prev, 0)
		t.Error(err)
		t.Nil(pr)
		t.ErrorIs(err, ErrProposalSelectionFailed)
	})
}

func (t *testBaseProposalSelector) TestContextCanceled() {
	nodes := t.newNodes(3)

	args := t.newargs(nodes)

	requestdelay := time.Second
	args.TimeoutRequest = func() time.Duration { return time.Millisecond * 10 }

	p := NewBaseProposalSelector(t.Local, args)

	args.ProposerSelectFunc = NewBlockBasedProposerSelector().Select
	args.RequestFunc = func(ctx context.Context, point base.Point, proposer base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
		done := make(chan struct{}, 1)

		var pr base.ProposalSignFact
		var err error
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(requestdelay):
			}

			for i := range nodes {
				n := nodes[i]
				if !n.Address().Equal(proposer.Address()) {
					continue
				}

				pr, err = args.Maker.Make(ctx, point, previousBlock)

				done <- struct{}{}

				break
			}
		}()

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-done:
			switch {
			case err != nil:
				return nil, false, err
			case pr == nil:
				return nil, false, errors.Errorf("proposer not found in suffrage")
			default:
				return pr, true, nil
			}
		}
	}

	t.T().Logf("available nodes: %d", len(nodes))

	prev := valuehash.RandomSHA512()
	point := base.RawPoint(66, 11)

	t.Run("internal", func() {
		pr, err := p.selectInternal(context.Background(), point, prev, 0)
		t.Error(err)
		t.Nil(pr)
		t.ErrorIs(err, ErrProposalSelectionFailed)
	})

	t.Run("Select; no local fallback", func() {
		pr, err := p.Select(context.Background(), point, prev, 0)
		t.Error(err)
		t.Nil(pr)
		t.ErrorIs(err, ErrProposalSelectionFailed)
	})
}

func (t *testBaseProposalSelector) TestMainContextCanceled() {
	nodes := t.newNodes(3)

	args := t.newargs(nodes)

	requestdelay := time.Second * 10
	args.TimeoutRequest = func() time.Duration { return requestdelay * 2 }

	p := NewBaseProposalSelector(t.Local, args)

	args.ProposerSelectFunc = NewBlockBasedProposerSelector().Select
	args.RequestFunc = func(ctx context.Context, point base.Point, proposer base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
		done := make(chan struct{}, 1)

		var pr base.ProposalSignFact
		var err error
		go func() {
			<-time.After(requestdelay)

			for i := range nodes {
				n := nodes[i]
				if !n.Address().Equal(proposer.Address()) {
					continue
				}

				pr, err = args.Maker.Make(ctx, point, previousBlock)

				done <- struct{}{}

				break
			}
		}()

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-done:
			switch {
			case err != nil:
				return nil, false, err
			case pr == nil:
				return nil, false, errors.Errorf("proposer not found in suffrage")
			default:
				return pr, true, nil
			}
		}
	}

	t.T().Logf("available nodes: %d", len(nodes))

	ctx, cancel := context.WithCancel(context.Background())

	prev := valuehash.RandomSHA512()
	point := base.RawPoint(66, 11)

	t.Run("internal", func() {
		done := make(chan struct{}, 1)

		var pr base.ProposalSignFact
		var err error
		go func() {
			pr, err = p.selectInternal(ctx, point, prev, 0)

			done <- struct{}{}
		}()

		<-time.After(time.Millisecond * 300)
		cancel()

		<-done

		t.Nil(pr)
		t.ErrorIs(err, context.Canceled)
		t.ErrorContains(err, "context canceled")
	})

	t.Run("Select; main context canceled", func() {
		done := make(chan struct{}, 1)

		var pr base.ProposalSignFact
		var err error
		go func() {
			pr, err = p.Select(ctx, point, prev, 0)

			done <- struct{}{}
		}()

		<-time.After(time.Millisecond * 300)
		cancel()

		<-done

		t.Nil(pr)
		t.ErrorIs(err, context.Canceled)
	})
}

func (t *testBaseProposalSelector) TestSelectedLocalProposerUsesLocalMaker() {
	nodes := t.newNodes(2, t.Local)

	args := t.newargs(nodes)
	p := NewBaseProposalSelector(t.Local, args)

	var requested int64

	args.ProposerSelectFunc = NewFixedProposerSelector(
		func(base.Point, []base.Node, util.Hash) (base.Node, error) {
			return t.Local, nil
		},
	).Select
	args.RequestFunc = func(context.Context, base.Point, base.Node, util.Hash) (base.ProposalSignFact, bool, error) {
		atomic.AddInt64(&requested, 1)

		return nil, false, errors.Errorf("request should not be called for local proposer")
	}

	prev := valuehash.RandomSHA512()
	point := base.RawPoint(66, 11)

	pr, err := p.Select(context.Background(), point, prev, time.Second)
	t.NoError(err)
	t.NotNil(pr)
	t.True(t.Local.Address().Equal(pr.ProposalFact().Proposer()))
	t.Equal(int64(0), atomic.LoadInt64(&requested))
}

func (t *testBaseProposalSelector) TestRemoteRequestWaitBudgetTimeoutIsProposalSelectionFailed() {
	nodes := t.newNodes(2)
	args := t.newargs(nodes)
	args.MinProposerWait = func() time.Duration { return time.Millisecond * 20 }
	args.TimeoutRequest = func() time.Duration { return time.Second }

	remote := nodes[1]
	args.ProposerSelectFunc = NewFixedProposerSelector(
		func(base.Point, []base.Node, util.Hash) (base.Node, error) {
			return remote, nil
		},
	).Select
	args.RequestFunc = func(ctx context.Context, _ base.Point, _ base.Node, _ util.Hash) (base.ProposalSignFact, bool, error) {
		<-ctx.Done()

		return nil, false, ctx.Err()
	}

	p := NewBaseProposalSelector(t.Local, args)

	pr, err := p.Select(context.Background(), base.RawPoint(66, 11), valuehash.RandomSHA512(), time.Millisecond*20)
	t.Nil(pr)
	t.ErrorIs(err, ErrProposalSelectionFailed)
}

func (t *testBaseProposalSelector) TestLocalProposerWaitBudgetTimeoutIsProposalSelectionFailed() {
	nodes := t.newNodes(2, t.Local)
	args := t.newargs(nodes)
	args.MinProposerWait = func() time.Duration { return time.Millisecond * 20 }
	args.Maker = NewProposalMaker(
		t.Local,
		t.LocalParams.NetworkID(),
		func(ctx context.Context, _ base.Height) ([][2]util.Hash, error) {
			<-ctx.Done()

			return nil, ctx.Err()
		},
		args.Pool,
		nil,
	)
	args.ProposerSelectFunc = NewFixedProposerSelector(
		func(base.Point, []base.Node, util.Hash) (base.Node, error) {
			return t.Local, nil
		},
	).Select

	p := NewBaseProposalSelector(t.Local, args)

	pr, err := p.Select(context.Background(), base.RawPoint(66, 11), valuehash.RandomSHA512(), time.Millisecond*20)
	t.Nil(pr)
	t.ErrorIs(err, ErrProposalSelectionFailed)
}

func (t *testBaseProposalSelector) TestParentContextCanceledIsNotProposalSelectionFailed() {
	nodes := t.newNodes(2)
	args := t.newargs(nodes)
	args.MinProposerWait = func() time.Duration { return time.Second }
	args.TimeoutRequest = func() time.Duration { return time.Second }

	remote := nodes[1]
	args.ProposerSelectFunc = NewFixedProposerSelector(
		func(base.Point, []base.Node, util.Hash) (base.Node, error) {
			return remote, nil
		},
	).Select
	args.RequestFunc = func(ctx context.Context, _ base.Point, _ base.Node, _ util.Hash) (base.ProposalSignFact, bool, error) {
		<-ctx.Done()

		return nil, false, ctx.Err()
	}

	p := NewBaseProposalSelector(t.Local, args)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		pr, err := p.Select(ctx, base.RawPoint(66, 11), valuehash.RandomSHA512(), time.Second)
		t.Nil(pr)
		done <- err
	}()

	time.Sleep(time.Millisecond * 20)
	cancel()

	err := <-done
	t.ErrorIs(err, context.Canceled)
	t.NotErrorIs(err, ErrProposalSelectionFailed)
}

func (t *testBaseProposalSelector) TestExpiredWaitUsesPoolHitWhileParentAlive() {
	nodes := t.newNodes(0, t.Local)
	args := t.newargs(nodes)
	point := base.RawPoint(66, 11)
	prev := valuehash.RandomSHA512()
	want, err := args.Maker.Make(context.Background(), point, prev)
	t.NoError(err)
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := NewBaseProposalSelector(t.Local, args).proposalFromNode(
		context.Background(), waitCtx, point, t.Local, prev)
	t.NoError(err)
	t.True(want.Fact().Hash().Equal(got.Fact().Hash()))
}

func (t *testBaseProposalSelector) TestCanceledParentRejectsPoolHit() {
	nodes := t.newNodes(0, t.Local)
	args := t.newargs(nodes)
	point := base.RawPoint(66, 11)
	prev := valuehash.RandomSHA512()
	_, err := args.Maker.Make(context.Background(), point, prev)
	t.NoError(err)
	parentCtx, cancel := context.WithCancel(context.Background())
	cancel()

	pr, err := NewBaseProposalSelector(t.Local, args).proposalFromNode(
		parentCtx, context.Background(), point, t.Local, prev)
	t.Nil(pr)
	t.ErrorIs(err, context.Canceled)
}

func (t *testBaseProposalSelector) TestLocalProposerNonContextErrorIsFatal() {
	nodes := t.newNodes(2, t.Local)
	args := t.newargs(nodes)
	expected := errors.Errorf("local maker failed")
	args.Maker = NewProposalMaker(
		t.Local,
		t.LocalParams.NetworkID(),
		func(_ context.Context, _ base.Height) ([][2]util.Hash, error) {
			return nil, expected
		},
		args.Pool,
		nil,
	)
	args.ProposerSelectFunc = NewFixedProposerSelector(
		func(base.Point, []base.Node, util.Hash) (base.Node, error) {
			return t.Local, nil
		},
	).Select

	p := NewBaseProposalSelector(t.Local, args)

	pr, err := p.Select(context.Background(), base.RawPoint(66, 11), valuehash.RandomSHA512(), time.Second)
	t.Nil(pr)
	t.ErrorIs(err, expected)
	t.NotErrorIs(err, ErrProposalSelectionFailed)
}

func (t *testBaseProposalSelector) TestLocalProposerStaleProposalPointIsPreserved() {
	nodes := t.newNodes(2, t.Local)
	args := t.newargs(nodes)
	args.Maker = NewProposalMaker(
		t.Local,
		t.LocalParams.NetworkID(),
		func(_ context.Context, _ base.Height) ([][2]util.Hash, error) {
			return nil, ErrStaleProposalPoint.Errorf("too old; ignored")
		},
		args.Pool,
		nil,
	)
	args.ProposerSelectFunc = NewFixedProposerSelector(
		func(base.Point, []base.Node, util.Hash) (base.Node, error) {
			return t.Local, nil
		},
	).Select

	p := NewBaseProposalSelector(t.Local, args)

	pr, err := p.Select(context.Background(), base.RawPoint(66, 11), valuehash.RandomSHA512(), time.Second)
	t.Nil(pr)
	t.ErrorIs(err, ErrStaleProposalPoint)
	t.NotErrorIs(err, ErrProposalSelectionFailed)
}

func (t *testBaseProposalSelector) TestFromProposer() {
	nodes := t.newNodes(2, t.Local)
	prev := valuehash.RandomSHA512()
	point := base.RawPoint(66, 11)

	excludelocal := util.FilterSlice(nodes, func(i base.Node) bool {
		return !i.Address().Equal(t.Local.Address())
	})

	t.T().Logf("all nodes: %d", len(nodes))
	t.T().Logf("non-local nodes: %d", len(excludelocal))

	newargs := func() *BaseProposalSelectorArgs {
		args := t.newargs(nodes)
		args.RequestProposalInterval = time.Millisecond * 300

		defaultselector := NewBlockBasedProposerSelector()

		args.ProposerSelectFunc = NewFixedProposerSelector(
			func(point base.Point, nodes []base.Node, previousBlock util.Hash) (base.Node, error) {
				// exclude local
				filtered := util.FilterSlice(nodes, func(i base.Node) bool {
					return !i.Address().Equal(t.Local.Address())
				})

				return defaultselector.Select(context.Background(), point, filtered, previousBlock)
			},
		).Select
		args.GetNodesFunc = func(base.Height) ([]base.Node, bool, error) {
			return nodes, len(nodes) > 0, nil
		}

		return args
	}

	args := newargs()

	selectedproposer, err := args.ProposerSelectFunc(context.Background(), point, nodes, prev)
	t.NoError(err)
	t.T().Logf("selected proposer: %q", selectedproposer.Address())

	t.T().Logf("nodes: %q", func() []string {
		sn := make([]string, len(nodes))
		for i := range nodes {
			sn[i] = nodes[i].Address().String()
		}

		return sn
	}())

	t.Run("from local", func() {
		args := newargs()

		p := NewBaseProposalSelector(t.Local, args)

		pr, err := p.Select(context.Background(), point, prev, time.Second)
		t.NoError(err)
		t.NotNil(pr)

		t.T().Logf("proposal proposer: %q", pr.ProposalFact().Proposer())
		t.True(selectedproposer.Address().Equal(pr.ProposalFact().Proposer()))
	})

	t.Run("failed from proposer", func() {
		args := newargs()
		var requested []string

		args.RequestFunc = func(ctx context.Context, point base.Point, proposer base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
			requested = append(requested, proposer.Address().String())

			if proposer.Address().Equal(selectedproposer.Address()) {
				return nil, false, errors.Errorf("hihihi")
			}

			for i := range nodes {
				n := nodes[i]
				if !n.Address().Equal(proposer.Address()) {
					continue
				}

				pr, err := args.Maker.Make(ctx, point, previousBlock)
				return pr, err == nil, err
			}

			return nil, false, errors.Errorf("proposer not found in suffrage")
		}

		p := NewBaseProposalSelector(t.Local, args)

		pr, err := p.Select(context.Background(), point, prev, time.Second)
		t.Error(err)
		t.Nil(pr)
		t.ErrorIs(err, ErrProposalSelectionFailed)
		t.NotEmpty(requested)
		for i := range requested {
			t.Equal(selectedproposer.Address().String(), requested[i])
		}
	})

	t.Run("failed from proposer with context.Canceled", func() {
		args := newargs()

		var requested int64
		var proposers []string

		args.RequestFunc = func(ctx context.Context, point base.Point, proposer base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
			proposers = append(proposers, proposer.Address().String())

			if n := atomic.LoadInt64(&requested); n < 1 && proposer.Address().Equal(selectedproposer.Address()) {
				atomic.AddInt64(&requested, 1)

				return nil, false, context.Canceled
			}

			for i := range nodes {
				n := nodes[i]
				if !n.Address().Equal(proposer.Address()) {
					continue
				}

				pr, err := NewProposalMaker(
					n.(base.LocalNode),
					t.LocalParams.NetworkID(),
					func(context.Context, base.Height) ([][2]util.Hash, error) { return nil, nil },
					args.Pool,
					nil,
				).Make(ctx, point, previousBlock)
				return pr, err == nil, err
			}

			return nil, false, errors.Errorf("proposer not found in suffrage")
		}

		p := NewBaseProposalSelector(t.Local, args)

		pr, err := p.Select(context.Background(), point, prev, time.Second*2)
		t.NoError(err)
		t.NotNil(pr)

		t.T().Logf("proposal proposer: %q", pr.ProposalFact().Proposer())

		t.True(selectedproposer.Address().Equal(pr.ProposalFact().Proposer()))
		for i := range proposers {
			t.Equal(selectedproposer.Address().String(), proposers[i])
		}
	})

	t.Run("all failed", func() {
		args := newargs()

		args.RequestFunc = func(ctx context.Context, point base.Point, proposer base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
			return nil, false, errors.Errorf("hihihi")
		}

		p := NewBaseProposalSelector(t.Local, args)

		t.Run("internal", func() {
			pr, err := p.selectInternal(context.Background(), point, prev, time.Second)
			t.Error(err)
			t.Nil(pr)
			t.ErrorIs(err, ErrProposalSelectionFailed)
		})

		t.Run("Select; no local fallback", func() {
			pr, err := p.Select(context.Background(), point, prev, time.Second)
			t.Error(err)
			t.Nil(pr)
			t.ErrorIs(err, ErrProposalSelectionFailed)
		})
	})
}

func TestBaseProposalSelector(tt *testing.T) {
	suite.Run(tt, new(testBaseProposalSelector))
}

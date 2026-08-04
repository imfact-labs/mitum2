package isaac

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/network/quicstream"
	"github.com/imfact-labs/mitum2/util"
	"github.com/pkg/errors"
)

var errFailedToRequestProposalToNode = util.NewIDError("request proposal to node")
var ErrProposalSelectionFailed = util.NewIDError("proposal selection failed")

type ProposalSelectFunc func(
	_ context.Context,
	_ base.Point,
	previousBlock util.Hash,
	wait time.Duration,
) (base.ProposalSignFact, error)

type BaseProposalSelectorArgs struct {
	Pool                    ProposalPool
	ProposerSelectFunc      ProposerSelectFunc
	Maker                   *ProposalMaker
	GetNodesFunc            func(base.Height) ([]base.Node, bool, error)
	RequestFunc             func(context.Context, base.Point, base.Node, util.Hash) (base.ProposalSignFact, bool, error)
	TimeoutRequest          func() time.Duration
	RequestProposalInterval time.Duration
	MinProposerWait         func() time.Duration
}

func NewBaseProposalSelectorArgs() *BaseProposalSelectorArgs {
	return &BaseProposalSelectorArgs{
		GetNodesFunc: func(base.Height) ([]base.Node, bool, error) {
			return nil, false, errors.Wrapf(context.Canceled, "get nodes")
		},
		RequestFunc: func(context.Context, base.Point, base.Node, util.Hash) (base.ProposalSignFact, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("request")
		},
		RequestProposalInterval: time.Millisecond * 666, //nolint:gomnd //...
		MinProposerWait: func() time.Duration {
			return DefaultMinProposerWait
		},
		TimeoutRequest: func() time.Duration {
			return DefaultTimeoutRequest
		},
	}
}

type BaseProposalSelector struct {
	local base.LocalNode
	args  *BaseProposalSelectorArgs
	sync.Mutex
}

func NewBaseProposalSelector(
	local base.LocalNode,
	args *BaseProposalSelectorArgs,
) *BaseProposalSelector {
	return &BaseProposalSelector{
		local: local,
		args:  args,
	}
}

func (p *BaseProposalSelector) Select(
	ctx context.Context,
	point base.Point,
	previousBlock util.Hash,
	wait time.Duration,
) (base.ProposalSignFact, error) {
	switch pr, err := p.selectInternal(ctx, point, previousBlock, wait); {
	case err != nil:
		return nil, err
	default:
		return pr, nil
	}
}

func (p *BaseProposalSelector) selectInternal(
	ctx context.Context,
	point base.Point,
	previousBlock util.Hash,
	wait time.Duration,
) (base.ProposalSignFact, error) {
	p.Lock()
	defer p.Unlock()

	pwait := wait
	minProposerWait := p.args.MinProposerWait()
	if pwait < minProposerWait {
		pwait = minProposerWait
	}

	wctx, cancel := context.WithTimeout(ctx, pwait)
	defer cancel()

	var nodes []base.Node

	switch i, found, err := p.getNodes(point.Height(), p.args.GetNodesFunc); {
	case err != nil, !found:
		if err == nil {
			err = errors.Errorf("nodes not found for height, %v", point)
		}

		return nil, errors.WithMessagef(err, "get suffrage for height, %d", point.Height())
	case len(i) < 2:
		pr, err := p.proposalFromNode(ctx, wctx, point, i[0], previousBlock)
		if err != nil {
			return nil, p.handleProposalRequestFailure(ctx, wctx, point, i[0], previousBlock, err)
		}

		return pr, nil
	default:
		nodes = i
	}

	switch pr, proposer, err := p.selectFromProposer(ctx, wctx, point, nodes, previousBlock); {
	case errors.Is(err, errFailedToRequestProposalToNode),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return nil, p.handleProposalRequestFailure(ctx, wctx, point, p.findNode(nodes, proposer), previousBlock, err)
	case err != nil:
		return nil, err
	case pr != nil:
		return pr, nil
	default:
		return nil, ErrProposalSelectionFailed.Errorf(
			"selected proposer did not provide proposal, point=%v previous_block=%q",
			point,
			previousBlock,
		)
	}
}

func (p *BaseProposalSelector) selectFromProposer(
	parentCtx context.Context,
	waitCtx context.Context,
	point base.Point,
	nodes []base.Node,
	previousBlock util.Hash,
) (base.ProposalSignFact, base.Address, error) {
	e := util.StringError("select proposal from proposer")

	proposer, err := p.args.ProposerSelectFunc(waitCtx, point, nodes, previousBlock)
	if err != nil {
		return nil, nil, e.WithMessage(err, "select proposer")
	}

	pr, err := p.proposalFromNode(parentCtx, waitCtx, point, proposer, previousBlock)
	if err != nil {
		return nil, proposer.Address(), e.Wrap(err)
	}

	return pr, proposer.Address(), err
}

func (p *BaseProposalSelector) proposalFromNode(
	parentCtx context.Context,
	waitCtx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	// A proposal already published just before the wait deadline remains usable;
	// the consensus parent and state layer decide whether it is still current.
	switch pr, found, err := p.args.Pool.ProposalByPoint(point, proposer.Address(), previousBlock); {
	case err != nil:
		return nil, err
	case found:
		if err := parentCtx.Err(); err != nil {
			return nil, errors.WithStack(err)
		}

		return pr, nil
	}

	ticker := time.NewTicker(time.Millisecond * 33)
	defer ticker.Stop()

	var reset sync.Once

	for {
		select {
		case <-parentCtx.Done():
			return nil, errors.WithStack(parentCtx.Err())
		case <-waitCtx.Done():
			return nil, errors.WithStack(waitCtx.Err())
		case <-ticker.C:
			reset.Do(func() {
				ticker.Reset(p.args.RequestProposalInterval)
			})

			switch pr, err := p.findProposalWithContexts(parentCtx, waitCtx, point, proposer, previousBlock); {
			case err == nil:
				return pr, nil
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				// NOTE ignore context error from findProposal; if context error
				// is from main context, it will be catched from the main select
				// ctx.Done().
			case errors.Is(err, errFailedToRequestProposalToNode):
			default:
				return nil, errors.WithMessage(err, "find proposal")
			}
		}
	}
}

func (p *BaseProposalSelector) findProposal(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	return p.findProposalWithContexts(ctx, ctx, point, proposer, previousBlock)
}

func (p *BaseProposalSelector) findProposalWithContexts(
	parentCtx context.Context,
	waitCtx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	e := util.StringError("find proposal")

	switch pr, found, err := p.args.Pool.ProposalByPoint(point, proposer.Address(), previousBlock); {
	case err != nil:
		return nil, e.Wrap(err)
	case found:
		if err := parentCtx.Err(); err != nil {
			return nil, e.Wrap(err)
		}

		return pr, nil
	}

	pr, err := p.findProposalFromProposer(parentCtx, waitCtx, point, proposer, previousBlock)
	if err != nil {
		return nil, e.Wrap(err)
	}

	return pr, nil
}

func (p *BaseProposalSelector) findProposalFromProposer(
	parentCtx context.Context,
	waitCtx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	if proposer.Address().Equal(p.local.Address()) {
		return p.args.Maker.MakeWithContexts(parentCtx, waitCtx, point, previousBlock)
	}

	// NOTE if not found in local, request to proposer node
	rctx, cancel := context.WithTimeout(waitCtx, p.args.TimeoutRequest())
	defer cancel()

	donech := make(chan interface{})

	go func() {
		switch pr, found, err := p.args.RequestFunc(rctx, point, proposer, previousBlock); {
		case err != nil, !found:
			if !found {
				err = errors.Errorf("empty proposal")
			}

			donech <- err
		default:
			donech <- pr
		}
	}()

	select {
	case <-rctx.Done():
		return nil, errFailedToRequestProposalToNode.WithMessage(
			rctx.Err(), "context error; remote node, %q", proposer.Address())
	case i := <-donech:
		switch t := i.(type) {
		case error:
			return nil, errFailedToRequestProposalToNode.WithMessage(
				t, "request failed; remote node, %q", proposer.Address())
		case base.ProposalSignFact:
			if _, err := p.args.Pool.SetProposal(t); err != nil {
				return nil, err
			}

			return t, nil
		}
	}

	return nil, errors.Errorf("empty proposal")
}

func (*BaseProposalSelector) getNodes(
	height base.Height,
	f func(base.Height) ([]base.Node, bool, error),
) ([]base.Node, bool, error) {
	switch nodes, found, err := f(height.SafePrev()); {
	case err != nil, !found:
		return nil, found, err
	case len(nodes) < 1:
		return nil, false, errors.Errorf("empty suffrage nodes")
	case len(nodes) < 2:
		return nodes, true, nil
	default:
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].Address().String() < nodes[j].Address().String()
		})

		return nodes, true, nil
	}
}

func (p *BaseProposalSelector) handleProposalRequestFailure(
	ctx context.Context,
	wctx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
	err error,
) error {
	if errors.Is(err, ErrStaleProposalPoint) {
		return err
	}

	if ctx.Err() != nil && isContextError(err) {
		return errors.WithStack(ctx.Err())
	}

	if ctx.Err() == nil && wctx.Err() != nil && isContextError(err) {
		return ErrProposalSelectionFailed.WithMessage(
			err,
			"selected proposer wait budget expired for point=%v proposer=%q previous_block=%q",
			point,
			proposerAddressString(proposer),
			previousBlock,
		)
	}

	if proposer == nil || proposer.Address().Equal(p.local.Address()) {
		return err
	}

	if !errors.Is(err, errFailedToRequestProposalToNode) && !isContextError(err) {
		return err
	}

	return ErrProposalSelectionFailed.WithMessage(
		err,
		"selected proposer failed for point=%v proposer=%q previous_block=%q",
		point,
		proposer.Address(),
		previousBlock,
	)
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func proposerAddressString(proposer base.Node) string {
	if proposer == nil {
		return "<nil>"
	}

	return proposer.Address().String()
}

func (*BaseProposalSelector) findNode(nodes []base.Node, addr base.Address) base.Node {
	for i := range nodes {
		if nodes[i].Address().Equal(addr) {
			return nodes[i]
		}
	}

	return nil
}

var errConcurrentRequestProposalFound = util.NewIDError("proposal found")

func ConcurrentRequestProposal(
	ctx context.Context,
	point base.Point,
	proposer base.Node,
	previousBlock util.Hash,
	client NetworkClient,
	cis []quicstream.ConnInfo,
	networkID base.NetworkID,
) (base.ProposalSignFact, bool, error) {
	worker, err := util.NewBaseJobWorker(ctx, int64(len(cis)))
	if err != nil {
		return nil, false, err
	}

	defer worker.Close()

	prlocked := util.EmptyLocked[base.ProposalSignFact]()

	go func() {
		defer worker.Done()

		for i := range cis {
			i := i
			ci := cis[i]

			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				switch pr, found, err := client.RequestProposal(ctx, ci, point, proposer.Address(), previousBlock); {
				case err != nil:
					return nil
				case !found:
					return nil
				case !isExpectedValidProposal(point, proposer, pr, networkID):
					return nil
				default:
					_ = prlocked.SetValue(pr)

					return errConcurrentRequestProposalFound.WithStack()
				}
			}); err != nil {
				return
			}
		}
	}()

	switch err := worker.Wait(); {
	case err == nil:
	case errors.Is(err, errConcurrentRequestProposalFound):
	default:
		return nil, false, err
	}

	switch pr, isempty := prlocked.Value(); {
	case isempty, pr == nil:
		return nil, false, nil
	default:
		return pr, true, nil
	}
}

func isExpectedValidProposal(
	point base.Point,
	proposer base.Node,
	pr base.ProposalSignFact,
	networkID base.NetworkID,
) bool {
	if err := pr.IsValid(networkID); err != nil {
		return false
	}

	switch {
	case !pr.Point().Equal(point):
		return false
	case !proposer.Address().Equal(pr.ProposalFact().Proposer()):
		return false
	case !proposer.Publickey().Equal(pr.Signs()[0].Signer()):
		return false
	default:
		return true
	}
}

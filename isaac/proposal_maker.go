package isaac

import (
	"context"
	"sync"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/logging"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

type ProposalMaker struct {
	*logging.Logging
	local            base.LocalNode
	pool             ProposalPool
	getOperations    func(context.Context, base.Height) ([][2]util.Hash, error)
	lastBlockMap     func() (base.BlockMap, bool, error)
	operationTimeout func() time.Duration
	networkID        base.NetworkID
	sync.Mutex
}

var ErrStaleProposalPoint = util.NewIDError("stale proposal point")

type proposalPointPolicy uint8

const (
	proposalPointOperations proposalPointPolicy = iota
	proposalPointPreferEmpty
)

func NewProposalMaker(
	local base.LocalNode,
	networkID base.NetworkID,
	getOperations func(context.Context, base.Height) ([][2]util.Hash, error),
	pool ProposalPool,
	lastBlockMap func() (base.BlockMap, bool, error),
) *ProposalMaker {
	if getOperations == nil {
		getOperations = func( //revive:disable-line:modifies-parameter
			context.Context, base.Height,
		) ([][2]util.Hash, error) {
			return nil, nil
		}
	}

	if lastBlockMap == nil {
		lastBlockMap = func() (base.BlockMap, bool, error) { //revive:disable-line:modifies-parameter
			return nil, false, nil
		}
	}

	return &ProposalMaker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "proposal-maker")
		}),
		local:            local,
		networkID:        networkID,
		getOperations:    getOperations,
		pool:             pool,
		lastBlockMap:     lastBlockMap,
		operationTimeout: func() time.Duration { return DefaultProposalOperationTimeout },
	}
}

// SetOperationTimeoutFunc replaces the operation collection budget source.
// It is primarily useful for deployments that tune proposer waits and tests.
func (p *ProposalMaker) SetOperationTimeoutFunc(f func() time.Duration) *ProposalMaker {
	if f != nil {
		p.operationTimeout = f
	}

	return p
}

func (p *ProposalMaker) PreferEmpty(
	ctx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	p.Lock()
	defer p.Unlock()

	e := util.StringError("make empty proposal")

	switch m, found, err := p.lastBlockMap(); {
	case err != nil:
		return nil, e.Wrap(err)
	case !found:
	case point.Height() < m.Manifest().Height()-1:
		return nil, e.Wrap(ErrStaleProposalPoint.Errorf("too old; ignored"))
	}

	pr, err := p.preferEmpty(ctx, point, previousBlock)

	return pr, e.Wrap(err)
}

func (p *ProposalMaker) preferEmpty(
	ctx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	return p.preferEmptyWithContexts(ctx, ctx, point, previousBlock)
}

func (p *ProposalMaker) preferEmptyWithContexts(
	parentCtx context.Context, waitCtx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	switch pr, found, err := p.pool.ProposalByPoint(point, p.local.Address(), previousBlock); {
	case err != nil:
		return nil, err
	case found:
		if _, err := p.validateProposalPoint(parentCtx, point, previousBlock); err != nil {
			return nil, err
		}

		return pr, nil
	}
	if err := parentCtx.Err(); err != nil {
		return nil, err
	}
	if err := waitCtx.Err(); err != nil {
		return nil, err
	}

	pr, err := p.signProposal(point, previousBlock, nil)
	if err != nil {
		return nil, errors.WithMessagef(err, "make empty proposal, %q", point)
	}
	if _, err := p.validateProposalPoint(parentCtx, point, previousBlock); err != nil {
		return nil, err
	}
	if err := waitCtx.Err(); err != nil {
		return nil, err
	}
	if _, err := p.pool.SetProposal(pr); err != nil {
		return nil, err
	}

	return pr, nil
}

func (p *ProposalMaker) Make(
	ctx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	return p.MakeWithContexts(ctx, ctx, point, previousBlock)
}

// MakeWithContexts keeps the consensus lifetime separate from the selector or
// RPC wait budget. Only operation collection gets an empty-proposal fallback.
func (p *ProposalMaker) MakeWithContexts(
	parentCtx context.Context, waitCtx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	lockStarted := time.Now()
	p.Lock()
	defer p.Unlock()
	p.Log().Debug().Stringer("elapsed", time.Since(lockStarted)).Msg("proposal maker lock wait done")

	e := util.StringError("make proposal, %q", point)

	switch policy, err := p.validateProposalPoint(parentCtx, point, previousBlock); {
	case err != nil:
		return nil, e.Wrap(err)
	case policy == proposalPointPreferEmpty:
		pr, err := p.preferEmptyWithContexts(parentCtx, waitCtx, point, previousBlock)

		return pr, e.Wrap(err)
	}

	pr, err := p.makeNew(parentCtx, waitCtx, point, previousBlock)

	return pr, e.Wrap(err)
}

func (p *ProposalMaker) makeNew(
	parentCtx context.Context, waitCtx context.Context, point base.Point, previousBlock util.Hash,
) (base.ProposalSignFact, error) {
	switch pr, found, err := p.pool.ProposalByPoint(point, p.local.Address(), previousBlock); {
	case err != nil:
		return nil, errors.WithStack(err)
	case found:
		if err := parentCtx.Err(); err != nil {
			return nil, errors.WithStack(err)
		}

		return pr, nil
	}
	if err := waitCtx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	budget := p.operationTimeout()
	if deadline, ok := waitCtx.Deadline(); ok {
		remaining := time.Until(deadline)
		if half := remaining / 2; half < budget {
			budget = half
		}
	}
	if budget <= 0 {
		if err := waitCtx.Err(); err != nil {
			return nil, errors.WithStack(err)
		}

		return nil, errors.WithStack(context.DeadlineExceeded)
	}

	operationCtx, cancel := context.WithTimeout(waitCtx, budget)
	started := time.Now()
	ops, err := p.getOperations(operationCtx, point.Height())
	operationErr := operationCtx.Err()
	cancel()
	p.Log().Debug().Stringer("elapsed", time.Since(started)).Stringer("budget", budget).
		Msg("proposal operation collection done")
	if err == nil && operationErr != nil {
		err = operationErr
	}
	if err != nil {
		switch {
		case parentCtx.Err() != nil:
			return nil, errors.WithStack(parentCtx.Err())
		case waitCtx.Err() != nil:
			return nil, errors.WithStack(waitCtx.Err())
		case isContextError(operationErr) && isContextError(err):
			pr, perr := p.publishProposal(parentCtx, waitCtx, point, previousBlock, nil, true)

			return pr, errors.WithMessage(perr, "operation budget expired; make empty proposal")
		}

		return nil, errors.WithMessage(err, "get operations")
	}
	p.Log().Trace().Func(func(e *zerolog.Event) {
		for i := range ops {
			e.Interface("operation", ops[i])
		}
	}).Msg("new operation for proposal maker")

	pr, err := p.publishProposal(parentCtx, waitCtx, point, previousBlock, ops, false)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return pr, nil
}

func (p *ProposalMaker) validateProposalPoint(
	parentCtx context.Context, point base.Point, previousBlock util.Hash,
) (proposalPointPolicy, error) {
	if err := parentCtx.Err(); err != nil {
		return proposalPointOperations, err
	}

	switch m, found, err := p.lastBlockMap(); {
	case err != nil:
		return proposalPointOperations, err
	case !found:
		return proposalPointOperations, nil
	case point.Height() < m.Manifest().Height()-1:
		return proposalPointOperations, ErrStaleProposalPoint.Errorf("too old; ignored")
	case point.Height() > m.Manifest().Height()+1:
		return proposalPointPreferEmpty, nil
	case point.Height() == m.Manifest().Height()+1 && !previousBlock.Equal(m.Manifest().Hash()):
		return proposalPointPreferEmpty, nil
	default:
		return proposalPointOperations, nil
	}
}

func (p *ProposalMaker) publishProposal(
	parentCtx context.Context,
	waitCtx context.Context,
	point base.Point,
	previousBlock util.Hash,
	ops [][2]util.Hash,
	forceEmpty bool,
) (base.ProposalSignFact, error) {
	if forceEmpty {
		return p.preferEmptyWithContexts(parentCtx, waitCtx, point, previousBlock)
	}

	pr, err := p.signProposal(point, previousBlock, ops)
	if err != nil {
		return nil, err
	}

	policy, err := p.validateProposalPoint(parentCtx, point, previousBlock)
	if err != nil {
		return nil, err
	}
	if policy == proposalPointPreferEmpty {
		return p.preferEmptyWithContexts(parentCtx, waitCtx, point, previousBlock)
	}

	if _, err := p.pool.SetProposal(pr); err != nil {
		return nil, err
	}

	return pr, nil
}

func (p *ProposalMaker) makeProposal(
	point base.Point, previousBlock util.Hash, ops [][2]util.Hash,
) (sf ProposalSignFact, _ error) {
	pr, err := p.signProposal(point, previousBlock, ops)
	if err != nil {
		return sf, err
	}

	if _, err := p.pool.SetProposal(pr); err != nil {
		return sf, err
	}

	return pr, nil
}

func (p *ProposalMaker) signProposal(
	point base.Point, previousBlock util.Hash, ops [][2]util.Hash,
) (sf ProposalSignFact, _ error) {
	fact := NewProposalFact(point, p.local.Address(), previousBlock, ops)

	signfact := NewProposalSignFact(fact)
	if err := signfact.Sign(p.local.Privatekey(), p.networkID); err != nil {
		return sf, err
	}

	return signfact, nil
}

package isaac

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testProposalMaker struct {
	suite.Suite
}

func (t *testProposalMaker) newMaker(
	getOperations func(context.Context, base.Height) ([][2]util.Hash, error),
	lastBlockMap func() (base.BlockMap, bool, error),
) *ProposalMaker {
	return NewProposalMaker(
		base.RandomLocalNode(),
		util.UUID().Bytes(),
		getOperations,
		newDummyProposalPool(10),
		lastBlockMap,
	)
}

func (t *testProposalMaker) TestPreferEmpty() {
	point := base.RawPoint(33, 3)

	t.Run("ok", func() {
		maker := t.newMaker(nil, nil)

		pr, err := maker.PreferEmpty(context.Background(), point, valuehash.RandomSHA256())
		t.NoError(err)
		t.Empty(pr.ProposalFact().Operations())
	})

	t.Run("not empty in pool", func() {
		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return [][2]util.Hash{
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
				}, nil
			},
			nil,
		)

		prev := valuehash.RandomSHA256()
		prevpr, err := maker.Make(context.Background(), point, prev)
		t.NoError(err)
		t.NotEmpty(prevpr.ProposalFact().Operations())

		pr, err := maker.PreferEmpty(context.Background(), point, prev)
		t.NoError(err)
		t.Equal(2, len(pr.ProposalFact().Operations()))
	})

	t.Run("old point", func() {
		maker := t.newMaker(
			nil,
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		_, err := maker.PreferEmpty(context.Background(), point.PrevHeight().PrevHeight(), valuehash.RandomSHA256())
		t.Error(err)
		t.ErrorContains(err, "too old")
	})

	t.Run("unreachable point", func() {
		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return [][2]util.Hash{
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
				}, nil
			},
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		pr, err := maker.PreferEmpty(context.Background(), point.NextHeight().NextHeight(), valuehash.RandomSHA256())
		t.NoError(err)
		t.Empty(pr.ProposalFact().Operations())
	})
}

func (t *testProposalMaker) TestMake() {
	point := base.RawPoint(33, 3)

	t.Run("ok", func() {
		ops := [][2]util.Hash{
			{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
			{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
		}

		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return ops, nil
			},
			nil,
		)

		prev := valuehash.RandomSHA256()

		pr, err := maker.Make(context.Background(), point, prev)
		t.NoError(err)
		t.NotEmpty(pr.ProposalFact().Operations())

		rops := pr.ProposalFact().Operations()
		t.Equal(len(ops), len(rops))
		for i := range ops {
			t.True(ops[i][0].Equal(rops[i][0]))
			t.True(ops[i][1].Equal(rops[i][1]))
		}

		rpr, err := maker.Make(context.Background(), point, prev)
		t.NoError(err)
		t.NotEmpty(rpr.ProposalFact().Operations())
		t.True(pr.Fact().Hash().Equal(rpr.Fact().Hash()))
	})

	t.Run("old point", func() {
		maker := t.newMaker(
			nil,
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		_, err := maker.Make(context.Background(), point.PrevHeight().PrevHeight(), valuehash.RandomSHA256())
		t.Error(err)
		t.ErrorContains(err, "too old")
	})

	t.Run("unreachable point", func() {
		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return [][2]util.Hash{
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
				}, nil
			},
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		pr, err := maker.Make(context.Background(), point.NextHeight().NextHeight(), valuehash.RandomSHA256())
		t.NoError(err)
		t.Empty(pr.ProposalFact().Operations())
	})

	t.Run("previous block unmarch", func() {
		maker := t.newMaker(
			func(context.Context, base.Height) ([][2]util.Hash, error) {
				return [][2]util.Hash{
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
					{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
				}, nil
			},
			func() (base.BlockMap, bool, error) {
				return base.NewDummyBlockMap(base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())), true, nil
			},
		)

		pr, err := maker.Make(context.Background(), point.NextHeight(), valuehash.RandomSHA256())
		t.NoError(err)
		t.Empty(pr.ProposalFact().Operations())
	})
}

// TestConcurrentMakeCreatesSingleProposal verifies the ISSUE-005 guard
// requirement: keeping selected operations in the candidate queue is safe
// against duplicate proposal creation because the existing ProposalMaker mutex
// already serializes Make() for the same point. The first caller builds the
// proposal and the rest reuse it via ProposalByPoint, so no extra in-flight
// guard is needed.
func (t *testProposalMaker) TestConcurrentMakeCreatesSingleProposal() {
	point := base.RawPoint(33, 3)
	prev := valuehash.RandomSHA256()

	ops := [][2]util.Hash{
		{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
		{valuehash.RandomSHA256(), valuehash.RandomSHA256()},
	}

	var getOperationsCalled int64

	maker := t.newMaker(
		func(context.Context, base.Height) ([][2]util.Hash, error) {
			atomic.AddInt64(&getOperationsCalled, 1)

			// NOTE widen the window between the ProposalByPoint miss and
			// SetProposal so concurrent callers would collide without the lock.
			time.Sleep(time.Millisecond * 50)

			return ops, nil
		},
		nil,
	)

	const workers = 8

	var wg sync.WaitGroup
	results := make([]base.ProposalSignFact, workers)
	errs := make([]error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			pr, err := maker.Make(context.Background(), point, prev)
			results[idx] = pr
			errs[idx] = err
		}(i)
	}

	wg.Wait()

	// NOTE only the first caller builds the proposal; the rest hit
	// ProposalByPoint, so getOperations runs exactly once.
	t.Equal(int64(1), atomic.LoadInt64(&getOperationsCalled))

	var facthash util.Hash
	for i := range results {
		t.NoError(errs[i])
		t.NotNil(results[i])

		if facthash == nil {
			facthash = results[i].Fact().Hash()

			continue
		}

		t.True(facthash.Equal(results[i].Fact().Hash()),
			"all concurrent Make() calls must return the same proposal")
	}
}

func TestProposalMaker(t *testing.T) {
	suite.Run(t, new(testProposalMaker))
}

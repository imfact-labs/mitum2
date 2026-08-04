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
	"github.com/pkg/errors"
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
		t.ErrorIs(err, ErrStaleProposalPoint)
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
		t.ErrorIs(err, ErrStaleProposalPoint)
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

func (t *testProposalMaker) TestOperationBudgetTimeoutPrefersAndReusesEmpty() {
	point := base.RawPoint(33, 3)
	prev := valuehash.RandomSHA256()
	var called int64
	maker := t.newMaker(func(ctx context.Context, _ base.Height) ([][2]util.Hash, error) {
		atomic.AddInt64(&called, 1)
		<-ctx.Done()

		return nil, ctx.Err()
	}, nil).SetOperationTimeoutFunc(func() time.Duration { return time.Millisecond * 10 })

	pr, err := maker.MakeWithContexts(context.Background(), context.Background(), point, prev)
	t.NoError(err)
	t.Empty(pr.ProposalFact().Operations())

	rpr, err := maker.MakeWithContexts(context.Background(), context.Background(), point, prev)
	t.NoError(err)
	t.True(pr.Fact().Hash().Equal(rpr.Fact().Hash()))
	t.Equal(int64(1), atomic.LoadInt64(&called))
}

func (t *testProposalMaker) TestContextErrorWithoutOperationContextEndIsFatal() {
	for name, expected := range map[string]error{
		"canceled":          context.Canceled,
		"deadline exceeded": context.DeadlineExceeded,
	} {
		t.Run(name, func() {
			point := base.RawPoint(33, 3)
			prev := valuehash.RandomSHA256()
			maker := t.newMaker(func(ctx context.Context, _ base.Height) ([][2]util.Hash, error) {
				t.NoError(ctx.Err())

				return nil, expected
			}, nil).SetOperationTimeoutFunc(func() time.Duration { return time.Second })

			pr, err := maker.MakeWithContexts(context.Background(), context.Background(), point, prev)
			t.Nil(pr)
			t.ErrorIs(err, expected)
			_, found, err := maker.pool.ProposalByPoint(point, maker.local.Address(), prev)
			t.NoError(err)
			t.False(found)
		})
	}
}

func (t *testProposalMaker) TestParentCancellationDoesNotPublishEmpty() {
	point := base.RawPoint(33, 3)
	prev := valuehash.RandomSHA256()
	started := make(chan struct{})
	maker := t.newMaker(func(ctx context.Context, _ base.Height) ([][2]util.Hash, error) {
		close(started)
		<-ctx.Done()

		return nil, ctx.Err()
	}, nil).SetOperationTimeoutFunc(func() time.Duration { return time.Second })
	parentCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := maker.MakeWithContexts(parentCtx, context.Background(), point, prev)
		done <- err
	}()
	<-started
	cancel()

	err := <-done
	t.ErrorIs(err, context.Canceled)
	_, found, err := maker.pool.ProposalByPoint(point, maker.local.Address(), prev)
	t.NoError(err)
	t.False(found)
}

func (t *testProposalMaker) TestExpiredWaitAfterMutexMissDoesNotPublishEmpty() {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int64
	maker := t.newMaker(func(context.Context, base.Height) ([][2]util.Hash, error) {
		if atomic.AddInt64(&calls, 1) == 1 {
			close(started)
			<-release
		}

		return nil, nil
	}, nil).SetOperationTimeoutFunc(func() time.Duration { return time.Second })

	go func() {
		_, _ = maker.Make(context.Background(), base.RawPoint(33, 1), valuehash.RandomSHA256())
	}()
	<-started
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()
	point := base.RawPoint(34, 1)
	prev := valuehash.RandomSHA256()
	done := make(chan error, 1)
	go func() {
		_, err := maker.MakeWithContexts(context.Background(), waitCtx, point, prev)
		done <- err
	}()
	time.Sleep(time.Millisecond * 20)
	close(release)

	err := <-done
	t.ErrorIs(err, context.DeadlineExceeded)
	_, found, err := maker.pool.ProposalByPoint(point, maker.local.Address(), prev)
	t.NoError(err)
	t.False(found)
	t.Equal(int64(1), atomic.LoadInt64(&calls))
}

func (t *testProposalMaker) TestWaitCancellationStopsOperationCollectionWithoutProposal() {
	point := base.RawPoint(33, 3)
	prev := valuehash.RandomSHA256()
	started := make(chan struct{})
	stopped := make(chan struct{})
	maker := t.newMaker(func(ctx context.Context, _ base.Height) ([][2]util.Hash, error) {
		close(started)
		<-ctx.Done()
		close(stopped)

		return nil, ctx.Err()
	}, nil).SetOperationTimeoutFunc(func() time.Duration { return time.Second })
	waitCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := maker.MakeWithContexts(context.Background(), waitCtx, point, prev)
		done <- err
	}()
	<-started
	cancel()
	<-stopped

	err := <-done
	t.ErrorIs(err, context.Canceled)
	_, found, err := maker.pool.ProposalByPoint(point, maker.local.Address(), prev)
	t.NoError(err)
	t.False(found)
}

func (t *testProposalMaker) TestNormalProposalRevalidatesPointBeforePublish() {
	point := base.RawPoint(33, 3)
	prev := valuehash.RandomSHA256()
	operationsStarted := make(chan struct{})
	releaseOperations := make(chan struct{})
	var stale atomic.Bool
	maker := t.newMaker(func(context.Context, base.Height) ([][2]util.Hash, error) {
		close(operationsStarted)
		<-releaseOperations

		return [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}}, nil
	}, func() (base.BlockMap, bool, error) {
		height := point.Height() - 1
		if stale.Load() {
			height = point.Height() + 2
		}

		return base.NewDummyBlockMap(base.NewDummyManifest(height, prev)), true, nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := maker.Make(context.Background(), point, prev)
		done <- err
	}()
	<-operationsStarted
	stale.Store(true)
	close(releaseOperations)

	err := <-done
	t.ErrorIs(err, ErrStaleProposalPoint)
	_, found, err := maker.pool.ProposalByPoint(point, maker.local.Address(), prev)
	t.NoError(err)
	t.False(found)
}

func (t *testProposalMaker) TestEmptyFallbackRevalidatesPointBeforePublish() {
	point := base.RawPoint(33, 3)
	prev := valuehash.RandomSHA256()
	revalidating := make(chan struct{})
	releaseValidation := make(chan struct{})
	var calls atomic.Int64
	maker := t.newMaker(func(ctx context.Context, _ base.Height) ([][2]util.Hash, error) {
		<-ctx.Done()

		return nil, ctx.Err()
	}, func() (base.BlockMap, bool, error) {
		if calls.Add(1) == 2 {
			close(revalidating)
			<-releaseValidation

			return base.NewDummyBlockMap(base.NewDummyManifest(point.Height()+2, prev)), true, nil
		}

		return base.NewDummyBlockMap(base.NewDummyManifest(point.Height()-1, prev)), true, nil
	}).SetOperationTimeoutFunc(func() time.Duration { return time.Millisecond * 10 })
	done := make(chan error, 1)
	go func() {
		_, err := maker.Make(context.Background(), point, prev)
		done <- err
	}()
	<-revalidating
	close(releaseValidation)

	err := <-done
	t.ErrorIs(err, ErrStaleProposalPoint)
	_, found, err := maker.pool.ProposalByPoint(point, maker.local.Address(), prev)
	t.NoError(err)
	t.False(found)
}

func (t *testProposalMaker) TestPublishRevalidationLastBlockMapErrorIsFatal() {
	point := base.RawPoint(33, 3)
	prev := valuehash.RandomSHA256()
	expected := errors.Errorf("last block map changed")
	var calls atomic.Int64
	maker := t.newMaker(func(context.Context, base.Height) ([][2]util.Hash, error) {
		return nil, nil
	}, func() (base.BlockMap, bool, error) {
		if calls.Add(1) == 2 {
			return nil, false, expected
		}

		return base.NewDummyBlockMap(base.NewDummyManifest(point.Height()-1, prev)), true, nil
	})

	pr, err := maker.Make(context.Background(), point, prev)
	t.Nil(pr)
	t.ErrorIs(err, expected)
	_, found, err := maker.pool.ProposalByPoint(point, maker.local.Address(), prev)
	t.NoError(err)
	t.False(found)
}

func (t *testProposalMaker) TestSpecialEmptyHonorsWaitAndPoolHitPolicy() {
	point := base.RawPoint(35, 3)
	prev := valuehash.RandomSHA256()
	manifest := base.NewDummyManifest(point.Height()-2, valuehash.RandomSHA256())
	newMaker := func() *ProposalMaker {
		return t.newMaker(nil, func() (base.BlockMap, bool, error) {
			return base.NewDummyBlockMap(manifest), true, nil
		})
	}

	t.Run("unreachable pool miss", func() {
		maker := newMaker()
		waitCtx, cancel := context.WithCancel(context.Background())
		cancel()
		pr, err := maker.MakeWithContexts(context.Background(), waitCtx, point, prev)
		t.Nil(pr)
		t.ErrorIs(err, context.Canceled)
		_, found, err := maker.pool.ProposalByPoint(point, maker.local.Address(), prev)
		t.NoError(err)
		t.False(found)
	})

	t.Run("previous block mismatch pool miss", func() {
		mismatchPoint := base.RawPoint(int64(manifest.Height()+1), 3)
		maker := newMaker()
		waitCtx, cancel := context.WithCancel(context.Background())
		cancel()
		pr, err := maker.MakeWithContexts(context.Background(), waitCtx, mismatchPoint, prev)
		t.Nil(pr)
		t.ErrorIs(err, context.Canceled)
		_, found, err := maker.pool.ProposalByPoint(mismatchPoint, maker.local.Address(), prev)
		t.NoError(err)
		t.False(found)
	})

	t.Run("expired wait pool hit", func() {
		maker := newMaker()
		want, err := maker.makeProposal(point, prev, nil)
		t.NoError(err)
		waitCtx, cancel := context.WithCancel(context.Background())
		cancel()
		got, err := maker.MakeWithContexts(context.Background(), waitCtx, point, prev)
		t.NoError(err)
		t.True(want.Fact().Hash().Equal(got.Fact().Hash()))
	})

	t.Run("previous block mismatch expired wait pool hit", func() {
		mismatchPoint := base.RawPoint(int64(manifest.Height()+1), 3)
		maker := newMaker()
		want, err := maker.makeProposal(mismatchPoint, prev, nil)
		t.NoError(err)
		waitCtx, cancel := context.WithCancel(context.Background())
		cancel()
		got, err := maker.MakeWithContexts(context.Background(), waitCtx, mismatchPoint, prev)
		t.NoError(err)
		t.True(want.Fact().Hash().Equal(got.Fact().Hash()))
	})

	t.Run("stale rejects pool hit", func() {
		maker := newMaker()
		want, err := maker.makeProposal(point, prev, nil)
		t.NoError(err)
		maker.lastBlockMap = func() (base.BlockMap, bool, error) {
			return base.NewDummyBlockMap(base.NewDummyManifest(point.Height()+2, want.Fact().Hash())), true, nil
		}
		pr, err := maker.MakeWithContexts(context.Background(), context.Background(), point, prev)
		t.Nil(pr)
		t.ErrorIs(err, ErrStaleProposalPoint)
	})

	t.Run("parent cancellation rejects pool hit", func() {
		maker := newMaker()
		_, err := maker.makeProposal(point, prev, nil)
		t.NoError(err)
		parentCtx, cancel := context.WithCancel(context.Background())
		cancel()
		pr, err := maker.MakeWithContexts(parentCtx, context.Background(), point, prev)
		t.Nil(pr)
		t.ErrorIs(err, context.Canceled)
	})
}

func TestProposalMaker(t *testing.T) {
	suite.Run(t, new(testProposalMaker))
}

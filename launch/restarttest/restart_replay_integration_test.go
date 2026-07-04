//go:build test

package restarttest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacblock "github.com/imfact-labs/mitum2/isaac/block"
	isaacdatabase "github.com/imfact-labs/mitum2/isaac/database"
	isaacstates "github.com/imfact-labs/mitum2/isaac/states"
	"github.com/imfact-labs/mitum2/launch"
	leveldbstorage "github.com/imfact-labs/mitum2/storage/leveldb"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/imfact-labs/mitum2/util/logging"
	"github.com/imfact-labs/mitum2/util/valuehash"
)

const restartReplayFrontierRound base.Round = 3

// restartReplayDatabase deliberately embeds the full production interface and
// overrides only the reads exercised by this integration fixture. Consensus
// storage under test is TempPool; this object represents the committed store.
type restartReplayDatabase struct {
	isaac.Database
	blockmap  base.BlockMap
	blockmaps map[base.Height]base.BlockMap
	policy    base.NetworkPolicy
	suffrage  base.Suffrage
	other     base.LocalNode
}

func (db *restartReplayDatabase) LastBlockMap() (base.BlockMap, bool, error) {
	return db.blockmap, db.blockmap != nil, nil
}

func (db *restartReplayDatabase) BlockMap(height base.Height) (base.BlockMap, bool, error) {
	if m := db.blockmaps[height]; m != nil {
		return m, true, nil
	}
	return nil, false, nil
}

type restartReplaySuffrageProof struct {
	base.SuffrageProof
	suffrage base.Suffrage
}

func (p restartReplaySuffrageProof) Suffrage() (base.Suffrage, error) { return p.suffrage, nil }

func (db *restartReplayDatabase) SuffrageProofByBlockHeight(base.Height) (base.SuffrageProof, bool, error) {
	if db.suffrage == nil {
		return nil, false, nil
	}
	return restartReplaySuffrageProof{suffrage: db.suffrage}, true, nil
}

func (db *restartReplayDatabase) LastNetworkPolicy() base.NetworkPolicy { return db.policy }

func (*restartReplayDatabase) ExistsKnownOperation(util.Hash) (bool, error) { return false, nil }

func (*restartReplayDatabase) ExistsInStateOperation(util.Hash) (bool, error) { return false, nil }

type restartReplayRun struct {
	states        *isaacstates.States
	ballotbox     *isaacstates.Ballotbox
	broadcasts    <-chan base.Ballot
	draws         <-chan base.Voteproof
	requestPoints <-chan base.Point
	stateErrors   <-chan error
}

func TestWholeNodeRestartDoesNotReplayPersistedRounds(t *testing.T) {
	testWholeNodeRestartDoesNotReplayPersistedRounds(t, "active")
}

func TestWholeNodeRestartFailsOnCorruptConsensusSnapshot(t *testing.T) {
	testWholeNodeRestartDoesNotReplayPersistedRounds(t, "corrupt")
}

func TestWholeNodeRestartFailsOnInvalidConsensusSnapshot(t *testing.T) {
	for _, mode := range []string{"invalid-empty", "invalid-signature", "invalid-suffrage", "invalid-height"} {
		t.Run(mode, func(t *testing.T) { testWholeNodeRestartDoesNotReplayPersistedRounds(t, mode) })
	}
}

func TestWholeNodeRestartIgnoresValidStaleConsensusSnapshot(t *testing.T) {
	testWholeNodeRestartDoesNotReplayPersistedRounds(t, "stale")
}

func TestWholeNodeRestartPreservesStaleSnapshotOnFrontierConflict(t *testing.T) {
	testWholeNodeRestartDoesNotReplayPersistedRounds(t, "stale-higher")
}

func TestWholeNodeRestartSafeStopsOnUnsupportedHigherFrontier(t *testing.T) {
	testWholeNodeRestartDoesNotReplayPersistedRounds(t, "higher-frontier")
}

func TestRecoveredDrawPromotionBroadcastsAccept(t *testing.T) {
	testWholeNodeRestartDoesNotReplayPersistedRounds(t, "promotion")
}

func testWholeNodeRestartDoesNotReplayPersistedRounds(t *testing.T, mode string) {
	t.Parallel()
	// Phase 1 observes the real pre-restart States voteproof path and persists
	// its legal frontier. Phase 2 is exercised by the real PLoadFromDatabase
	// call below, which restores that frontier into the same LVPS consumed by
	// the rebuilt States. The behavioral assertions therefore remain unchanged.

	const height base.Height = 33

	root := t.TempDir()
	dataroot := launch.LocalFSDataDirectory(root)
	dbroot := filepath.Join(root, "restart-pool")
	requireNoError(t, os.MkdirAll(dataroot, 0o700))

	ctx, encs, enc := restartReplayEncoders(t)
	local := base.RandomLocalNode()
	other := base.RandomLocalNode()
	networkID := base.RandomNetworkID()
	params := isaac.DefaultParams(networkID)
	requireNoError(t, params.SetThreshold(base.Threshold(100)))

	committedPoint := base.RawPoint(int64(height-1), 0)
	committedManifest := base.NewDummyManifest(committedPoint.Height(), valuehash.RandomSHA256())
	committedINIT, committedACCEPT := restartReplayCommittedVoteproofs(
		t, []base.LocalNode{local, other}, networkID, params.Threshold(), committedPoint, committedManifest.Hash(),
	)

	blockmap := writeRestartReplayCommittedBlock(
		t, dataroot, encs, enc, local, networkID, committedManifest, committedINIT, committedACCEPT,
	)

	suffrage, _ := isaac.NewTestSuffrage(0, local, other)
	committedDB := &restartReplayDatabase{
		blockmap:  blockmap,
		blockmaps: map[base.Height]base.BlockMap{blockmap.Manifest().Height(): blockmap},
		policy:    isaac.DefaultNetworkPolicy(),
		suffrage:  suffrage,
		other:     other,
	}

	storage := leveldbstorage.NewFSStorage(dbroot)
	pool, err := isaacdatabase.NewTempPool(storage, encs, enc, 32)
	requireNoError(t, err)

	lvps := isaac.NewLastVoteproofsHandler()
	requireTrue(t, lvps.Set(committedINIT), "set committed INIT voteproof")
	requireTrue(t, lvps.Set(committedACCEPT), "set committed ACCEPT voteproof")

	pre := restartReplayStartStates(
		t, local, networkID, params, committedDB, pool, lvps, restartReplayFrontierRound,
	)

	frontier := waitRestartReplayVoteproof(t, pre.draws, height, restartReplayFrontierRound)
	if frontier.Result() != base.VoteResultDraw {
		t.Fatalf("pre-restart frontier must be DRAW, got %v", frontier.Result())
	}
	requireNoError(t, pre.states.Stop())
	requireNoError(t, pre.ballotbox.Stop())
	waitRestartReplayQuiet(t, pre.broadcasts, pre.draws)
	_, found, err := pool.Ballot(
		base.RawPoint(int64(height), uint64(restartReplayFrontierRound+1)),
		base.StageINIT,
		false,
	)
	requireNoError(t, err)
	if found {
		t.Fatal("post-frontier INIT ballot was persisted after quiesce")
	}
	if strings.HasPrefix(mode, "stale") {
		newPoint := base.RawPoint(int64(height), 0)
		newManifest := base.NewDummyManifest(newPoint.Height(), valuehash.RandomSHA256())
		newINIT, newACCEPT := restartReplayCommittedVoteproofs(
			t, []base.LocalNode{local, other}, networkID, params.Threshold(), newPoint, newManifest.Hash(),
		)
		newWriter, err := isaacblock.NewLocalFSWriter(
			dataroot, newPoint.Height(), encs.JSON(), enc, local, networkID,
		)
		requireNoError(t, err)
		requireNoError(t, newWriter.SetManifest(context.Background(), newManifest))
		requireNoError(t, newWriter.SetINITVoteproof(context.Background(), newINIT))
		requireNoError(t, newWriter.SetACCEPTVoteproof(context.Background(), newACCEPT))
		newBlockmap, err := newWriter.Save(context.Background())
		requireNoError(t, err)
		committedDB.blockmap = newBlockmap
		committedDB.blockmaps[newBlockmap.Manifest().Height()] = newBlockmap
		if mode == "stale-higher" {
			point := base.RawPoint(int64(height+1), 1)
			fact := isaac.NewINITBallotFact(point, newManifest.Hash(), valuehash.RandomSHA256(), nil)
			sf := isaac.NewINITBallotSignFact(fact)
			requireNoError(t, sf.NodeSign(local.Privatekey(), networkID, local.Address()))
			updated, err := pool.SetBallot(isaac.NewINITBallot(newACCEPT, sf, nil))
			requireNoError(t, err)
			requireTrue(t, updated, "write stale-conflict higher frontier")
		}
	} else if mode == "higher-frontier" {
		point := base.RawPoint(int64(height), uint64(restartReplayFrontierRound+2))
		fact := isaac.NewINITBallotFact(point, committedManifest.Hash(), valuehash.RandomSHA256(), nil)
		sf := isaac.NewINITBallotSignFact(fact)
		requireNoError(t, sf.NodeSign(local.Privatekey(), networkID, local.Address()))
		updated, err := pool.SetBallot(isaac.NewINITBallot(frontier, sf, nil))
		requireNoError(t, err)
		requireTrue(t, updated, "write unsupported higher ballot frontier")
	}

	operationFact := isaac.NewDummyOperationFact(base.Token("restart-op"), util.BytesToByter([]byte("new-after-frontier")))
	operation, err := isaac.NewDummyOperation(operationFact, local.Privatekey(), networkID)
	requireNoError(t, err)
	_, err = pool.SetOperation(context.Background(), operation)
	requireNoError(t, err)

	// Whole-process teardown: TempPool owns only its prefix view. The underlying
	// Storage owns the LevelDB file lock and must be closed independently.
	requireNoError(t, pool.Close())
	requireNoError(t, storage.Close())

	storage = leveldbstorage.NewFSStorage(dbroot)
	defer storage.Close()
	pool, err = isaacdatabase.NewTempPool(storage, encs, enc, 32)
	requireNoError(t, err)
	defer pool.Close()
	if mode == "corrupt" {
		key := append(append([]byte{}, pool.Prefix()...), 0x02, 0x13)
		requireNoError(t, storage.Put(key, []byte("corrupt snapshot"), nil))
	} else if strings.HasPrefix(mode, "invalid-") {
		snapshot, found, err := pool.DurableConsensusSnapshot()
		requireNoError(t, err)
		requireTrue(t, found, "durable snapshot missing before invalid fixture mutation")
		if mode == "invalid-height" {
			snapshot.ManifestHeight--
		} else {
			bad := isaac.NewINITVoteproof(snapshot.INIT.Point().Point)
			bad.SetThreshold(params.Threshold())
			if mode != "invalid-empty" {
				fact := isaac.NewINITBallotFact(
					snapshot.INIT.Point().Point, snapshot.PreviousBlock, valuehash.RandomSHA256(), nil,
				)
				sf := isaac.NewINITBallotSignFact(fact)
				signer := base.RandomLocalNode()
				signNetworkID := networkID
				if mode == "invalid-signature" {
					signNetworkID = base.RandomNetworkID()
				}
				requireNoError(t, sf.NodeSign(signer.Privatekey(), signNetworkID, signer.Address()))
				bad.SetSignFacts([]base.BallotSignFact{sf})
			}
			bad.Finish()
			snapshot.INIT = bad
		}
		requireNoError(t, pool.RemoveDurableConsensusSnapshot())
		updated, err := pool.SetDurableConsensusSnapshot(snapshot)
		requireNoError(t, err)
		requireTrue(t, updated, "write invalid snapshot fixture")
	}

	// Phase 2 may add a production snapshot reader dependency here (for example,
	// the reopened pool/storage). Keep the behavioral assertions below unchanged;
	// only extend this loader context when PLoadFromDatabase gains that dependency.
	loaderctx := util.ContextWithValues(ctx, map[util.ContextKey]interface{}{
		launch.DesignContextKey: launch.NodeDesign{
			Storage: launch.NodeStorageDesign{Base: root}, LocalParams: &launch.LocalParams{ISAAC: params},
		},
		launch.CenterDatabaseContextKey: isaac.Database(committedDB),
		launch.PoolDatabaseContextKey:   pool,
		launch.LeveldbStorageContextKey: storage,
	})
	loaderctx, err = launch.PLoadFromDatabase(loaderctx)
	if mode == "corrupt" || mode == "higher-frontier" || mode == "stale-higher" || strings.HasPrefix(mode, "invalid-") {
		if err == nil {
			t.Fatal("invalid durable consensus snapshot silently fell back")
		}
		if mode == "stale-higher" {
			_, found, ferr := pool.DurableConsensusSnapshot()
			requireNoError(t, ferr)
			requireTrue(t, found, "stale conflict removed diagnostic snapshot")
		}
		return
	}
	requireNoError(t, err)

	var recoveredLVPS *isaac.LastVoteproofsHandler
	requireNoError(t, util.LoadFromContextOK(loaderctx, launch.LastVoteproofsHandlerContextKey, &recoveredLVPS))
	if mode == "stale" {
		_, found, err := pool.DurableConsensusSnapshot()
		requireNoError(t, err)
		if found {
			t.Fatal("valid stale consensus snapshot was not removed")
		}
		if recoveredLVPS.Last().Cap().Point().Height() != height {
			t.Fatalf("clean start cap=%v want committed height=%d", recoveredLVPS.Last().Cap().Point(), height)
		}
		return
	}
	if mode == "promotion" {
		var boundary base.StagePoint
		requireNoError(t, util.LoadFromContextOK(loaderctx, launch.NoSignBeforeContextKey, &boundary))
		snapshot, found, err := pool.DurableConsensusSnapshot()
		requireNoError(t, err)
		requireTrue(t, found, "promotion snapshot missing")
		point := snapshot.Cap().Point().Point
		proposalFact := isaac.NewProposalFact(point, local.Address(), snapshot.PreviousBlock, nil)
		proposal := isaac.NewProposalSignFact(proposalFact)
		requireNoError(t, proposal.Sign(local.Privatekey(), networkID))
		_, err = pool.SetProposal(proposal)
		requireNoError(t, err)
		majorityFact := isaac.NewINITBallotFact(point, snapshot.PreviousBlock, proposal.ProposalFact().Hash(), nil)
		promotionBallots := make([]base.Ballot, 0, 2)
		for _, node := range []base.LocalNode{local, other} {
			sf := isaac.NewINITBallotSignFact(majorityFact)
			requireNoError(t, sf.NodeSign(node.Privatekey(), networkID, node.Address()))
			promotionBallots = append(promotionBallots, isaac.NewINITBallot(snapshot.ACCEPT, sf, nil))
		}
		post := restartReplayStartStatesWithObserverAndBoundary(
			t, local, networkID, params, committedDB, pool, recoveredLVPS, boundary, promotionBallots,
			func(ctx context.Context, height base.Height, limit uint64) ([][2]util.Hash, error) {
				return pool.OperationHashes(ctx, height, limit, nil)
			},
		)
		defer func() {
			_ = post.states.Stop()
			_ = post.ballotbox.Stop()
			waitRestartReplayQuiet(t, post.broadcasts, post.draws)
		}()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case bl := <-post.broadcasts:
				if bl.Point().Stage() == base.StageACCEPT {
					if !bl.Point().Point.Equal(point) {
						t.Fatalf("promotion ACCEPT point=%v want=%v", bl.Point().Point, point)
					}
					stored, found, err := pool.Ballot(point, base.StageACCEPT, false)
					requireNoError(t, err)
					requireTrue(t, found, "promotion ACCEPT was broadcast before DB save")
					if !stored.Point().Equal(bl.Point()) {
						t.Fatalf("stored promotion ACCEPT=%v broadcast=%v", stored.Point(), bl.Point())
					}
					return
				}
			case <-timer.C:
				t.Fatalf("timed out waiting for promoted ACCEPT broadcast; ballotbox last=%v recovered=%v", post.ballotbox.LastPoint(), recoveredLVPS.Last().Cap().Point())
			case err := <-post.stateErrors:
				t.Fatalf("states stopped before promoted ACCEPT: %v", err)
			}
		}
	}

	var operationHashCalls atomic.Int64
	post := restartReplayStartStatesWithObserver(
		t, local, networkID, params, committedDB, pool, recoveredLVPS,
		func(ctx context.Context, height base.Height, limit uint64) ([][2]util.Hash, error) {
			operationHashCalls.Add(1)

			return pool.OperationHashes(ctx, height, limit, nil)
		},
	)
	defer func() {
		_ = post.states.Stop()
		_ = post.ballotbox.Stop()
		waitRestartReplayQuiet(t, post.broadcasts, post.draws)
	}()

	first := waitRestartReplayBallot(t, post.broadcasts)
	want := base.RawPoint(int64(height), uint64(restartReplayFrontierRound+1))
	if !first.Point().Point.Equal(want) {
		t.Errorf("first INIT after restart = %v, want recovered frontier %v", first.Point().Point, want)
	}

	if first.Point().Round() < restartReplayFrontierRound {
		t.Errorf("historical round replayed after restart: first INIT=%v", first.Point().Point)
	}

	if got := operationHashCalls.Load(); got < 1 {
		t.Errorf("new operation did not reach OperationHashes within first INIT; calls=%d", got)
	}
	select {
	case point := <-post.requestPoints:
		if !point.Equal(want) {
			t.Errorf("requestProposal reached wrong point first: got=%v want=%v", point, want)
		}
	default:
		t.Errorf("first INIT bypassed requestProposal")
	}

	prfact, ok := first.SignFact().Fact().(base.INITBallotFact)
	if ok && operationHashCalls.Load() > 0 {
		proposal, found, ferr := pool.Proposal(prfact.Proposal())
		requireNoError(t, ferr)
		if !found || !containsRestartReplayOperation(proposal, operation.Hash()) {
			t.Errorf("new operation %s was not included by first post-restart proposal", operation.Hash())
		}
	}
}

func writeRestartReplayCommittedBlock(
	t *testing.T,
	dataroot string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	local base.LocalNode,
	networkID base.NetworkID,
	manifest base.Manifest,
	committedINIT base.INITVoteproof,
	committedACCEPT base.ACCEPTVoteproof,
) base.BlockMap {
	t.Helper()
	writer, err := isaacblock.NewLocalFSWriter(
		dataroot, manifest.Height(), encs.JSON(), enc, local, networkID,
	)
	requireNoError(t, err)
	requireNoError(t, writer.SetManifest(context.Background(), manifest))
	requireNoError(t, writer.SetINITVoteproof(context.Background(), committedINIT))
	requireNoError(t, writer.SetACCEPTVoteproof(context.Background(), committedACCEPT))
	blockmap, err := writer.Save(context.Background())
	requireNoError(t, err)
	return blockmap
}

func restartReplayEncoders(t *testing.T) (context.Context, *encoder.Encoders, encoder.Encoder) {
	t.Helper()

	ctx := context.WithValue(context.Background(), launch.LoggingContextKey, logging.TestNilLogging)
	var err error
	ctx, err = launch.PEncoder(ctx)
	requireNoError(t, err)
	ctx, err = launch.PAddHinters(ctx)
	requireNoError(t, err)
	ctx, err = launch.PBlockItemReadersDecompressFunc(ctx)
	requireNoError(t, err)
	ctx, err = launch.PBlockItemReaders(ctx)
	requireNoError(t, err)
	ctx, err = launch.PRemotesBlockItemReaderFunc(ctx)
	requireNoError(t, err)

	var encs *encoder.Encoders
	requireNoError(t, util.LoadFromContextOK(ctx, launch.EncodersContextKey, &encs))
	requireNoError(t, encs.AddDetail(encoder.DecodeDetail{Hint: base.DummyManifestHint, Instance: base.DummyManifest{}}))
	requireNoError(t, encs.AddDetail(encoder.DecodeDetail{Hint: isaac.DummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))
	requireNoError(t, encs.AddDetail(encoder.DecodeDetail{Hint: isaac.DummyOperationHint, Instance: isaac.DummyOperation{}}))

	return ctx, encs, encs.Default()
}

func restartReplayCommittedVoteproofs(
	t *testing.T,
	nodes []base.LocalNode,
	networkID base.NetworkID,
	threshold base.Threshold,
	point base.Point,
	newBlock util.Hash,
) (base.INITVoteproof, base.ACCEPTVoteproof) {
	t.Helper()

	proposal := valuehash.RandomSHA256()
	initFact := isaac.NewINITBallotFact(point, valuehash.RandomSHA256(), proposal, nil)
	initSigns := make([]base.BallotSignFact, len(nodes))
	acceptSigns := make([]base.BallotSignFact, len(nodes))
	for i := range nodes {
		initSign := isaac.NewINITBallotSignFact(initFact)
		requireNoError(t, initSign.NodeSign(nodes[i].Privatekey(), networkID, nodes[i].Address()))
		initSigns[i] = initSign
	}
	ivp := isaac.NewINITVoteproof(point)
	ivp.SetMajority(initFact).SetSignFacts(initSigns).SetThreshold(threshold).Finish()

	acceptFact := isaac.NewACCEPTBallotFact(point, proposal, newBlock, nil)
	for i := range nodes {
		acceptSign := isaac.NewACCEPTBallotSignFact(acceptFact)
		requireNoError(t, acceptSign.NodeSign(nodes[i].Privatekey(), networkID, nodes[i].Address()))
		acceptSigns[i] = acceptSign
	}
	avp := isaac.NewACCEPTVoteproof(point)
	avp.SetMajority(acceptFact).SetSignFacts(acceptSigns).SetThreshold(threshold).Finish()

	return ivp, avp
}

func restartReplayStartStates(
	t *testing.T,
	local base.LocalNode,
	networkID base.NetworkID,
	params *isaac.Params,
	db *restartReplayDatabase,
	pool *isaacdatabase.TempPool,
	lvps *isaac.LastVoteproofsHandler,
	frontier base.Round,
) restartReplayRun {
	t.Helper()

	return restartReplayStartStatesInternal(t, local, networkID, params, db, pool, lvps, frontier, nil, nil, base.StagePoint{}, nil)
}

func restartReplayStartStatesWithObserver(
	t *testing.T,
	local base.LocalNode,
	networkID base.NetworkID,
	params *isaac.Params,
	db *restartReplayDatabase,
	pool *isaacdatabase.TempPool,
	lvps *isaac.LastVoteproofsHandler,
	opf func(context.Context, base.Height, uint64) ([][2]util.Hash, error),
) restartReplayRun {
	t.Helper()

	return restartReplayStartStatesInternal(t, local, networkID, params, db, pool, lvps, ^base.Round(0), opf, nil, base.StagePoint{}, nil)
}

func restartReplayStartStatesWithObserverAndBoundary(
	t *testing.T,
	local base.LocalNode,
	networkID base.NetworkID,
	params *isaac.Params,
	db *restartReplayDatabase,
	pool *isaacdatabase.TempPool,
	lvps *isaac.LastVoteproofsHandler,
	boundary base.StagePoint,
	preStartBallots []base.Ballot,
	opf func(context.Context, base.Height, uint64) ([][2]util.Hash, error),
) restartReplayRun {
	t.Helper()
	return restartReplayStartStatesInternal(t, local, networkID, params, db, pool, lvps, ^base.Round(0), opf, nil, boundary, preStartBallots)
}

func restartReplayStartStatesWithWriter(
	t *testing.T,
	local base.LocalNode,
	networkID base.NetworkID,
	params *isaac.Params,
	db *restartReplayDatabase,
	pool *isaacdatabase.TempPool,
	lvps *isaac.LastVoteproofsHandler,
	frontier base.Round,
	writer launch.SetDurableConsensusSnapshotFunc,
) restartReplayRun {
	t.Helper()
	return restartReplayStartStatesInternal(t, local, networkID, params, db, pool, lvps, frontier, nil, writer, base.StagePoint{}, nil)
}

func restartReplayStartStatesInternal(
	t *testing.T,
	local base.LocalNode,
	networkID base.NetworkID,
	params *isaac.Params,
	db *restartReplayDatabase,
	pool *isaacdatabase.TempPool,
	lvps *isaac.LastVoteproofsHandler,
	frontier base.Round,
	observedOpf func(context.Context, base.Height, uint64) ([][2]util.Hash, error),
	writer launch.SetDurableConsensusSnapshotFunc,
	noSignBefore base.StagePoint,
	preStartBallots []base.Ballot,
) restartReplayRun {
	t.Helper()

	suffrage := db.suffrage
	nodeInConsensus := func(node base.Node, _ base.Height) (base.Suffrage, bool, error) {
		return suffrage, suffrage.Exists(node.Address()), nil
	}
	getSuffrage := func(base.Height) (base.Suffrage, bool, error) { return suffrage, true, nil }

	ballotbox := isaacstates.NewBallotbox(local.Address(), params.Threshold, getSuffrage).
		SetCountAfter(0).
		SetInterval(time.Millisecond)
	broadcasts := make(chan base.Ballot, 64)
	draws := make(chan base.Voteproof, 64)
	requestPoints := make(chan base.Point, 64)

	var maker *isaac.ProposalMaker
	opf := func(ctx context.Context, height base.Height) ([][2]util.Hash, error) {
		if observedOpf != nil {
			return observedOpf(ctx, height, 100)
		}

		return pool.OperationHashes(ctx, height, 100, nil)
	}
	maker = isaac.NewProposalMaker(local, networkID, opf, pool, db.LastBlockMap)

	selectorArgs := isaac.NewBaseProposalSelectorArgs()
	selectorArgs.Pool = pool
	selectorArgs.Maker = maker
	selectorArgs.GetNodesFunc = func(base.Height) ([]base.Node, bool, error) {
		return []base.Node{local}, true, nil
	}
	selectorArgs.ProposerSelectFunc = isaac.NewFixedProposerSelector(
		func(base.Point, []base.Node, util.Hash) (base.Node, error) { return local, nil },
	).Select
	selectorArgs.MinProposerWait = func() time.Duration { return 0 }
	selector := isaac.NewBaseProposalSelector(local, selectorArgs)
	selectProposal := func(
		ctx context.Context, point base.Point, previousBlock util.Hash, wait time.Duration,
	) (base.ProposalSignFact, error) {
		requestPoints <- point

		return selector.Select(ctx, point, previousBlock, wait)
	}

	broadcaster := isaacstates.NewDefaultBallotBroadcaster(local.Address(), pool, func(bl base.Ballot) error {
		broadcasts <- bl
		if observedOpf != nil {
			return nil
		}
		if _, err := ballotbox.Vote(bl); err != nil {
			return err
		}
		if bl.Point().Stage() == base.StageINIT {
			fact := bl.SignFact().Fact().(base.INITBallotFact) //nolint:forcetypeassert // fixture INIT path
			otherFact := isaac.NewINITBallotFact(
				fact.Point().Point, fact.PreviousBlock(), valuehash.RandomSHA256(), nil,
			)
			otherSign := isaac.NewINITBallotSignFact(otherFact)
			if err := otherSign.NodeSign(db.other.Privatekey(), networkID, db.other.Address()); err != nil {
				return err
			}
			_, err := ballotbox.Vote(isaac.NewINITBallot(bl.Voteproof(), otherSign, nil))
			return err
		}
		return nil
	})

	statesArgs := isaacstates.NewStatesArgs()
	statesArgs.Ballotbox = ballotbox
	statesArgs.BallotBroadcaster = broadcaster
	statesArgs.LastVoteproofsHandler = lvps
	statesArgs.NoSignBefore = noSignBefore
	persistConsensusProgress := launch.NewPersistConsensusProgressFunc(logging.TestNilLogging, pool, db, writer)
	statesArgs.PersistConsensusProgress = func(vp base.Voteproof, previous isaac.LastVoteproofs) error {
		return persistConsensusProgress(vp, previous)
	}
	statesArgs.AllowConsensus = true
	statesArgs.IntervalBroadcastBallot = func() time.Duration { return time.Hour }
	statesArgs.BroadcastTimerMult = func() int { return 1 }
	statesArgs.WhenNewVoteproof = func(vp base.Voteproof) {
		if vp.Point().Stage() == base.StageINIT && vp.Result() == base.VoteResultDraw {
			draws <- vp
		}
	}
	var preStartCallbacks chan struct{}
	if len(preStartBallots) > 0 {
		preStartCallbacks = make(chan struct{}, len(preStartBallots))
		ballotbox.SetNewBallotFunc(func(base.Ballot) { preStartCallbacks <- struct{}{} })
	}
	for i := range preStartBallots {
		_, err := ballotbox.Vote(preStartBallots[i])
		requireNoError(t, err)
	}
	for range preStartBallots {
		select {
		case <-preStartCallbacks:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for pre-start ballot callback")
		}
	}
	if len(preStartBallots) > 0 {
		_ = ballotbox.Count()
		vp := ballotbox.LastVoteproof()
		if vp == nil || vp.Result() != base.VoteResultMajority || !vp.Point().Equal(preStartBallots[0].Point()) {
			t.Fatalf("pre-start quorum ballots did not produce majority voteproof: %v", vp)
		}
	}

	states, err := isaacstates.NewStates(networkID, local, statesArgs)
	requireNoError(t, err)

	lastManifest := func() (base.Manifest, bool, error) { return db.blockmap.Manifest(), true, nil }
	vote := func(bl base.Ballot) (bool, error) { return ballotbox.Vote(bl) }
	findExpels := func(context.Context, base.Height, base.Suffrage) ([]base.SuffrageExpelOperation, error) {
		return nil, nil
	}
	isEmpty := func(_ context.Context, pr base.ProposalSignFact) (bool, error) {
		return len(pr.ProposalFact().Operations()) < 1, nil
	}

	bootingArgs := isaacstates.NewBootingHandlerArgs()
	bootingArgs.LastManifestFunc = lastManifest
	bootingArgs.NodeInConsensusNodesFunc = nodeInConsensus

	joiningArgs := isaacstates.NewJoiningHandlerArgs()
	joiningArgs.LastManifestFunc = lastManifest
	joiningArgs.NodeInConsensusNodesFunc = nodeInConsensus
	joiningArgs.ProposalSelectFunc = selectProposal
	joiningArgs.VoteFunc = vote
	joiningArgs.SuffrageVotingFindFunc = findExpels
	joiningArgs.JoinMemberlistFunc = func(context.Context, base.Suffrage) error { return nil }
	joiningArgs.LeaveMemberlistFunc = func() error { return nil }
	joiningArgs.WaitFirstVoteproof = func() time.Duration { return 0 }
	joiningArgs.WaitPreparingINITBallot = func() time.Duration { return 200 * time.Millisecond }
	joiningArgs.MinWaitNextBlockINITBallot = func() time.Duration { return 0 }
	joiningArgs.IsEmptyProposalFunc = isEmpty
	joiningArgs.IsEmptyProposalNoBlockFunc = func() bool { return true }

	consensusArgs := isaacstates.NewConsensusHandlerArgs()
	consensusArgs.NodeInConsensusNodesFunc = nodeInConsensus
	consensusArgs.ProposalSelectFunc = selectProposal
	consensusArgs.VoteFunc = vote
	consensusArgs.SuffrageVotingFindFunc = findExpels
	consensusArgs.WaitPreparingINITBallot = func() time.Duration { return 200 * time.Millisecond }
	consensusArgs.MinWaitNextBlockINITBallot = func() time.Duration { return 0 }
	consensusArgs.IsEmptyProposalFunc = isEmpty
	consensusArgs.IsEmptyProposalNoBlockFunc = func() bool { return true }
	consensusArgs.ProposalProcessors = isaac.NewProposalProcessors(nil, nil)
	if observedOpf != nil {
		dummyProcessor := isaac.NewDummyProposalProcessor()
		dummyProcessor.Processerr = func(context.Context, base.ProposalFact, base.INITVoteproof) (base.Manifest, error) {
			return base.NewDummyManifest(db.blockmap.Manifest().Height()+1, valuehash.RandomSHA256()), nil
		}
		consensusArgs.ProposalProcessors = isaac.NewProposalProcessors(
			dummyProcessor.Make,
			func(_ context.Context, _ base.Point, hash util.Hash) (base.ProposalSignFact, error) {
				proposal, found, err := pool.Proposal(hash)
				if err != nil {
					return nil, err
				}
				if !found {
					return nil, util.ErrNotFound.Errorf("proposal %s", hash)
				}
				return proposal, nil
			},
		)
	}
	consensusArgs.GetManifestFunc = func(base.Height) (base.Manifest, error) { return db.blockmap.Manifest(), nil }

	brokenArgs := isaacstates.NewBrokenHandlerArgs()
	brokenArgs.LeaveMemberlistFunc = func() error { return nil }

	states.
		SetHandler(isaacstates.StateStopped, isaacstates.NewNewStoppedHandlerType(networkID, local)).
		SetHandler(isaacstates.StateBooting, isaacstates.NewNewBootingHandlerType(networkID, local, bootingArgs)).
		SetHandler(isaacstates.StateJoining, isaacstates.NewNewJoiningHandlerType(networkID, local, joiningArgs)).
		SetHandler(isaacstates.StateConsensus, isaacstates.NewNewConsensusHandlerType(networkID, local, consensusArgs)).
		SetHandler(isaacstates.StateBroken, isaacstates.NewNewBrokenHandlerType(networkID, local, brokenArgs))
	_ = states.SetLogging(logging.TestNilLogging)

	requireNoError(t, ballotbox.Start(context.Background()))
	stateErrors := states.Wait(context.Background())

	if frontier != ^base.Round(0) {
		for r := base.Round(0); r <= frontier; r++ {
			bl := waitRestartReplayBallot(t, broadcasts)
			if !bl.Point().Point.Equal(base.RawPoint(int64(db.blockmap.Manifest().Height()+1), uint64(r))) {
				t.Fatalf("pre-restart INIT sequence: got %v want round %d", bl.Point().Point, r)
			}
		}
	}

	return restartReplayRun{
		states: states, ballotbox: ballotbox, broadcasts: broadcasts, draws: draws, requestPoints: requestPoints,
		stateErrors: stateErrors,
	}
}

func waitRestartReplayBallot(t *testing.T, ch <-chan base.Ballot) base.Ballot {
	t.Helper()
	select {
	case bl := <-ch:
		return bl
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for INIT ballot")
		return nil
	}
}

func waitRestartReplayVoteproof(t *testing.T, ch <-chan base.Voteproof, height base.Height, round base.Round) base.Voteproof {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case vp := <-ch:
			if vp.Point().Height() == height && vp.Point().Round() == round {
				return vp
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for DRAW at (%d,%d)", height, round)
			return nil
		}
	}
}

func waitRestartReplayQuiet(t *testing.T, broadcasts <-chan base.Ballot, draws <-chan base.Voteproof) {
	t.Helper()

	// WaitPreparingINITBallot is 200ms in this fixture. Requiring a substantially
	// longer quiet period drains handler-owned nextRound goroutines before the
	// LevelDB-backed pool is closed.
	quiet := time.NewTimer(750 * time.Millisecond)
	defer quiet.Stop()

	for {
		select {
		case <-broadcasts:
			if !quiet.Stop() {
				<-quiet.C
			}
			quiet.Reset(750 * time.Millisecond)
		case <-draws:
			if !quiet.Stop() {
				<-quiet.C
			}
			quiet.Reset(750 * time.Millisecond)
		case <-quiet.C:
			return
		}
	}
}

func containsRestartReplayOperation(pr base.ProposalSignFact, operation util.Hash) bool {
	if pr == nil {
		return false
	}
	for _, pair := range pr.ProposalFact().Operations() {
		if pair[0].Equal(operation) {
			return true
		}
	}

	return false
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func requireTrue(t *testing.T, ok bool, message string) {
	t.Helper()
	if !ok {
		t.Fatal(message)
	}
}

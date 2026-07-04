//go:build test

package restarttest

// Multi-node convergence harness (ISSUE-009 멀티노드 수렴 Part B).
//
// This file builds N independent nodes, each with its own on-disk TempPool and a
// pre-restart signing history that is fully consistent: every INIT DRAW voteproof
// used in a snapshot is assembled from the exact SignFacts of the cached ballots
// stored in the signer nodes' pools (no synthetic address-only sign facts).
//
// Recovery always goes through the real launch.PLoadFromDatabase; LVPS is never
// injected directly. Rounds are kept small (R0..R3) for readability.
//
// NOTE: this first implementation unit (no commit is created) lands the
// deterministic fixture + recovery foundation and the gated router, and verifies
// the consistency invariant plus the REQ-NET-005 single-ballot forward jump.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
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

// mnRouter delivers ballots only to registered (Started) nodes. A broadcaster
// callback enqueues and returns; a delivery goroutine calls Ballotbox.Vote
// strictly outside the router lock. Per-target/source/ballot gating is explicit
// (no sleep-based ordering).
type mnDelivery struct {
	source, target string
	point          base.StagePoint
	allowed        bool // gate accepted delivery
	voted          bool // Ballotbox.Vote bool return
	err            error
}

type mnRouter struct {
	mu         sync.Mutex
	boxes      map[string]*isaacstates.Ballotbox
	allow      func(target, source string, bl base.Ballot) bool
	queue      chan mnRouterItem
	deliveries []mnDelivery
	overflow   int
	errs       chan error
	done       chan struct{}
	stop       chan struct{}
}

type mnRouterItem struct {
	source string
	bl     base.Ballot
	flush  chan struct{} // barrier marker (no delivery)
}

func newMNRouter() *mnRouter {
	r := &mnRouter{
		boxes: map[string]*isaacstates.Ballotbox{},
		allow: func(string, string, base.Ballot) bool { return true },
		queue: make(chan mnRouterItem, 1024),
		errs:  make(chan error, 256),
		done:  make(chan struct{}),
		stop:  make(chan struct{}),
	}
	go r.run()
	return r
}

func (r *mnRouter) run() {
	defer close(r.done)
	for {
		var item mnRouterItem
		select {
		case <-r.stop:
			return
		case item = <-r.queue:
		}
		if item.flush != nil {
			close(item.flush)
			continue
		}
		r.mu.Lock()
		allow := r.allow
		type tgt struct {
			addr string
			box  *isaacstates.Ballotbox
		}
		targets := make([]tgt, 0, len(r.boxes))
		for addr, box := range r.boxes {
			targets = append(targets, tgt{addr, box})
		}
		r.mu.Unlock()

		for _, tg := range targets {
			accepted := allow(tg.addr, item.source, item.bl)
			var voted bool
			var verr error
			if accepted {
				voted, verr = tg.box.Vote(item.bl) // strictly outside the router lock
			}
			r.mu.Lock()
			r.deliveries = append(r.deliveries, mnDelivery{
				source:  item.source,
				target:  tg.addr,
				point:   item.bl.Point(),
				allowed: accepted,
				voted:   voted,
				err:     verr,
			})
			r.mu.Unlock()
			if verr != nil {
				select {
				case r.errs <- verr:
				default:
				}
			}
		}
	}
}

func (r *mnRouter) register(addr string, box *isaacstates.Ballotbox) {
	r.mu.Lock()
	r.boxes[addr] = box
	r.mu.Unlock()
}

func (r *mnRouter) unregister(addr string) {
	r.mu.Lock()
	delete(r.boxes, addr)
	r.mu.Unlock()
}

func (r *mnRouter) setAllow(f func(target, source string, bl base.Ballot) bool) {
	r.mu.Lock()
	r.allow = f
	r.mu.Unlock()
}

// enqueue never blocks the caller (consensus goroutine); overflow is recorded
// explicitly so a test can assert no ballot was dropped.
func (r *mnRouter) enqueue(source string, bl base.Ballot) bool {
	select {
	case r.queue <- mnRouterItem{source: source, bl: bl}:
		return true
	default:
		r.mu.Lock()
		r.overflow++
		r.mu.Unlock()
		return false
	}
}

// Flush blocks until every item enqueued before it has been delivered.
func (r *mnRouter) Flush() {
	ch := make(chan struct{})
	r.queue <- mnRouterItem{flush: ch}
	<-ch
}

func (r *mnRouter) close() {
	close(r.stop)
	<-r.done
}

func (r *mnRouter) drainErrors() []error {
	var out []error
	for {
		select {
		case e := <-r.errs:
			out = append(out, e)
		default:
			return out
		}
	}
}

func (r *mnRouter) overflowCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.overflow
}

func (r *mnRouter) snapshotDeliveries() []mnDelivery {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]mnDelivery, len(r.deliveries))
	copy(out, r.deliveries)

	return out
}

// mnNode is a live node: real States wired to the shared router.
type mnNode struct {
	local       base.LocalNode
	states      *isaacstates.States
	ballotbox   *isaacstates.Ballotbox
	pool        *isaacdatabase.TempPool
	storage     *leveldbstorage.Storage
	lvps        *isaac.LastVoteproofsHandler
	broadcasts  chan base.Ballot
	draws       chan base.Voteproof
	stateErrors <-chan error
	router      *mnRouter
	ready       chan struct{}
	release     chan struct{}
}

func (n *mnNode) stop() {
	n.router.unregister(n.local.Address().String())
	_ = n.states.Stop()
	_ = n.ballotbox.Stop()
}

// mnPrepareNode builds a real States for node, wires its broadcaster to the
// router, but does not register or start it yet. A readiness barrier pauses the
// first observable state switch until the caller explicitly registers/releases
// the node, and the broadcaster also waits on the same release gate. Together
// they ensure no ballot is received before States.current() exists and no
// startup ballot can race ahead of router registration.
func (env *mnEnv) mnPrepareNode(
	t *testing.T,
	node base.LocalNode,
	pool *isaacdatabase.TempPool,
	storage *leveldbstorage.Storage,
	lvps *isaac.LastVoteproofsHandler,
	noSignBefore base.StagePoint,
	router *mnRouter,
	proposer base.LocalNode,
	requestFunc func(context.Context, base.Point, base.Node, util.Hash) (base.ProposalSignFact, bool, error),
) *mnNode {
	t.Helper()

	db := env.mnDatabase()
	nodeInConsensus := func(nd base.Node, _ base.Height) (base.Suffrage, bool, error) {
		return env.suffrage, env.suffrage.Exists(nd.Address()), nil
	}
	getSuffrage := func(base.Height) (base.Suffrage, bool, error) { return env.suffrage, true, nil }

	ballotbox := isaacstates.NewBallotbox(node.Address(), env.params.Threshold, getSuffrage).
		SetCountAfter(0).SetInterval(time.Millisecond)
	broadcasts := make(chan base.Ballot, 64)
	draws := make(chan base.Voteproof, 64)
	ready := make(chan struct{})
	release := make(chan struct{})
	var switchReady atomic.Bool

	opf := func(ctx context.Context, height base.Height) ([][2]util.Hash, error) {
		return pool.OperationHashes(ctx, height, 100, nil)
	}
	maker := isaac.NewProposalMaker(node, env.networkID, opf, pool, db.LastBlockMap)

	selectorArgs := isaac.NewBaseProposalSelectorArgs()
	selectorArgs.Pool = pool
	selectorArgs.Maker = maker
	selectorArgs.GetNodesFunc = func(base.Height) ([]base.Node, bool, error) {
		out := make([]base.Node, len(env.nodes))
		for i := range env.nodes {
			out[i] = env.nodes[i]
		}
		return out, true, nil
	}
	selectorArgs.ProposerSelectFunc = isaac.NewFixedProposerSelector(
		func(base.Point, []base.Node, util.Hash) (base.Node, error) { return proposer, nil },
	).Select
	selectorArgs.RequestFunc = requestFunc
	selectorArgs.MinProposerWait = func() time.Duration { return 0 }
	selectorArgs.TimeoutRequest = func() time.Duration { return time.Second }
	selector := isaac.NewBaseProposalSelector(node, selectorArgs)
	selectProposal := func(ctx context.Context, point base.Point, prev util.Hash, wait time.Duration) (base.ProposalSignFact, error) {
		return selector.Select(ctx, point, prev, wait)
	}

	broadcaster := isaacstates.NewDefaultBallotBroadcaster(node.Address(), pool, func(bl base.Ballot) error {
		<-release
		router.enqueue(node.Address().String(), bl)
		select {
		case broadcasts <- bl:
		default:
		}
		return nil
	})

	statesArgs := isaacstates.NewStatesArgs()
	statesArgs.Ballotbox = ballotbox
	statesArgs.BallotBroadcaster = broadcaster
	statesArgs.LastVoteproofsHandler = lvps
	statesArgs.NoSignBefore = noSignBefore
	persist := launch.NewPersistConsensusProgressFunc(logging.TestNilLogging, pool, db)
	statesArgs.PersistConsensusProgress = func(vp base.Voteproof, prev isaac.LastVoteproofs) error {
		return persist(vp, prev)
	}
	statesArgs.AllowConsensus = true
	statesArgs.IntervalBroadcastBallot = func() time.Duration { return time.Hour }
	statesArgs.BroadcastTimerMult = func() int { return 1 }
	statesArgs.WhenNewVoteproof = func(vp base.Voteproof) {
		if vp.Point().Stage() == base.StageINIT && vp.Result() == base.VoteResultDraw {
			select {
			case draws <- vp:
			default:
			}
		}
	}

	states, err := isaacstates.NewStates(env.networkID, node, statesArgs)
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
	joiningArgs.WaitPreparingINITBallot = func() time.Duration { return 100 * time.Millisecond }
	joiningArgs.MinWaitNextBlockINITBallot = func() time.Duration { return 0 }
	joiningArgs.IsEmptyProposalFunc = isEmpty
	joiningArgs.IsEmptyProposalNoBlockFunc = func() bool { return true }

	consensusArgs := isaacstates.NewConsensusHandlerArgs()
	consensusArgs.NodeInConsensusNodesFunc = nodeInConsensus
	consensusArgs.ProposalSelectFunc = selectProposal
	consensusArgs.VoteFunc = vote
	consensusArgs.SuffrageVotingFindFunc = findExpels
	consensusArgs.WaitPreparingINITBallot = func() time.Duration { return 100 * time.Millisecond }
	consensusArgs.MinWaitNextBlockINITBallot = func() time.Duration { return 0 }
	consensusArgs.IsEmptyProposalFunc = isEmpty
	consensusArgs.IsEmptyProposalNoBlockFunc = func() bool { return true }
	consensusArgs.ProposalProcessors = isaac.NewProposalProcessors(nil, nil)
	consensusArgs.GetManifestFunc = func(base.Height) (base.Manifest, error) { return db.blockmap.Manifest(), nil }

	brokenArgs := isaacstates.NewBrokenHandlerArgs()
	brokenArgs.LeaveMemberlistFunc = func() error { return nil }

	states.
		SetHandler(isaacstates.StateStopped, isaacstates.NewNewStoppedHandlerType(env.networkID, node)).
		SetHandler(isaacstates.StateBooting, isaacstates.NewNewBootingHandlerType(env.networkID, node, bootingArgs)).
		SetHandler(isaacstates.StateJoining, isaacstates.NewNewJoiningHandlerType(env.networkID, node, joiningArgs)).
		SetHandler(isaacstates.StateConsensus, isaacstates.NewNewConsensusHandlerType(env.networkID, node, consensusArgs)).
		SetHandler(isaacstates.StateBroken, isaacstates.NewNewBrokenHandlerType(env.networkID, node, brokenArgs))
	_ = states.SetLogging(logging.TestNilLogging)
	states.SetWhenStateSwitched(func(st isaacstates.StateType) {
		if st == isaacstates.StateStopped {
			return
		}
		if switchReady.CompareAndSwap(false, true) {
			close(ready)
			<-release
		}
	})

	requireNoError(t, ballotbox.Start(context.Background()))

	return &mnNode{
		local: node, states: states, ballotbox: ballotbox, pool: pool, storage: storage, lvps: lvps,
		broadcasts: broadcasts, draws: draws, router: router, ready: ready, release: release,
	}
}

// startWaitReady launches the node's States and blocks until the first
// state-switched callback has established States.current() and is waiting on
// release.
func (n *mnNode) startWaitReady(t *testing.T) {
	t.Helper()
	n.stateErrors = n.states.Wait(context.Background())
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-n.ready:
	case <-timer.C:
		t.Fatalf("timed out waiting for node %s startup readiness", n.local.Address())
	}
}

func (n *mnNode) register() {
	n.router.register(n.local.Address().String(), n.ballotbox)
}

func (n *mnNode) releaseStart() {
	close(n.release)
}

func mnStartGroup(t *testing.T, nodes ...*mnNode) {
	t.Helper()
	for i := range nodes {
		nodes[i].startWaitReady(t)
	}
	for i := range nodes {
		nodes[i].register()
	}
	for i := range nodes {
		nodes[i].releaseStart()
	}
}

func mnStartSingle(t *testing.T, node *mnNode) {
	t.Helper()
	mnStartGroup(t, node)
}

// mnStartNode prepares and immediately starts a node (single-node convenience).
func (env *mnEnv) mnStartNode(
	t *testing.T,
	node base.LocalNode,
	pool *isaacdatabase.TempPool,
	storage *leveldbstorage.Storage,
	lvps *isaac.LastVoteproofsHandler,
	noSignBefore base.StagePoint,
	router *mnRouter,
	proposer base.LocalNode,
	requestFunc func(context.Context, base.Point, base.Node, util.Hash) (base.ProposalSignFact, bool, error),
) *mnNode {
	t.Helper()
	n := env.mnPrepareNode(t, node, pool, storage, lvps, noSignBefore, router, proposer, requestFunc)
	mnStartSingle(t, n)
	return n
}

// mnRoundBallots holds, for a single consensus round, one signed INIT ballot per
// node plus the INIT DRAW voteproof assembled from exactly those ballots' sign
// facts. Each node's ballot is a scatter fact (distinct random proposal), so the
// voteproof never reaches majority and is a valid DRAW under the suffrage.
type mnRoundBallots struct {
	point   base.Point
	ballots map[string]isaac.INITBallot // node address -> that node's own INIT ballot
	draw    base.INITVoteproof          // DRAW voteproof over all nodes' sign facts
}

// mnBuildScatterDrawRound builds a consistent DRAW round: each node signs its own
// scatter INIT ballot at point, and the returned draw voteproof carries exactly
// those ballots' sign facts. attached is the voteproof each new ballot references
// (the previous round's draw, or the committed ACCEPT for R0).
func mnBuildScatterDrawRound(
	t *testing.T,
	nodes []base.LocalNode,
	point base.Point,
	previousBlock util.Hash,
	networkID base.NetworkID,
	threshold base.Threshold,
	attached base.Voteproof,
) mnRoundBallots {
	t.Helper()

	signfacts := make([]base.BallotSignFact, 0, len(nodes))
	ballots := make(map[string]isaac.INITBallot, len(nodes))

	for i := range nodes {
		// distinct proposal per node => scatter => no majority => DRAW.
		fact := isaac.NewINITBallotFact(point, previousBlock, valuehash.RandomSHA256(), nil)
		sf := isaac.NewINITBallotSignFact(fact)
		requireNoError(t, sf.NodeSign(nodes[i].Privatekey(), networkID, nodes[i].Address()))

		signfacts = append(signfacts, sf)
		ballots[nodes[i].Address().String()] = isaac.NewINITBallot(attached, sf, nil)
	}

	draw := isaac.NewINITVoteproof(point)
	draw.
		SetSignFacts(signfacts).
		SetThreshold(threshold).
		SetResult(base.VoteResultDraw).
		Finish()

	return mnRoundBallots{point: point, ballots: ballots, draw: draw}
}

// mnBuildSelectiveScatterDrawRound is the selective-signer variant used by
// Fixture C: only signer nodes cache/sign the round's ballot and only those
// exact SignFacts appear in the DRAW voteproof.
func mnBuildSelectiveScatterDrawRound(
	t *testing.T,
	signerNodes []base.LocalNode,
	point base.Point,
	previousBlock util.Hash,
	networkID base.NetworkID,
	threshold base.Threshold,
	attached base.Voteproof,
) mnRoundBallots {
	t.Helper()

	signfacts := make([]base.BallotSignFact, 0, len(signerNodes))
	ballots := make(map[string]isaac.INITBallot, len(signerNodes))

	for i := range signerNodes {
		fact := isaac.NewINITBallotFact(point, previousBlock, valuehash.RandomSHA256(), nil)
		sf := isaac.NewINITBallotSignFact(fact)
		requireNoError(t, sf.NodeSign(signerNodes[i].Privatekey(), networkID, signerNodes[i].Address()))

		signfacts = append(signfacts, sf)
		ballots[signerNodes[i].Address().String()] = isaac.NewINITBallot(attached, sf, nil)
	}

	draw := isaac.NewINITVoteproof(point)
	draw.
		SetSignFacts(signfacts).
		SetThreshold(threshold).
		SetResult(base.VoteResultDraw).
		Finish()

	return mnRoundBallots{point: point, ballots: ballots, draw: draw}
}

// mnNodeFixture is the on-disk pre-restart state for one node.
type mnNodeFixture struct {
	node           base.LocalNode
	root           string
	dbroot         string
	cachedFrontier base.Round // highest round this node actually signed a ballot for
	snapshotRound  base.Round // Cap round persisted in this node's durable snapshot
	isSigner       bool       // whether this node's sign fact is part of the high DRAW voteproof
}

// mnEnv is the shared multi-node environment.
type mnEnv struct {
	ctx               context.Context
	encs              *encoder.Encoders
	enc               encoder.Encoder
	height            base.Height
	networkID         base.NetworkID
	params            *isaac.Params
	nodes             []base.LocalNode
	suffrage          base.Suffrage
	committedManifest base.Manifest
	committedBlockmap base.BlockMap
	committedINIT     base.INITVoteproof
	committedACCEPT   base.ACCEPTVoteproof
	previousBlock     util.Hash
	// rounds[r] = the consistent scatter DRAW round r built once and shared, so
	// every node caches the same-per-node ballot and the same draw voteproof.
	rounds []mnRoundBallots
}

// mnNewEnv creates suffrage, committed block artifacts and the shared consistent
// per-round ballot/voteproof history up to maxRound.
func mnNewEnv(t *testing.T, nNodes int, threshold base.Threshold, maxRound base.Round) *mnEnv {
	t.Helper()

	const height base.Height = 33

	ctx, encs, enc := restartReplayEncoders(t)

	nodes := make([]base.LocalNode, nNodes)
	for i := range nodes {
		nodes[i] = base.RandomLocalNode()
	}
	suffrage, _ := isaac.NewTestSuffrage(0, nodes...)

	networkID := base.RandomNetworkID()
	params := isaac.DefaultParams(networkID)
	requireNoError(t, params.SetThreshold(threshold))

	committedPoint := base.RawPoint(int64(height-1), 0)
	committedManifest := base.NewDummyManifest(committedPoint.Height(), valuehash.RandomSHA256())
	committedINIT, committedACCEPT := restartReplayCommittedVoteproofs(
		t, nodes, networkID, params.Threshold(), committedPoint, committedManifest.Hash(),
	)
	committedBlockmap := base.NewDummyBlockMap(committedManifest)
	previousBlock := committedManifest.Hash()

	env := &mnEnv{
		ctx: ctx, encs: encs, enc: enc, height: height, networkID: networkID, params: params,
		nodes: nodes, suffrage: suffrage,
		committedManifest: committedManifest, committedBlockmap: committedBlockmap,
		committedINIT: committedINIT, committedACCEPT: committedACCEPT, previousBlock: previousBlock,
	}

	// R0 attaches committed ACCEPT; Rn attaches R(n-1) draw.
	var attached base.Voteproof = committedACCEPT
	for r := base.Round(0); r <= maxRound; r++ {
		round := mnBuildScatterDrawRound(
			t, nodes, base.RawPoint(int64(height), uint64(r)), previousBlock, networkID, params.Threshold(), attached,
		)
		env.rounds = append(env.rounds, round)
		attached = round.draw
	}

	return env
}

func (env *mnEnv) mnReplaceRoundWithSelectiveSigners(t *testing.T, round base.Round, signerIndexes ...int) {
	t.Helper()

	signerNodes := make([]base.LocalNode, 0, len(signerIndexes))
	for _, idx := range signerIndexes {
		signerNodes = append(signerNodes, env.nodes[idx])
	}

	attached := base.Voteproof(env.committedACCEPT)
	if round > 0 {
		attached = env.rounds[round-1].draw
	}

	env.rounds[round] = mnBuildSelectiveScatterDrawRound(
		t,
		signerNodes,
		base.RawPoint(int64(env.height), uint64(round)),
		env.previousBlock,
		env.networkID,
		env.params.Threshold(),
		attached,
	)
}

// mnDatabase returns a committed-store stub for a node (suffrage lookup + block maps).
func (env *mnEnv) mnDatabase() *restartReplayDatabase {
	return &restartReplayDatabase{
		blockmap:  env.committedBlockmap,
		blockmaps: map[base.Height]base.BlockMap{env.committedManifest.Height(): env.committedBlockmap},
		policy:    isaac.DefaultNetworkPolicy(),
		suffrage:  env.suffrage,
	}
}

// mnWriteCommittedFS writes the local block-item files PLoadFromDatabase requires.
func (env *mnEnv) mnWriteCommittedFS(t *testing.T, node base.LocalNode, root string) {
	t.Helper()

	dataroot := launch.LocalFSDataDirectory(root)
	requireNoError(t, os.MkdirAll(dataroot, 0o700))

	writer, err := isaacblock.NewLocalFSWriter(
		dataroot, env.committedManifest.Height(), env.encs.JSON(), env.enc, node, env.networkID,
	)
	requireNoError(t, err)
	requireNoError(t, writer.SetManifest(context.Background(), env.committedManifest))
	requireNoError(t, writer.SetINITVoteproof(context.Background(), env.committedINIT))
	requireNoError(t, writer.SetACCEPTVoteproof(context.Background(), env.committedACCEPT))
	_, err = writer.Save(context.Background())
	requireNoError(t, err)
}

// mnSetupNodePool builds a node's on-disk pool: committed FS, cached ballots up to
// cachedFrontier, and a durable snapshot whose Cap is the snapshotRound DRAW.
// Returns the pool + underlying storage (both must be Closed by the caller before
// the node is "restarted" via PLoadFromDatabase).
func (env *mnEnv) mnSetupNodePool(
	t *testing.T, fx mnNodeFixture,
) (*isaacdatabase.TempPool, *leveldbstorage.Storage) {
	t.Helper()

	env.mnWriteCommittedFS(t, fx.node, fx.root)

	storage := leveldbstorage.NewFSStorage(fx.dbroot)
	pool, err := isaacdatabase.NewTempPool(storage, env.encs, env.enc, 32)
	requireNoError(t, err)

	// cache this node's own ballot for each round it participated in.
	for r := base.Round(0); r <= fx.cachedFrontier; r++ {
		bl := env.rounds[r].ballots[fx.node.Address().String()]
		requireNoError(t, bl.IsValid(env.networkID))
		_, err := pool.SetBallot(bl)
		requireNoError(t, err)
	}

	// durable snapshot: Cap = snapshotRound DRAW, anchored to committed block.
	draw := env.rounds[fx.snapshotRound].draw
	snapshot := isaacdatabase.DurableConsensusSnapshot{
		Version:        isaacdatabase.DurableConsensusSnapshotVersion,
		INIT:           draw,
		ACCEPT:         env.committedACCEPT,
		MajorityAnchor: env.committedACCEPT,
		ManifestHeight: env.committedManifest.Height(),
		ManifestHash:   env.committedManifest.Hash(),
		PreviousBlock:  env.previousBlock,
		SavedAt:        time.Now(),
	}
	updated, err := pool.SetDurableConsensusSnapshot(snapshot)
	requireNoError(t, err)
	requireTrue(t, updated, "write node durable snapshot")

	return pool, storage
}

// mnRecover closes+reopens a node's pool and runs the real PLoadFromDatabase,
// returning the recovered LVPS, the reopened pool and the boundary NoSignBefore.
func (env *mnEnv) mnRecover(
	t *testing.T, fx mnNodeFixture, pool *isaacdatabase.TempPool, storage *leveldbstorage.Storage,
) (*isaac.LastVoteproofsHandler, *isaacdatabase.TempPool, *leveldbstorage.Storage, base.StagePoint) {
	t.Helper()

	requireNoError(t, pool.Close())
	requireNoError(t, storage.Close())

	storage = leveldbstorage.NewFSStorage(fx.dbroot)
	pool, err := isaacdatabase.NewTempPool(storage, env.encs, env.enc, 32)
	requireNoError(t, err)

	loaderctx := util.ContextWithValues(env.ctx, map[util.ContextKey]interface{}{
		launch.DesignContextKey: launch.NodeDesign{
			Storage: launch.NodeStorageDesign{Base: fx.root}, LocalParams: &launch.LocalParams{ISAAC: env.params},
		},
		launch.CenterDatabaseContextKey: isaac.Database(env.mnDatabase()),
		launch.PoolDatabaseContextKey:   pool,
		launch.LeveldbStorageContextKey: storage,
	})
	loaderctx, err = launch.PLoadFromDatabase(loaderctx)
	requireNoError(t, err)

	var lvps *isaac.LastVoteproofsHandler
	requireNoError(t, util.LoadFromContextOK(loaderctx, launch.LastVoteproofsHandlerContextKey, &lvps))

	var noSignBefore base.StagePoint
	requireNoError(t, util.LoadFromContextOK(loaderctx, launch.NoSignBeforeContextKey, &noSignBefore))

	return lvps, pool, storage, noSignBefore
}

// TestMultiNodeFixtureConsistencyAndRecovery verifies the correctness-critical
// invariants of the fixture before any async convergence is attempted:
//   - each node's cached ballot sign fact is exactly the sign fact carried for
//     that node in the shared DRAW voteproof (no synthetic sign facts);
//   - the DRAW voteproof is valid under the real suffrage validator;
//   - a node recovers through the real PLoadFromDatabase to the expected
//     NextLegalPoint (= snapshot Cap round + 1, INIT) with a consistent LVPS.
func TestMultiNodeFixtureConsistencyAndRecovery(t *testing.T) {
	t.Parallel()

	env := mnNewEnv(t, 4, base.Threshold(67), 3)

	// (1) sign-fact consistency: the DRAW voteproof at round 3 must contain, for
	// each node, exactly the sign fact of that node's cached round-3 ballot.
	round := env.rounds[3]
	vpSignByNode := map[string]base.BallotSignFact{}
	for _, sf := range round.draw.SignFacts() {
		vpSignByNode[sf.Node().String()] = sf
	}
	for _, node := range env.nodes {
		cached := round.ballots[node.Address().String()]
		vpsf, ok := vpSignByNode[node.Address().String()]
		requireTrue(t, ok, "node missing from DRAW voteproof sign facts")
		csf := cached.SignFact()
		// full sign-fact equality: HashBytes covers fact + all Signs
		// (signer, signature, signed-at), not just the fact hash.
		if !bytes.Equal(csf.HashBytes(), vpsf.HashBytes()) {
			t.Fatalf("cached ballot sign fact != DRAW voteproof sign fact for %v", node.Address())
		}
		if !csf.Node().Equal(vpsf.Node()) {
			t.Fatalf("cached ballot signer node != DRAW voteproof signer for %v", node.Address())
		}
	}

	// (2) DRAW voteproof passes the real suffrage validator (same used by recovery).
	requireNoError(t, base.IsValidVoteproof(round.draw, env.networkID))
	requireNoError(t, isaac.IsValidVoteproofWithSuffrage(round.draw, env.suffrage))
	if round.draw.Result() != base.VoteResultDraw {
		t.Fatalf("round voteproof result=%v want DRAW", round.draw.Result())
	}

	// (3) real recovery: node with snapshot Cap R2 resumes at R3 INIT.
	root := t.TempDir()
	fx := mnNodeFixture{
		node: env.nodes[0], root: root, dbroot: filepath.Join(root, "pool"),
		cachedFrontier: 3, snapshotRound: 2, isSigner: true,
	}
	pool, storage := env.mnSetupNodePool(t, fx)
	lvps, pool, storage, noSignBefore := env.mnRecover(t, fx, pool, storage)
	defer pool.Close()
	defer storage.Close()

	cap := lvps.Last().Cap()
	requireTrue(t, cap != nil, "recovered LVPS cap nil")
	if cap.Point().Point.Round() != 2 {
		t.Fatalf("recovered cap round=%d want 2 (snapshot Cap)", cap.Point().Point.Round())
	}
	wantBoundary := base.NewStagePoint(base.RawPoint(int64(env.height), 3), base.StageINIT)
	if noSignBefore.Compare(wantBoundary) != 0 {
		t.Fatalf("NoSignBefore=%v want %v", noSignBefore, wantBoundary)
	}

	// the cached R3 ballot survives the real LevelDB reopen and is the exact fact.
	bl, found, err := pool.Ballot(base.RawPoint(int64(env.height), 3), base.StageINIT, false)
	requireNoError(t, err)
	requireTrue(t, found, "cached R3 ballot missing after reopen")
	if !bl.SignFact().Fact().Hash().Equal(round.ballots[fx.node.Address().String()].SignFact().Fact().Hash()) {
		t.Fatal("reopened R3 ballot fact changed")
	}
}

func mnWaitDrawRound(t *testing.T, ch <-chan base.Voteproof, round base.Round) base.Voteproof {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case vp := <-ch:
			if vp.Point().Point.Round() == round && vp.Point().Stage() == base.StageINIT {
				return vp
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for INIT DRAW voteproof round %d", round)
			return nil
		}
	}
}

func mnWaitBroadcastRound(t *testing.T, ch <-chan base.Ballot, round base.Round) base.Ballot {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case bl := <-ch:
			if bl.Point().Point.Round() == round && bl.Point().Stage() == base.StageINIT {
				return bl
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for broadcast INIT round %d", round)
			return nil
		}
	}
}

func mnRequireCapRound(t *testing.T, lvps *isaac.LastVoteproofsHandler, want base.Round, label string) {
	t.Helper()

	cap := lvps.Last().Cap()
	if cap == nil {
		t.Fatalf("%s cap nil", label)
	}
	if got := cap.Point().Point.Round(); got != want {
		t.Fatalf("%s cap round=%d want %d", label, got, want)
	}
}

func mnAssertNoRoundBroadcast(t *testing.T, ch <-chan base.Ballot, round base.Round, label string) {
	t.Helper()

	for {
		select {
		case bl := <-ch:
			if bl != nil && bl.Point().Point.Round() >= round {
				t.Fatalf("%s unexpected broadcast at round %d: %v", label, round, bl.Point())
			}
		default:
			return
		}
	}
}

func mnAssertNoDrawRound(t *testing.T, ch <-chan base.Voteproof, round base.Round, label string) {
	t.Helper()

	for {
		select {
		case vp := <-ch:
			if vp != nil && vp.Point().Point.Round() >= round {
				t.Fatalf("%s unexpected draw at round %d: %v", label, round, vp.Point())
			}
		default:
			return
		}
	}
}

func mnVoteproofSignerSet(vp base.Voteproof) map[string]base.BallotSignFact {
	out := map[string]base.BallotSignFact{}
	if vp == nil {
		return out
	}

	for _, sf := range vp.SignFacts() {
		out[sf.Node().String()] = sf
	}

	return out
}

// TestMultiNodeStartBarrierDefersRegistrationUntilReady locks in the startup
// invariant: a prepared node is not a router target yet, so pre-start ballots
// are dropped by the test router; after a coordinated group start, the same
// point is delivered and processed without startup races or panics.
func TestMultiNodeStartBarrierDefersRegistrationUntilReady(t *testing.T) {
	t.Parallel()

	env := mnNewEnv(t, 2, base.Threshold(100), 3)
	proposer := env.nodes[0]
	requestFunc := env.mnProposalRequestFunc(t, proposer, 3, 6)
	router := newMNRouter()

	type holder struct {
		node *mnNode
	}
	holders := make([]holder, 2)

	for i := 0; i < 2; i++ {
		root := t.TempDir()
		fx := mnNodeFixture{
			node: env.nodes[i], root: root, dbroot: filepath.Join(root, "pool"),
			cachedFrontier: 3, snapshotRound: 2, isSigner: true,
		}
		pool, storage := env.mnSetupNodePool(t, fx)
		lvps, pool, storage, nsb := env.mnRecover(t, fx, pool, storage)
		defer pool.Close()
		defer storage.Close()

		holders[i].node = env.mnPrepareNode(t, env.nodes[i], pool, storage, lvps, nsb, router, proposer, requestFunc)
	}

	requireTrue(t, router.enqueue(env.nodes[0].Address().String(), env.rounds[3].ballots[env.nodes[0].Address().String()]), "pre-start enqueue")
	router.Flush()
	if ds := router.snapshotDeliveries(); len(ds) != 0 {
		t.Fatalf("prepared-but-unstarted node unexpectedly became router target: %+v", ds)
	}

	mnStartGroup(t, holders[0].node, holders[1].node)

	for i := 0; i < 2; i++ {
		bl := mnWaitBroadcastRound(t, holders[i].node.broadcasts, 3)
		if !bl.SignFact().Node().Equal(env.nodes[i].Address()) {
			t.Fatalf("node%d R3 ballot not self-signed", i)
		}
	}
	router.Flush()

	var delivered bool
	for _, d := range router.snapshotDeliveries() {
		if d.source == env.nodes[0].Address().String() &&
			d.target == env.nodes[1].Address().String() &&
			d.point.Point.Round() == 3 {
			delivered = true
			if !d.allowed || d.err != nil {
				t.Fatalf("post-start delivery semantics wrong: %+v", d)
			}
		}
	}
	if !delivered {
		t.Fatal("group start did not expose node1 as router target")
	}

	for i := 0; i < 2; i++ {
		select {
		case err := <-holders[i].node.stateErrors:
			t.Fatalf("node%d unexpected state error/safe-stop: %v", i, err)
		default:
		}
	}

	for i := 0; i < 2; i++ {
		holders[i].node.stop()
	}
	router.close()
	if errs := router.drainErrors(); len(errs) > 0 {
		t.Fatalf("router delivery errors: %v", errs)
	}
	if o := router.overflowCount(); o > 0 {
		t.Fatalf("router queue overflow=%d", o)
	}
}

func mnProposalRegistryKey(point base.Point, proposer base.Node, previousBlock util.Hash) string {
	return point.String() + "|" + proposer.Address().String() + "|" + previousBlock.String()
}

func (env *mnEnv) mnProposalRequestFunc(
	t *testing.T,
	proposer base.LocalNode,
	fromRound,
	toRound base.Round,
) func(context.Context, base.Point, base.Node, util.Hash) (base.ProposalSignFact, bool, error) {
	t.Helper()

	root := t.TempDir()
	storage := leveldbstorage.NewFSStorage(filepath.Join(root, "pool"))
	pool, err := isaacdatabase.NewTempPool(storage, env.encs, env.enc, 32)
	requireNoError(t, err)

	t.Cleanup(func() {
		requireNoError(t, pool.Close())
		requireNoError(t, storage.Close())
	})

	maker := isaac.NewProposalMaker(
		proposer, env.networkID,
		func(ctx context.Context, h base.Height) ([][2]util.Hash, error) {
			return pool.OperationHashes(ctx, h, 100, nil)
		},
		pool, env.mnDatabase().LastBlockMap,
	)

	registry := map[string]base.ProposalSignFact{}
	for r := fromRound; r <= toRound; r++ {
		point := base.RawPoint(int64(env.height), uint64(r))
		pr, merr := maker.Make(context.Background(), point, env.previousBlock)
		requireNoError(t, merr)
		registry[mnProposalRegistryKey(point, proposer, env.previousBlock)] = pr
	}

	return func(ctx context.Context, point base.Point, requested base.Node, previousBlock util.Hash) (base.ProposalSignFact, bool, error) {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		default:
		}

		if requested == nil || !requested.Address().Equal(proposer.Address()) {
			return nil, false, nil
		}

		pr, found := registry[mnProposalRegistryKey(point, requested, previousBlock)]

		return pr, found, nil
	}
}

// TestMultiNodeReqNet005SingleBallotForwardJump proves REQ-NET-005: a single
// higher-node ballot carrying a valid attached DRAW voteproof advances a lower
// node's LVPS independent of any target-round quorum. All other R3 ballots are
// gated so node1 cannot form an R3 voteproof by counting.
func TestMultiNodeReqNet005SingleBallotForwardJump(t *testing.T) {
	t.Parallel()

	env := mnNewEnv(t, 2, base.Threshold(100), 4) // 2 nodes, 2/2, rounds R0..R4
	node0, node1 := env.nodes[0], env.nodes[1]

	// node1 (lower): snapshot Cap R2, cached R0..R3 (incl. cached R3 ballot).
	root1 := t.TempDir()
	fx1 := mnNodeFixture{
		node: node1, root: root1, dbroot: filepath.Join(root1, "pool"),
		cachedFrontier: 3, snapshotRound: 2, isSigner: true,
	}
	pool1, storage1 := env.mnSetupNodePool(t, fx1)
	lvps1, pool1, storage1, nsb1 := env.mnRecover(t, fx1, pool1, storage1)
	defer pool1.Close()
	defer storage1.Close()

	if r := lvps1.Last().Cap().Point().Point.Round(); r != 2 {
		t.Fatalf("node1 recovered cap round=%d want 2", r)
	}

	requestFunc := env.mnProposalRequestFunc(t, node0, 4, 6)

	router := newMNRouter()
	// gate: block node0's R3 ballot to node1 => node1 cannot count an R3 quorum.
	router.setAllow(func(target, source string, bl base.Ballot) bool {
		if target == node1.Address().String() && source == node0.Address().String() &&
			bl.Point().Point.Round() == 3 {
			return false
		}
		return true
	})

	n1 := env.mnStartNode(t, node1, pool1, storage1, lvps1, nsb1, router, node0, requestFunc)

	// barrier: node1 resumes and broadcasts its cached R3 ballot.
	_ = mnWaitBroadcastRound(t, n1.broadcasts, 3)

	// attempt to deliver node0's R3 ballot; gate blocks it -> still no R3 quorum.
	router.enqueue(node0.Address().String(), env.rounds[3].ballots[node0.Address().String()])

	// (assert) before the jump node1 has no R3 quorum: LVPS cap is still R2.
	if r := lvps1.Last().Cap().Point().Point.Round(); r != 2 {
		t.Fatalf("node1 cap round=%d before jump; expected 2 (no R3 quorum)", r)
	}
	cachedR3Before := env.rounds[3].ballots[node1.Address().String()].SignFact().HashBytes()

	// deliver exactly one high-node ballot: node0's R4 (attached R3 DRAW voteproof).
	high := env.rounds[4].ballots[node0.Address().String()]
	if high.Voteproof() == nil || high.Voteproof().Point().Point.Round() != 3 {
		t.Fatalf("high ballot attached voteproof round=%v want 3", high.Voteproof())
	}
	router.enqueue(node0.Address().String(), high)

	// (assert) node1's forward-jump evidence is the delivered ballot's attached
	// R3 voteproof: the R3 voteproof node1 observes has exactly that ID.
	r3vp := mnWaitDrawRound(t, n1.draws, 3)
	if r3vp.ID() != high.Voteproof().ID() {
		t.Fatalf("node1 R3 voteproof ID=%s != attached ID=%s", r3vp.ID(), high.Voteproof().ID())
	}

	// barrier: node1 forward jumps and broadcasts a new R4 ballot.
	r4 := mnWaitBroadcastRound(t, n1.broadcasts, 4)

	// (assert) node1's LVPS advanced to R3 via the attached voteproof.
	if r := lvps1.Last().Cap().Point().Point.Round(); r < 3 {
		t.Fatalf("node1 cap round=%d after jump; expected >=3", r)
	}
	// (assert) node1 did not re-sign at R3: reopened ballot's full sign fact
	// (HashBytes covers signer/signature/signed-at) and Node are the fixture's,
	// and match the R3 DRAW voteproof's sign fact for node1.
	after, found, aerr := pool1.Ballot(base.RawPoint(int64(env.height), 3), base.StageINIT, false)
	requireNoError(t, aerr)
	requireTrue(t, found, "cached R3 ballot missing after jump")
	if !bytes.Equal(after.SignFact().HashBytes(), cachedR3Before) {
		t.Fatal("node1 re-signed a new ballot at R3")
	}
	var vpNode1SF base.BallotSignFact
	for _, sf := range high.Voteproof().SignFacts() {
		if sf.Node().Equal(node1.Address()) {
			vpNode1SF = sf
		}
	}
	requireTrue(t, vpNode1SF != nil, "node1 missing from R3 voteproof")
	if !bytes.Equal(after.SignFact().HashBytes(), vpNode1SF.HashBytes()) ||
		!after.SignFact().Node().Equal(vpNode1SF.Node()) {
		t.Fatal("reopened R3 ballot sign fact != R3 voteproof sign fact for node1")
	}
	// (assert) the new R4 ballot is node1's own and at the correct point.
	if !r4.SignFact().Node().Equal(node1.Address()) {
		t.Fatal("R4 ballot not signed by node1")
	}
	// (assert) no state error / unexpected safe-stop.
	select {
	case err := <-n1.stateErrors:
		t.Fatalf("unexpected state error/safe-stop: %v", err)
	default:
	}

	n1.stop()
	router.close()
	if errs := router.drainErrors(); len(errs) > 0 {
		t.Fatalf("router delivery errors: %v", errs)
	}
	if o := router.overflowCount(); o > 0 {
		t.Fatalf("router queue overflow=%d", o)
	}
}

// TestMultiNodeConvergenceFixtureA (Fixture A): only node3 holds the completed
// higher (R3) durable snapshot; node0..2 hold R2 snapshots plus a cached R3
// ballot. All recover through the real PLoadFromDatabase and start real States.
// R3 delivery is allowed so node0..2 reconstruct the R3 frontier and advance;
// R4 vote delivery is gated to prevent round explosion. Every node must create
// an R4 INIT ballot and settle on a common Cap of R3.
func TestMultiNodeConvergenceFixtureA(t *testing.T) {
	t.Parallel()

	env := mnNewEnv(t, 4, base.Threshold(75), 4) // 4 nodes, 3/4, rounds R0..R4
	proposer := env.nodes[0]

	// shared proposal registry backed by a dedicated proposer maker.
	requestFunc := env.mnProposalRequestFunc(t, proposer, 3, 8)

	router := newMNRouter()
	// R3 (and below) delivery allowed; gate R4+ votes to avoid round explosion.
	router.setAllow(func(_, _ string, bl base.Ballot) bool {
		return bl.Point().Point.Round() < 4
	})

	type holder struct {
		fx           mnNodeFixture
		node         *mnNode
		cachedR3Hash []byte
	}
	holders := make([]*holder, len(env.nodes))

	for i := range env.nodes {
		root := t.TempDir()
		snapRound := base.Round(2)
		if i == 3 {
			snapRound = 3 // only node3 holds the completed R3 snapshot
		}
		fx := mnNodeFixture{
			node: env.nodes[i], root: root, dbroot: filepath.Join(root, "pool"),
			cachedFrontier: 3, snapshotRound: snapRound, isSigner: true,
		}
		pool, storage := env.mnSetupNodePool(t, fx)
		lvps, pool, storage, nsb := env.mnRecover(t, fx, pool, storage) // real PLoadFromDatabase
		defer pool.Close()
		defer storage.Close()

		// startup frontier check succeeded (recovery returned without safe-stop).
		wantCap := base.Round(2)
		if i == 3 {
			wantCap = 3
		}
		if r := lvps.Last().Cap().Point().Point.Round(); r != wantCap {
			t.Fatalf("node%d recovered cap round=%d want %d", i, r, wantCap)
		}

		holders[i] = &holder{
			fx:           fx,
			node:         env.mnPrepareNode(t, env.nodes[i], pool, storage, lvps, nsb, router, proposer, requestFunc),
			cachedR3Hash: env.rounds[3].ballots[env.nodes[i].Address().String()].SignFact().HashBytes(),
		}
	}

	// all prepared; now start/register/release as one group.
	starts := make([]*mnNode, len(holders))
	for i := range holders {
		starts[i] = holders[i].node
	}
	mnStartGroup(t, starts...)

	// every node must create an R4 INIT ballot (forward progress to R4).
	for i := range holders {
		r4 := mnWaitBroadcastRound(t, holders[i].node.broadcasts, 4)
		if !r4.SignFact().Node().Equal(env.nodes[i].Address()) {
			t.Fatalf("node%d R4 ballot not self-signed", i)
		}
	}
	router.Flush()

	// assertions.
	for i := range holders {
		h := holders[i]
		cap := h.node.lvps.Last().Cap()
		requireTrue(t, cap != nil, "cap nil")
		capR := cap.Point().Point.Round()

		// node0..2 must not regress below R2; node3 not below R3.
		floor := base.Round(2)
		if i == 3 {
			floor = 3
		}
		if capR < floor {
			t.Fatalf("node%d cap round=%d regressed below %d", i, capR, floor)
		}
		// all nodes settle on common Cap R3.
		if capR != 3 {
			t.Fatalf("node%d final cap round=%d want 3 (common frontier)", i, capR)
		}

		// cached R3 ballot's full sign fact unchanged (no re-sign at same point).
		after, found, aerr := h.node.pool.Ballot(base.RawPoint(int64(env.height), 3), base.StageINIT, false)
		requireNoError(t, aerr)
		requireTrue(t, found, "cached R3 ballot missing")
		if !bytes.Equal(after.SignFact().HashBytes(), h.cachedR3Hash) {
			t.Fatalf("node%d re-signed at R3 (cached sign fact changed)", i)
		}

		// no unexpected state error / safe-stop.
		select {
		case err := <-h.node.stateErrors:
			t.Fatalf("node%d unexpected state error/safe-stop: %v", i, err)
		default:
		}
	}

	for i := range holders {
		holders[i].node.stop()
	}
	router.close()
	if errs := router.drainErrors(); len(errs) > 0 {
		t.Fatalf("router delivery errors: %v", errs)
	}
	if o := router.overflowCount(); o > 0 {
		t.Fatalf("router queue overflow=%d", o)
	}
}

// TestMultiNodeConvergenceFixtureBDelayedHighestSnapshotNode (Fixture B):
// node0..2 start first from snapshot R2 + cached R3 and advance to Cap R4.
// node3 starts later from snapshot R3 + cached R3, creates its legal R4 ballot,
// then forward-jumps to Cap R4 only when an explicit R5 ballot carrying the
// valid attached R4 DRAW voteproof is delivered through the real router path.
func TestMultiNodeConvergenceFixtureBDelayedHighestSnapshotNode(t *testing.T) {
	t.Parallel()

	env := mnNewEnv(t, 4, base.Threshold(75), 5) // 4 nodes, 3/4, rounds R0..R5
	proposer := env.nodes[0]
	lateNode := env.nodes[3]
	lateAddr := lateNode.Address().String()
	requestFunc := env.mnProposalRequestFunc(t, proposer, 3, 8)

	router := newMNRouter()
	var allowLateR5 atomic.Bool
	router.setAllow(func(target, _ string, bl base.Ballot) bool {
		switch round := bl.Point().Point.Round(); {
		case round < 5:
			return true
		case round == 5:
			return allowLateR5.Load() && target == lateAddr
		default:
			return false
		}
	})

	type holder struct {
		node         *mnNode
		cachedR3Hash []byte
	}
	holders := make([]*holder, len(env.nodes))

	for i := 0; i < 3; i++ {
		root := t.TempDir()
		fx := mnNodeFixture{
			node: env.nodes[i], root: root, dbroot: filepath.Join(root, "pool"),
			cachedFrontier: 3, snapshotRound: 2, isSigner: true,
		}
		pool, storage := env.mnSetupNodePool(t, fx)
		lvps, pool, storage, nsb := env.mnRecover(t, fx, pool, storage)
		defer pool.Close()
		defer storage.Close()

		mnRequireCapRound(t, lvps, 2, "early recovered node")

		holders[i] = &holder{
			node:         env.mnPrepareNode(t, env.nodes[i], pool, storage, lvps, nsb, router, proposer, requestFunc),
			cachedR3Hash: env.rounds[3].ballots[env.nodes[i].Address().String()].SignFact().HashBytes(),
		}
	}
	starts := make([]*mnNode, 0, 3)
	for i := 0; i < 3; i++ {
		starts = append(starts, holders[i].node)
	}
	mnStartGroup(t, starts...)

	capturedR5 := map[string]base.Ballot{}
	for i := 0; i < 3; i++ {
		bl := mnWaitBroadcastRound(t, holders[i].node.broadcasts, 5)
		if !bl.SignFact().Node().Equal(env.nodes[i].Address()) {
			t.Fatalf("node%d R5 ballot not self-signed", i)
		}
		capturedR5[env.nodes[i].Address().String()] = bl
	}
	router.Flush()

	blockedBeforeLate := router.snapshotDeliveries()
	blockedR5 := 0
	for _, d := range blockedBeforeLate {
		if d.point.Point.Round() != 5 {
			continue
		}

		blockedR5++
		if d.allowed {
			t.Fatalf("pre-late round5 delivery unexpectedly allowed: %+v", d)
		}
		if d.voted {
			t.Fatalf("pre-late round5 delivery unexpectedly voted: %+v", d)
		}
		if d.err != nil {
			t.Fatalf("pre-late round5 delivery error: %v", d.err)
		}
	}
	if blockedR5 != 9 {
		t.Fatalf("pre-late blocked round5 deliveries=%d want 9", blockedR5)
	}

	for i := 0; i < 3; i++ {
		mnRequireCapRound(t, holders[i].node.lvps, 4, "early started node")
	}

	high := capturedR5[proposer.Address().String()]
	requireTrue(t, high != nil, "captured proposer R5 ballot")
	requireTrue(t, high.Voteproof() != nil, "captured R5 attached voteproof")
	if vp := high.Voteproof(); vp.Point().Point.Round() != 4 || vp.Point().Stage() != base.StageINIT || vp.Result() != base.VoteResultDraw {
		t.Fatalf("captured R5 attached voteproof=%v want R4 INIT DRAW", vp)
	}
	requireNoError(t, base.IsValidVoteproof(high.Voteproof(), env.networkID))
	requireNoError(t, isaac.IsValidVoteproofWithSuffrage(high.Voteproof(), env.suffrage))

	root := t.TempDir()
	fx := mnNodeFixture{
		node: lateNode, root: root, dbroot: filepath.Join(root, "pool"),
		cachedFrontier: 3, snapshotRound: 3, isSigner: true,
	}
	pool, storage := env.mnSetupNodePool(t, fx)
	lvps, pool, storage, nsb := env.mnRecover(t, fx, pool, storage)
	defer pool.Close()
	defer storage.Close()

	mnRequireCapRound(t, lvps, 3, "late recovered node")

	holders[3] = &holder{
		node:         env.mnPrepareNode(t, lateNode, pool, storage, lvps, nsb, router, proposer, requestFunc),
		cachedR3Hash: env.rounds[3].ballots[lateNode.Address().String()].SignFact().HashBytes(),
	}
	mnStartSingle(t, holders[3].node)

	lateR4 := mnWaitBroadcastRound(t, holders[3].node.broadcasts, 4)
	if !lateR4.SignFact().Node().Equal(lateNode.Address()) {
		t.Fatal("late node R4 ballot not self-signed")
	}
	router.Flush()

	for i := 0; i < 3; i++ {
		mnRequireCapRound(t, holders[i].node.lvps, 4, "early node after late R4")
	}

	beforeExplicit := len(router.snapshotDeliveries())
	allowLateR5.Store(true)
	requireTrue(t, router.enqueue(proposer.Address().String(), high), "explicit late R5 enqueue")
	router.Flush()
	allowLateR5.Store(false)

	r4vp := mnWaitDrawRound(t, holders[3].node.draws, 4)
	if r4vp.ID() != high.Voteproof().ID() {
		t.Fatalf("late node R4 voteproof ID=%s != attached ID=%s", r4vp.ID(), high.Voteproof().ID())
	}
	lateR5 := mnWaitBroadcastRound(t, holders[3].node.broadcasts, 5)
	if !lateR5.SignFact().Node().Equal(lateNode.Address()) {
		t.Fatal("late node R5 ballot not self-signed")
	}
	router.Flush()

	for i := range holders {
		mnRequireCapRound(t, holders[i].node.lvps, 4, "final node")
	}

	delta := router.snapshotDeliveries()[beforeExplicit:]
	if len(delta) < 4 {
		t.Fatalf("explicit delivery delta=%d want at least 4", len(delta))
	}

	var explicitAllowed bool
	var explicitBlocked int
	for _, d := range delta {
		if d.source != proposer.Address().String() || d.point.Point.Round() != 5 {
			continue
		}

		if d.target == lateAddr {
			explicitAllowed = true
			if !d.allowed || !d.voted || d.err != nil {
				t.Fatalf("late-target explicit delivery semantics wrong: %+v", d)
			}
			continue
		}

		explicitBlocked++
		if d.allowed || d.voted || d.err != nil {
			t.Fatalf("non-late explicit delivery semantics wrong: %+v", d)
		}
	}
	if !explicitAllowed {
		t.Fatal("explicit late-node R5 delivery record missing")
	}
	if explicitBlocked != 3 {
		t.Fatalf("explicit blocked deliveries=%d want 3", explicitBlocked)
	}

	for i := range holders {
		after, found, err := holders[i].node.pool.Ballot(base.RawPoint(int64(env.height), 3), base.StageINIT, false)
		requireNoError(t, err)
		requireTrue(t, found, "cached R3 ballot missing")
		if !bytes.Equal(after.SignFact().HashBytes(), holders[i].cachedR3Hash) {
			t.Fatalf("node%d re-signed at R3 (cached sign fact changed)", i)
		}
	}

	for i := range holders {
		mnAssertNoRoundBroadcast(t, holders[i].node.broadcasts, 6, "node broadcast")
		select {
		case err := <-holders[i].node.stateErrors:
			t.Fatalf("node%d unexpected state error/safe-stop: %v", i, err)
		default:
		}
	}

	for i := range holders {
		holders[i].node.stop()
	}
	router.close()
	if errs := router.drainErrors(); len(errs) > 0 {
		t.Fatalf("router delivery errors: %v", errs)
	}
	if o := router.overflowCount(); o > 0 {
		t.Fatalf("router queue overflow=%d", o)
	}
}

// TestMultiNodeConvergenceFixtureCHighestSnapshotAbsent proves that convergence
// still happens through the real restart path when the node holding the highest
// durable snapshot never starts, as long as the remaining active nodes can form
// a fresh quorum. The selective-signer R3 fixture keeps node2 unsigned at R3
// before startup and excludes node2 from the offline high R3 voteproof.
func TestMultiNodeConvergenceFixtureCHighestSnapshotAbsent(t *testing.T) {
	t.Parallel()

	env := mnNewEnv(t, 4, base.Threshold(75), 3) // only up to R3; runtime builds R4
	env.mnReplaceRoundWithSelectiveSigners(t, 3, 0, 1, 3)

	proposer := env.nodes[0]
	offlineNode := env.nodes[3]
	offlineAddr := offlineNode.Address().String()
	requestFunc := env.mnProposalRequestFunc(t, proposer, 3, 7)

	r3signers := mnVoteproofSignerSet(env.rounds[3].draw)
	for _, idx := range []int{0, 1, 3} {
		addr := env.nodes[idx].Address().String()
		cached, ok := env.rounds[3].ballots[addr]
		requireTrue(t, ok, "selective R3 cached ballot missing")
		sf, found := r3signers[addr]
		requireTrue(t, found, "selective R3 signer missing from voteproof")
		if !bytes.Equal(cached.SignFact().HashBytes(), sf.HashBytes()) {
			t.Fatalf("selective R3 signer %s cached sign fact != voteproof sign fact", addr)
		}
	}
	if _, found := r3signers[env.nodes[2].Address().String()]; found {
		t.Fatal("node2 unexpectedly present in offline high R3 voteproof")
	}
	if _, found := env.rounds[3].ballots[env.nodes[2].Address().String()]; found {
		t.Fatal("node2 unexpectedly has cached R3 ballot in selective fixture")
	}

	offlineRoot := t.TempDir()
	offlineFX := mnNodeFixture{
		node: offlineNode, root: offlineRoot, dbroot: filepath.Join(offlineRoot, "pool"),
		cachedFrontier: 3, snapshotRound: 3, isSigner: true,
	}
	offlinePool, offlineStorage := env.mnSetupNodePool(t, offlineFX)
	defer offlinePool.Close()
	defer offlineStorage.Close()

	snapshot, found, err := offlinePool.DurableConsensusSnapshot()
	requireNoError(t, err)
	requireTrue(t, found, "offline node durable snapshot missing")
	if cap := snapshot.Cap(); cap == nil || cap.Point().Point.Round() != 3 {
		t.Fatalf("offline node durable snapshot cap=%v want R3", cap)
	}
	requireNoError(t, base.IsValidVoteproof(snapshot.INIT, env.networkID))
	requireNoError(t, isaac.IsValidVoteproofWithSuffrage(snapshot.INIT, env.suffrage))

	router := newMNRouter()
	router.setAllow(func(_, _ string, bl base.Ballot) bool {
		return bl.Point().Point.Round() < 4
	})

	type holder struct {
		node         *mnNode
		cachedR3Hash []byte
	}
	holders := make([]*holder, 3)

	for i := 0; i < 2; i++ {
		root := t.TempDir()
		fx := mnNodeFixture{
			node: env.nodes[i], root: root, dbroot: filepath.Join(root, "pool"),
			cachedFrontier: 3, snapshotRound: 2, isSigner: true,
		}
		pool, storage := env.mnSetupNodePool(t, fx)
		lvps, pool, storage, nsb := env.mnRecover(t, fx, pool, storage)
		defer pool.Close()
		defer storage.Close()
		mnRequireCapRound(t, lvps, 2, "fixture C recovered node")

		holders[i] = &holder{
			node:         env.mnPrepareNode(t, env.nodes[i], pool, storage, lvps, nsb, router, proposer, requestFunc),
			cachedR3Hash: env.rounds[3].ballots[env.nodes[i].Address().String()].SignFact().HashBytes(),
		}
	}

	mnStartGroup(t, holders[0].node, holders[1].node)

	for i := 0; i < 2; i++ {
		bl := mnWaitBroadcastRound(t, holders[i].node.broadcasts, 3)
		if !bl.SignFact().Node().Equal(env.nodes[i].Address()) {
			t.Fatalf("node%d cached R3 ballot not self-signed", i)
		}
	}
	router.Flush()

	root := t.TempDir()
	fx := mnNodeFixture{
		node: env.nodes[2], root: root, dbroot: filepath.Join(root, "pool"),
		cachedFrontier: 2, snapshotRound: 2, isSigner: true,
	}
	pool, storage := env.mnSetupNodePool(t, fx)
	_, found, err = pool.Ballot(base.RawPoint(int64(env.height), 3), base.StageINIT, false)
	requireNoError(t, err)
	if found {
		t.Fatal("node2 must not have R3 ballot before startup")
	}
	lvps, pool, storage, nsb := env.mnRecover(t, fx, pool, storage)
	defer pool.Close()
	defer storage.Close()
	mnRequireCapRound(t, lvps, 2, "fixture C recovered node")
	_, found, err = pool.Ballot(base.RawPoint(int64(env.height), 3), base.StageINIT, false)
	requireNoError(t, err)
	if found {
		t.Fatal("node2 reopened pool must not have R3 ballot before startup")
	}

	holders[2] = &holder{
		node: env.mnPrepareNode(t, env.nodes[2], pool, storage, lvps, nsb, router, proposer, requestFunc),
	}
	mnStartSingle(t, holders[2].node)
	requireTrue(t, router.enqueue(env.nodes[0].Address().String(), env.rounds[3].ballots[env.nodes[0].Address().String()]), "replay node0 cached R3 to node2")
	requireTrue(t, router.enqueue(env.nodes[1].Address().String(), env.rounds[3].ballots[env.nodes[1].Address().String()]), "replay node1 cached R3 to node2")
	node2r3 := mnWaitBroadcastRound(t, holders[2].node.broadcasts, 3)
	if !node2r3.SignFact().Node().Equal(env.nodes[2].Address()) {
		t.Fatal("node2 new R3 ballot not self-signed")
	}

	r3draw := mnWaitDrawRound(t, holders[0].node.draws, 3)
	router.Flush()

	drawSigners := mnVoteproofSignerSet(r3draw)
	for _, idx := range []int{0, 1, 2} {
		if _, found := drawSigners[env.nodes[idx].Address().String()]; !found {
			t.Fatalf("active R3 DRAW missing node%d signer", idx)
		}
	}
	if _, found := drawSigners[offlineAddr]; found {
		t.Fatal("offline node unexpectedly included in active R3 DRAW")
	}

	activeAddrs := []base.Address{
		env.nodes[0].Address(),
		env.nodes[1].Address(),
		env.nodes[2].Address(),
	}
	for i := 0; i < 3; i++ {
		voted := holders[i].node.ballotbox.Voted(base.NewStagePoint(base.RawPoint(int64(env.height), 3), base.StageINIT), activeAddrs)
		if len(voted) != 3 {
			t.Fatalf("node%d active R3 voted count=%d want 3", i, len(voted))
		}
	}

	r4Broadcasts := map[string]base.Ballot{}
	for i := 0; i < 3; i++ {
		bl := mnWaitBroadcastRound(t, holders[i].node.broadcasts, 4)
		if !bl.SignFact().Node().Equal(env.nodes[i].Address()) {
			t.Fatalf("node%d R4 ballot not self-signed", i)
		}
		r4Broadcasts[env.nodes[i].Address().String()] = bl
	}
	router.Flush()

	for i := 0; i < 3; i++ {
		mnRequireCapRound(t, holders[i].node.lvps, 3, "fixture C final active node")
	}

	afterNode2, found, err := holders[2].node.pool.Ballot(base.RawPoint(int64(env.height), 3), base.StageINIT, false)
	requireNoError(t, err)
	requireTrue(t, found, "node2 R3 ballot missing after runtime sign")
	if !afterNode2.SignFact().Node().Equal(env.nodes[2].Address()) {
		t.Fatal("node2 pool R3 ballot signer changed")
	}
	if !bytes.Equal(afterNode2.SignFact().HashBytes(), node2r3.SignFact().HashBytes()) {
		t.Fatal("node2 pool R3 ballot != runtime-generated R3 ballot")
	}

	for i := 0; i < 2; i++ {
		after, found, err := holders[i].node.pool.Ballot(base.RawPoint(int64(env.height), 3), base.StageINIT, false)
		requireNoError(t, err)
		requireTrue(t, found, "cached R3 ballot missing")
		if !bytes.Equal(after.SignFact().HashBytes(), holders[i].cachedR3Hash) {
			t.Fatalf("node%d cached R3 sign fact changed", i)
		}
	}

	for _, d := range router.snapshotDeliveries() {
		if d.target == offlineAddr {
			t.Fatalf("offline node unexpectedly became a router target: %+v", d)
		}
	}

	requireTrue(t, len(r4Broadcasts) == 3, "all active nodes produced R4")

	for i := 0; i < 3; i++ {
		mnAssertNoRoundBroadcast(t, holders[i].node.broadcasts, 5, "fixture C broadcast")
		select {
		case err := <-holders[i].node.stateErrors:
			t.Fatalf("node%d unexpected state error/safe-stop: %v", i, err)
		default:
		}
	}

	for i := 0; i < 3; i++ {
		holders[i].node.stop()
	}
	router.close()
	if errs := router.drainErrors(); len(errs) > 0 {
		t.Fatalf("router delivery errors: %v", errs)
	}
	if o := router.overflowCount(); o > 0 {
		t.Fatalf("router queue overflow=%d", o)
	}
}

// TestMultiNodeQuorumShortageIsNormalNotIssue009Failure covers the below-
// threshold case explicitly: with only two active nodes in a 3/4 quorum setup,
// R3 ballots are exchanged but no R3 voteproof or R4 ballot is produced.
func TestMultiNodeQuorumShortageIsNormalNotIssue009Failure(t *testing.T) {
	t.Parallel()

	env := mnNewEnv(t, 4, base.Threshold(75), 3)
	env.mnReplaceRoundWithSelectiveSigners(t, 3, 0, 1, 3)

	proposer := env.nodes[0]
	requestFunc := env.mnProposalRequestFunc(t, proposer, 3, 6)

	router := newMNRouter()
	router.setAllow(func(_, _ string, bl base.Ballot) bool {
		return bl.Point().Point.Round() < 4
	})

	type holder struct {
		node *mnNode
	}
	holders := make([]*holder, 2)

	for i := 0; i < 2; i++ {
		root := t.TempDir()
		fx := mnNodeFixture{
			node: env.nodes[i], root: root, dbroot: filepath.Join(root, "pool"),
			cachedFrontier: 3, snapshotRound: 2, isSigner: true,
		}
		pool, storage := env.mnSetupNodePool(t, fx)
		lvps, pool, storage, nsb := env.mnRecover(t, fx, pool, storage)
		defer pool.Close()
		defer storage.Close()
		mnRequireCapRound(t, lvps, 2, "quorum-shortage recovered node")

		holders[i] = &holder{
			node: env.mnPrepareNode(t, env.nodes[i], pool, storage, lvps, nsb, router, proposer, requestFunc),
		}
	}
	mnStartGroup(t, holders[0].node, holders[1].node)

	for i := 0; i < 2; i++ {
		bl := mnWaitBroadcastRound(t, holders[i].node.broadcasts, 3)
		if !bl.SignFact().Node().Equal(env.nodes[i].Address()) {
			t.Fatalf("node%d R3 ballot not self-signed", i)
		}
	}
	router.Flush()

	activeAddrs := []base.Address{env.nodes[0].Address(), env.nodes[1].Address()}
	for i := 0; i < 2; i++ {
		voted := holders[i].node.ballotbox.Voted(base.NewStagePoint(base.RawPoint(int64(env.height), 3), base.StageINIT), activeAddrs)
		if len(voted) != 2 {
			t.Fatalf("node%d active R3 voted count=%d want 2", i, len(voted))
		}
		mnRequireCapRound(t, holders[i].node.lvps, 2, "quorum-shortage cap")
	}

	for i := 0; i < 2; i++ {
		mnAssertNoDrawRound(t, holders[i].node.draws, 3, "quorum-shortage draw")
		mnAssertNoRoundBroadcast(t, holders[i].node.broadcasts, 4, "quorum-shortage broadcast")
		select {
		case err := <-holders[i].node.stateErrors:
			t.Fatalf("node%d unexpected state error/safe-stop: %v", i, err)
		default:
		}
	}

	for i := 0; i < 2; i++ {
		holders[i].node.stop()
	}
	router.close()
	if errs := router.drainErrors(); len(errs) > 0 {
		t.Fatalf("router delivery errors: %v", errs)
	}
	if o := router.overflowCount(); o > 0 {
		t.Fatalf("router queue overflow=%d", o)
	}
}

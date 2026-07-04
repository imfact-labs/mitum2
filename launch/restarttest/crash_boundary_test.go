//go:build test

package restarttest

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacdatabase "github.com/imfact-labs/mitum2/isaac/database"
	"github.com/imfact-labs/mitum2/launch"
	leveldbstorage "github.com/imfact-labs/mitum2/storage/leveldb"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/valuehash"
	"github.com/pkg/errors"
)

func TestDurableConsensusCrashBoundaries(t *testing.T) {
	for _, boundary := range []string{"a_before_memory_set", "b_before_snapshot_write", "c_after_snapshot_write", "d_after_next_ballot_broadcast"} {
		t.Run(boundary, func(t *testing.T) { testDurableConsensusCrashBoundary(t, boundary) })
	}
}

func testDurableConsensusCrashBoundary(t *testing.T, boundary string) {
	const height base.Height = 33
	root := t.TempDir()
	dbroot := filepath.Join(root, "pool")
	dataroot := launch.LocalFSDataDirectory(root)
	requireNoError(t, os.MkdirAll(dbroot, 0o700))
	requireNoError(t, os.MkdirAll(dataroot, 0o700))
	ctx, encs, enc := restartReplayEncoders(t)
	local, other := base.RandomLocalNode(), base.RandomLocalNode()
	networkID := base.RandomNetworkID()
	params := isaac.DefaultParams(networkID)
	requireNoError(t, params.SetThreshold(100))
	manifest := base.NewDummyManifest(height-1, valuehash.RandomSHA256())
	suffrage, _ := isaac.NewTestSuffrage(0, local, other)
	committedINIT, committedACCEPT := restartReplayCommittedVoteproofs(
		t, []base.LocalNode{local, other}, networkID, params.Threshold(), base.RawPoint(int64(height-1), 0), manifest.Hash(),
	)
	blockmap := writeRestartReplayCommittedBlock(
		t, dataroot, encs, enc, local, networkID, manifest, committedINIT, committedACCEPT,
	)
	db := &restartReplayDatabase{
		blockmap: blockmap, blockmaps: map[base.Height]base.BlockMap{height - 1: blockmap},
		policy: isaac.DefaultNetworkPolicy(), suffrage: suffrage, other: other,
	}
	storage := leveldbstorage.NewFSStorage(dbroot)
	pool, err := isaacdatabase.NewTempPool(storage, encs, enc, 32)
	requireNoError(t, err)
	lvps := isaac.NewLastVoteproofsHandler()
	requireTrue(t, lvps.Set(committedINIT), "set committed INIT")
	requireTrue(t, lvps.Set(committedACCEPT), "set committed ACCEPT")

	fault := errors.New("injected snapshot crash boundary")
	target := base.Round(3)
	writer := launch.SetDurableConsensusSnapshotFunc(nil)
	if boundary == "b_before_snapshot_write" {
		writer = func(snapshot isaacdatabase.DurableConsensusSnapshot) (bool, error) {
			if snapshot.Cap().Point().Round() == target {
				return false, fault
			}
			return pool.SetDurableConsensusSnapshot(snapshot)
		}
	} else if boundary == "c_after_snapshot_write" {
		writer = func(snapshot isaacdatabase.DurableConsensusSnapshot) (bool, error) {
			updated, err := pool.SetDurableConsensusSnapshot(snapshot)
			if err == nil && snapshot.Cap().Point().Round() == target {
				return updated, fault
			}
			return updated, err
		}
	} else if boundary == "d_after_next_ballot_broadcast" {
		writer = func(snapshot isaacdatabase.DurableConsensusSnapshot) (bool, error) {
			if snapshot.Cap().Point().Round() == target+1 {
				return false, fault
			}
			return pool.SetDurableConsensusSnapshot(snapshot)
		}
	}

	frontier := target
	if boundary == "a_before_memory_set" {
		frontier = target - 1
	}
	run := restartReplayStartStatesWithWriter(t, local, networkID, params, db, pool, lvps, frontier, writer)
	if boundary == "a_before_memory_set" {
		_ = waitRestartReplayVoteproof(t, run.draws, height, target-1)
		requireNoError(t, run.states.Stop())
	} else if boundary == "d_after_next_ballot_broadcast" {
		_ = waitRestartReplayVoteproof(t, run.draws, height, target)
		bl := waitRestartReplayBallot(t, run.broadcasts)
		if bl.Point().Round() != target+1 {
			t.Fatalf("next broadcast round=%d want=%d", bl.Point().Round(), target+1)
		}
		waitStateError(t, run.stateErrors, fault)
	} else {
		waitStateError(t, run.stateErrors, fault)
	}
	if boundary == "b_before_snapshot_write" || boundary == "c_after_snapshot_write" {
		select {
		case bl := <-run.broadcasts:
			t.Fatalf("ballot broadcast after snapshot fault: %v", bl.Point())
		case <-time.After(250 * time.Millisecond):
		}
	}
	_ = run.ballotbox.Stop()
	waitRestartReplayQuiet(t, run.broadcasts, run.draws)

	_, found, err := pool.Ballot(base.RawPoint(int64(height), uint64(target+1)), base.StageINIT, false)
	requireNoError(t, err)
	if boundary == "b_before_snapshot_write" || boundary == "c_after_snapshot_write" {
		if found {
			t.Fatal("next ballot persisted after snapshot fault")
		}
	} else if boundary == "d_after_next_ballot_broadcast" && !found {
		t.Fatal("broadcast next ballot was not persisted first")
	}

	requireNoError(t, pool.Close())
	requireNoError(t, storage.Close())
	storage = leveldbstorage.NewFSStorage(dbroot)
	defer storage.Close()
	pool, err = isaacdatabase.NewTempPool(storage, encs, enc, 32)
	requireNoError(t, err)
	defer pool.Close()
	snapshot, found, err := pool.DurableConsensusSnapshot()
	requireNoError(t, err)
	requireTrue(t, found, "snapshot missing after reopen")
	wantRound := target - 1
	if boundary == "c_after_snapshot_write" || boundary == "d_after_next_ballot_broadcast" {
		wantRound = target
	}
	if snapshot.Cap().Point().Round() != wantRound {
		t.Fatalf("reopened snapshot round=%d want=%d", snapshot.Cap().Point().Round(), wantRound)
	}
	loaderctx := util.ContextWithValues(ctx, map[util.ContextKey]interface{}{
		launch.DesignContextKey: launch.NodeDesign{
			Storage: launch.NodeStorageDesign{Base: root}, LocalParams: &launch.LocalParams{ISAAC: params},
		},
		launch.CenterDatabaseContextKey: isaac.Database(db),
		launch.PoolDatabaseContextKey:   pool,
		launch.LeveldbStorageContextKey: storage,
	})
	loaderctx, err = launch.PLoadFromDatabase(loaderctx)
	requireNoError(t, err)
	var recovered *isaac.LastVoteproofsHandler
	requireNoError(t, util.LoadFromContextOK(loaderctx, launch.LastVoteproofsHandlerContextKey, &recovered))
	if !recovered.Last().Cap().Point().Equal(snapshot.Cap().Point()) {
		t.Fatalf("recovered cap=%v persisted cap=%v", recovered.Last().Cap().Point(), snapshot.Cap().Point())
	}
	var noSignBefore base.StagePoint
	requireNoError(t, util.LoadFromContextOK(loaderctx, launch.NoSignBeforeContextKey, &noSignBefore))
	wantBoundary := base.NewStagePoint(snapshot.Cap().Point().Point.NextRound(), base.StageINIT)
	if !noSignBefore.Equal(wantBoundary) {
		t.Fatalf("recovered boundary=%v want=%v", noSignBefore, wantBoundary)
	}
	if boundary == "d_after_next_ballot_broadcast" {
		highest, found, err := pool.HighestBallotPoint()
		requireNoError(t, err)
		requireTrue(t, found, "reopened ballot frontier missing")
		if highest.Compare(wantBoundary) != 0 {
			t.Fatalf("reopened ballot frontier=%v boundary=%v", highest, wantBoundary)
		}
	}
}

func waitStateError(t *testing.T, ch <-chan error, want error) {
	t.Helper()
	select {
	case err := <-ch:
		if !errors.Is(err, want) {
			t.Fatalf("states error=%v want=%v", err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for States.Wait error")
	}
}

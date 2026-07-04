//go:build test

package restarttest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacdatabase "github.com/imfact-labs/mitum2/isaac/database"
	"github.com/imfact-labs/mitum2/launch"
	leveldbstorage "github.com/imfact-labs/mitum2/storage/leveldb"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/valuehash"
)

func TestRestartWithoutSnapshotChecksCommittedBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stage     base.Stage
		round     base.Round
		wantError bool
	}{
		{name: "empty pool allowed"},
		{name: "boundary INIT allowed", stage: base.StageINIT},
		{name: "same-round ACCEPT safe-stop", stage: base.StageACCEPT, wantError: true},
		{name: "higher INIT safe-stop", stage: base.StageINIT, round: 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, encs, enc := restartReplayEncoders(t)
			root := t.TempDir()
			dataroot := launch.LocalFSDataDirectory(root)
			requireNoError(t, os.MkdirAll(dataroot, 0o700))
			local, other := base.RandomLocalNode(), base.RandomLocalNode()
			networkID := base.RandomNetworkID()
			params := isaac.DefaultParams(networkID)
			requireNoError(t, params.SetThreshold(100))
			manifest := base.NewDummyManifest(32, valuehash.RandomSHA256())
			committedINIT, committedACCEPT := restartReplayCommittedVoteproofs(
				t, []base.LocalNode{local, other}, networkID, params.Threshold(), base.RawPoint(32, 0), manifest.Hash(),
			)
			blockmap := writeRestartReplayCommittedBlock(
				t, dataroot, encs, enc, local, networkID, manifest, committedINIT, committedACCEPT,
			)
			suffrage, _ := isaac.NewTestSuffrage(0, local, other)
			db := &restartReplayDatabase{
				blockmap: blockmap, blockmaps: map[base.Height]base.BlockMap{32: blockmap},
				policy: isaac.DefaultNetworkPolicy(), suffrage: suffrage, other: other,
			}
			storage := leveldbstorage.NewFSStorage(filepath.Join(root, "pool"))
			defer storage.Close()
			pool, err := isaacdatabase.NewTempPool(storage, encs, enc, 8)
			requireNoError(t, err)
			defer pool.Close()
			if tc.stage.CanVote() {
				point := base.NewPoint(33, tc.round)
				var ballot base.Ballot
				if tc.stage == base.StageINIT {
					fact := isaac.NewINITBallotFact(point, manifest.Hash(), valuehash.RandomSHA256(), nil)
					sf := isaac.NewINITBallotSignFact(fact)
					requireNoError(t, sf.NodeSign(local.Privatekey(), networkID, local.Address()))
					ballot = isaac.NewINITBallot(committedACCEPT, sf, nil)
				} else {
					proposal := valuehash.RandomSHA256()
					initFact := isaac.NewINITBallotFact(point, manifest.Hash(), proposal, nil)
					initSigns := make([]base.BallotSignFact, 2)
					for i, node := range []base.LocalNode{local, other} {
						sf := isaac.NewINITBallotSignFact(initFact)
						requireNoError(t, sf.NodeSign(node.Privatekey(), networkID, node.Address()))
						initSigns[i] = sf
					}
					ivp := isaac.NewINITVoteproof(point)
					ivp.SetMajority(initFact).SetSignFacts(initSigns).SetThreshold(100).Finish()
					acceptFact := isaac.NewACCEPTBallotFact(point, proposal, valuehash.RandomSHA256(), nil)
					acceptSign := isaac.NewACCEPTBallotSignFact(acceptFact)
					requireNoError(t, acceptSign.NodeSign(local.Privatekey(), networkID, local.Address()))
					ballot = isaac.NewACCEPTBallot(ivp, acceptSign, nil)
				}
				_, err := pool.SetBallot(ballot)
				requireNoError(t, err)
			}
			loaderctx := util.ContextWithValues(ctx, map[util.ContextKey]interface{}{
				launch.DesignContextKey: launch.NodeDesign{
					Storage: launch.NodeStorageDesign{Base: root}, LocalParams: &launch.LocalParams{ISAAC: params},
				},
				launch.CenterDatabaseContextKey: isaac.Database(db),
				launch.PoolDatabaseContextKey:   pool,
				launch.LeveldbStorageContextKey: storage,
			})
			_, err = launch.PLoadFromDatabase(loaderctx)
			if tc.wantError && err == nil {
				t.Fatal("frontier above committed boundary was accepted")
			}
			if !tc.wantError {
				requireNoError(t, err)
			}
		})
	}
}

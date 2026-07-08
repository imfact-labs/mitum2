package launchcmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacblock "github.com/imfact-labs/mitum2/isaac/block"
	isaacdatabase "github.com/imfact-labs/mitum2/isaac/database"
	"github.com/imfact-labs/mitum2/launch"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/logging"
	"github.com/imfact-labs/mitum2/util/ps"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

var PNameStorageRepairFrontier = ps.Name("storage-repair-frontier")

type StorageRepairFrontierCommand struct { //nolint:govet //...
	launch.DesignFlag
	launch.PrivatekeyFlags
	Apply           bool   `name:"apply" help:"actually repair database frontier"`
	From            int64  `name:"from" help:"start height; default db last height"`
	Min             int64  `name:"min" help:"lowest height to scan"`
	CheckVoteproofs bool   `name:"check-voteproofs" default:"true"`
	CheckAllItems   bool   `name:"check-all-items" default:"true"`
	Backup          string `name:"backup" help:"path to existing storage backup archive; required with --apply unless --skip-backup-check"`
	SkipBackupCheck bool   `name:"skip-backup-check" help:"allow apply without backup confirmation"`
	PruneLocalFS    bool   `name:"prune-local-fs" help:"remove incomplete local block item files above recovered frontier"`
	log             *zerolog.Logger
	launch.DevFlags `embed:"" prefix:"dev."`
}

type repairFrontierResult struct {
	dbLatest base.Height
	scanFrom base.Height
	scanMin  base.Height
	frontier base.Height
	bad      map[base.Height]string
}

func (cmd *StorageRepairFrontierCommand) Run(pctx context.Context) error {
	if cmd.Min < 0 {
		return errors.Errorf("invalid --min, %d", cmd.Min)
	}

	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return err
	}

	cmd.log = log.Log()

	nctx := util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		launch.DesignFlagContextKey: cmd.DesignFlag,
		launch.DevFlagsContextKey:   cmd.DevFlags,
		launch.PrivatekeyContextKey: string(cmd.PrivatekeyFlags.Flag.Body()),
	})

	pps := ps.NewPS("cmd-repair-frontier")
	_ = pps.SetLogging(log)

	_ = pps.
		AddOK(launch.PNameEncoder, launch.PEncoder, nil).
		AddOK(launch.PNameDesign, launch.PLoadDesign, nil, launch.PNameEncoder).
		AddOK(launch.PNameLocal, launch.PLocal, nil, launch.PNameDesign).
		AddOK(launch.PNameBlockItemReaders, launch.PBlockItemReaders, nil, launch.PNameDesign).
		AddOK(launch.PNameStorage, launch.PStorage, launch.PCloseStorage, launch.PNameLocal)

	_ = pps.POK(launch.PNameEncoder).
		PostAddOK(launch.PNameAddHinters, launch.PAddHinters)

	_ = pps.POK(launch.PNameDesign).
		PostAddOK(launch.PNameCheckDesign, launch.PCheckDesign)

	_ = pps.POK(launch.PNameBlockItemReaders).
		PreAddOK(launch.PNameBlockItemReadersDecompressFunc, launch.PBlockItemReadersDecompressFunc).
		PostAddOK(launch.PNameRemotesBlockItemReaderFunc, launch.PRemotesBlockItemReaderFunc)

	_ = pps.POK(launch.PNameStorage).
		PreAddOK(launch.PNameCheckLocalFS, launch.PCheckLocalFS).
		PreAddOK(launch.PNameLoadDatabase, launch.PLoadDatabase).
		PostAddOK(launch.PNameCheckLeveldbStorage, launch.PCheckLeveldbStorage).
		PostAddOK(launch.PNamePatchBlockItemReaders, launch.PPatchBlockItemReaders).
		PostAddOK(PNameStorageRepairFrontier, cmd.pRepairFrontier)

	nctx, err := pps.Run(nctx)
	defer func() {
		if _, cerr := pps.Close(nctx); cerr != nil {
			cmd.log.Error().Err(cerr).Msg("failed to close")
		}
	}()

	return err
}

func (cmd *StorageRepairFrontierCommand) pRepairFrontier(pctx context.Context) (context.Context, error) {
	e := util.StringError("storage repair-frontier")

	var design launch.NodeDesign
	var params *isaac.Params
	var db isaac.Database
	var perm isaac.PermanentDatabase
	var newReaders func(context.Context, string, *isaac.BlockItemReadersArgs) (*isaac.BlockItemReaders, error)

	if err := util.LoadFromContextOK(pctx,
		launch.DesignContextKey, &design,
		launch.ISAACParamsContextKey, &params,
		launch.CenterDatabaseContextKey, &db,
		launch.PermanentDatabaseContextKey, &perm,
		launch.NewBlockItemReadersFuncContextKey, &newReaders,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	readers, err := newReaders(pctx, launch.LocalFSDataDirectory(design.Storage.Base), nil)
	if err != nil {
		return pctx, e.Wrap(err)
	}
	defer readers.Close()

	result, err := cmd.findRecoverableFrontier(db, readers, params.NetworkID())
	if err != nil {
		return pctx, e.Wrap(err)
	}

	cmd.printResult(result)

	if !cmd.Apply {
		return pctx, nil
	}

	if result.frontier == base.NilHeight {
		return pctx, e.Errorf("apply refused: no recoverable frontier")
	}

	if !cmd.SkipBackupCheck {
		if cmd.Backup == "" {
			return pctx, e.Errorf("apply refused: --backup is required unless --skip-backup-check")
		}

		if _, err := os.Stat(cmd.Backup); err != nil {
			return pctx, e.WithMessage(err, "apply refused: backup path is not available")
		}
	} else {
		fmt.Println("warning: backup check skipped")
	}

	ldb, ok := perm.(*isaacdatabase.LeveldbPermanent)
	if !ok {
		return pctx, e.Errorf("apply unsupported: permanent database is %T; only leveldb is supported", perm)
	}

	deleted, err := ldb.RepairBlockMapFrontier(result.frontier)
	if err != nil {
		return pctx, e.WithMessage(err, "apply failed; ensure the node is offline and leveldb is not locked")
	}

	fmt.Printf("applied: deleted db blockmap keys: %s\n", formatHeights(deleted))

	if cmd.PruneLocalFS {
		pruned, err := cmd.pruneLocalFS(readers, result.frontier+1, result.dbLatest)
		if err != nil {
			return pctx, e.WithMessage(err, "local fs pruning failed")
		}

		fmt.Printf("applied: pruned local fs files: %d\n", pruned)
	}

	if err := cmd.verifyFrontier(db, readers, result.frontier); err != nil {
		return pctx, e.WithMessage(err, "post-apply verification failed")
	}

	fmt.Printf("result: repair applied; db latest height is now %d\n", result.frontier)

	return pctx, nil
}

func (*StorageRepairFrontierCommand) pruneLocalFS(
	readers *isaac.BlockItemReaders,
	from,
	to base.Height,
) (int, error) {
	if from > to {
		return 0, nil
	}

	var removed int

	for h := from; h <= to; h++ {
		bfiles, found, err := readers.ItemFiles(h)
		if err != nil {
			return removed, err
		}

		if found {
			for _, item := range bfiles.Items() {
				u := item.URI()
				if u.Scheme != isaac.LocalFSBlockItemScheme {
					continue
				}

				p := filepath.Join(readers.Root(), isaac.BlockHeightDirectory(h), strings.TrimPrefix(u.Path, "/"))
				switch err := os.Remove(p); {
				case err == nil:
					removed++
				case os.IsNotExist(err):
				default:
					return removed, err
				}
			}
		}

		switch err := os.Remove(isaac.BlockItemFilesPath(readers.Root(), h)); {
		case err == nil:
			removed++
		case os.IsNotExist(err):
		default:
			return removed, err
		}
	}

	return removed, nil
}

func (cmd *StorageRepairFrontierCommand) findRecoverableFrontier(
	db isaac.Database,
	readers *isaac.BlockItemReaders,
	networkID base.NetworkID,
) (repairFrontierResult, error) {
	result := repairFrontierResult{
		frontier: base.NilHeight,
		bad:      map[base.Height]string{},
		scanMin:  base.Height(cmd.Min),
	}

	last, found, err := db.LastBlockMap()
	if err != nil || !found {
		return result, err
	}

	result.dbLatest = last.Manifest().Height()
	result.scanFrom = result.dbLatest
	if cmd.From > 0 && base.Height(cmd.From) < result.scanFrom {
		result.scanFrom = base.Height(cmd.From)
	}

	for h := result.scanFrom; h >= result.scanMin; h-- {
		switch reason, err := cmd.checkFrontier(db, readers, h, networkID); {
		case err != nil:
			return result, err
		case reason != "":
			result.bad[h] = reason
		default:
			result.frontier = h

			return result, nil
		}
	}

	return result, nil
}

func (cmd *StorageRepairFrontierCommand) checkFrontier(
	db isaac.Database,
	readers *isaac.BlockItemReaders,
	height base.Height,
	networkID base.NetworkID,
) (string, error) {
	dbm, found, err := db.BlockMap(height)
	if err != nil {
		return "", err
	}
	if !found {
		return "db blockmap missing", nil
	}

	itemf := readers.Item

	lm, found, err := isaac.BlockItemReadersDecode[base.BlockMap](itemf, height, base.BlockItemMap, nil)
	if err != nil {
		return fmt.Sprintf("local map invalid: %v", err), nil
	}
	if !found {
		return "local map missing", nil
	}

	if err := base.IsEqualBlockMap(dbm, lm); err != nil {
		return "db/local blockmap different", nil
	}

	if cmd.CheckVoteproofs {
		if _, found, err := isaac.BlockItemReadersDecode[[2]base.Voteproof](itemf, height, base.BlockItemVoteproofs, nil); err != nil {
			return fmt.Sprintf("voteproofs invalid: %v", err), nil
		} else if !found {
			return "voteproofs missing", nil
		}
	}

	if height > base.GenesisHeight {
		pm, found, err := db.BlockMap(height - 1)
		if err != nil {
			return "", err
		}
		if !found {
			return "previous db blockmap missing", nil
		}
		if !dbm.Manifest().Previous().Equal(pm.Manifest().Hash()) {
			return "previous block continuity invalid", nil
		}
	}

	if cmd.CheckAllItems {
		if err := isaacblock.IsValidBlockFromLocalFS(itemf, height, networkID, nil, nil, nil); err != nil {
			return fmt.Sprintf("invalid block items: %v", err), nil
		}
	}

	return "", nil
}

func (cmd *StorageRepairFrontierCommand) verifyFrontier(
	db isaac.Database,
	readers *isaac.BlockItemReaders,
	height base.Height,
) error {
	last, found, err := db.LastBlockMap()
	if err != nil {
		return err
	}
	if !found {
		return errors.Errorf("last blockmap not found after repair")
	}
	if last.Manifest().Height() != height {
		return errors.Errorf("unexpected latest height after repair; expected=%d actual=%d", height, last.Manifest().Height())
	}

	return cmd.verifyLoadFromDatabaseCore(db, readers, height)
}

func (*StorageRepairFrontierCommand) verifyLoadFromDatabaseCore(
	db isaac.Database,
	readers *isaac.BlockItemReaders,
	height base.Height,
) error {
	dbm, found, err := db.BlockMap(height)
	if err != nil {
		return err
	}
	if !found {
		return errors.Errorf("db blockmap missing, %d", height)
	}

	lm, found, err := isaac.BlockItemReadersDecode[base.BlockMap](readers.Item, height, base.BlockItemMap, nil)
	if err != nil {
		return err
	}
	if !found {
		return util.ErrNotFound.Errorf("blockmap in local fs")
	}
	if err := base.IsEqualBlockMap(dbm, lm); err != nil {
		return errors.WithMessage(err, "different blockmap in db and local fs")
	}

	_, found, err = isaac.BlockItemReadersDecode[[2]base.Voteproof](readers.Item, height, base.BlockItemVoteproofs, nil)
	if err != nil {
		return err
	}
	if !found {
		return util.ErrNotFound.Errorf("last voteproofs not found in local fs")
	}

	return nil
}

func (cmd *StorageRepairFrontierCommand) printResult(result repairFrontierResult) {
	mode := "dry-run"
	if cmd.Apply {
		mode = "apply"
	}

	fmt.Printf("storage repair-frontier: %s\n", mode)
	fmt.Printf("db_latest_height: %d\n", result.dbLatest)
	fmt.Printf("scan_from: %d\n", result.scanFrom)
	fmt.Printf("scan_min: %d\n", result.scanMin)

	if result.frontier == base.NilHeight {
		fmt.Println("recoverable_frontier: none")
		fmt.Println("result: no recoverable frontier")

		return
	}

	fmt.Printf("recoverable_frontier: %d\n", result.frontier)

	if len(result.bad) > 0 {
		fmt.Println("bad_heights:")
		for h := result.scanFrom; h > result.frontier; h-- {
			if reason, found := result.bad[h]; found {
				fmt.Printf("  %d: %s\n", h, reason)
			}
		}
	}

	if result.frontier < result.dbLatest {
		fmt.Println("apply_plan:")
		fmt.Printf("  delete db blockmap keys: %d..%d\n", result.frontier+1, result.dbLatest)
		if cmd.PruneLocalFS {
			fmt.Println("  local fs pruning: enabled")
		} else {
			fmt.Println("  local fs pruning: disabled")
		}

		if !cmd.Apply {
			fmt.Println("result: repair needed")
		}

		return
	}

	fmt.Println("result: repair not needed")
}

func formatHeights(heights []base.Height) string {
	if len(heights) < 1 {
		return "none"
	}

	if len(heights) == 1 {
		return fmt.Sprintf("%d", heights[0])
	}

	return fmt.Sprintf("%d..%d", heights[0], heights[len(heights)-1])
}

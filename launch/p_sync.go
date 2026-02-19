package launch

import (
	"context"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacnetwork "github.com/imfact-labs/mitum2/isaac/network"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/imfact-labs/mitum2/util/logging"
	"github.com/imfact-labs/mitum2/util/ps"
	"github.com/pkg/errors"
)

var (
	PNameSyncSourceChecker      = ps.Name("sync-source-checker")
	PNameStartSyncSourceChecker = ps.Name("start-sync-source-checker")
	SyncSourceCheckerContextKey = util.ContextKey("sync-source-checker")
	SyncSourcePoolContextKey    = util.ContextKey("sync-source-pool")
)

func PSyncSourceChecker(pctx context.Context) (context.Context, error) {
	e := util.StringError("prepare SyncSourceChecker")

	var log *logging.Logging
	var encs *encoder.Encoders
	var design NodeDesign
	var local base.LocalNode
	var client isaac.NetworkClient

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		DesignContextKey, &design,
		LocalContextKey, &local,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	sources := append([]isaacnetwork.SyncSource{}, design.SyncSources.Sources()...)

	switch {
	case len(sources) < 1:
		log.Log().Warn().Msg("empty initial sync sources; connected memberlist members will be used")
	default:
		log.Log().Debug().Interface("sync_sources", sources).Msg("initial sync sources found")
	}

	syncSourcePool := isaac.NewSyncSourcePool(nil)

	syncSourceChecker := isaacnetwork.NewSyncSourceChecker(
		local,
		design.LocalParams.ISAAC.NetworkID(),
		client,
		design.LocalParams.MISC.SyncSourceCheckerInterval(),
		encs.Default(),
		sources,
		func(ncis []isaac.NodeConnInfo, _ error) {
			syncSourcePool.UpdateFixed(ncis)

			log.Log().Debug().
				Interface("node_conninfo", ncis).
				Msg("sync sources updated")
		},
		design.LocalParams.Network.TimeoutRequest,
	)
	_ = syncSourceChecker.SetLogging(log)

	return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		SyncSourceCheckerContextKey: syncSourceChecker,
		SyncSourcePoolContextKey:    syncSourcePool,
	}), nil
}

func PStartSyncSourceChecker(pctx context.Context) (context.Context, error) {
	var syncSourceChecker *isaacnetwork.SyncSourceChecker
	if err := util.LoadFromContextOK(pctx, SyncSourceCheckerContextKey, &syncSourceChecker); err != nil {
		return pctx, err
	}

	return pctx, syncSourceChecker.Start(context.Background())
}

func PCloseSyncSourceChecker(pctx context.Context) (context.Context, error) {
	var syncSourceChecker *isaacnetwork.SyncSourceChecker
	if err := util.LoadFromContextOK(pctx,
		SyncSourceCheckerContextKey, &syncSourceChecker,
	); err != nil {
		return pctx, err
	}

	if err := syncSourceChecker.Stop(); err != nil && !errors.Is(err, util.ErrDaemonAlreadyStopped) {
		return pctx, err
	}

	return pctx, nil
}

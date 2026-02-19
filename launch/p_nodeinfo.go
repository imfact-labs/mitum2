package launch

import (
	"context"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacnetwork "github.com/imfact-labs/mitum2/isaac/network"
	isaacstates "github.com/imfact-labs/mitum2/isaac/states"
	nutil "github.com/imfact-labs/mitum2/network/util"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/logging"
	"github.com/imfact-labs/mitum2/util/ps"
	"github.com/pkg/errors"
)

var (
	PNameNodeInfo      = ps.Name("nodeinfo")
	NodeInfoContextKey = util.ContextKey("nodeinfo")
)

func PNodeInfo(pctx context.Context) (context.Context, error) {
	e := util.StringError("prepare nodeInfo")

	var log *logging.Logging
	var version util.Version
	var local base.LocalNode
	var design NodeDesign
	var db isaac.Database

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		VersionContextKey, &version,
		DesignContextKey, &design,
		LocalContextKey, &local,
		CenterDatabaseContextKey, &db,
	); err != nil {
		return pctx, e.Wrap(err)
	}

	nodeInfo := isaacnetwork.NewNodeInfoUpdater(design.NetworkID, local, version)
	_ = nodeInfo.SetConsensusState(isaacstates.StateBooting)
	_ = nodeInfo.SetConnInfo(nutil.ConnInfoToString(
		design.Network.PublishString,
		design.Network.TLSInsecure,
	))

	_ = nodeInfo.SetIsaacParams(design.LocalParams.ISAAC)
	_ = nodeInfo.SetMemberlistParams(design.LocalParams.Memberlist)
	_ = nodeInfo.SetMISCParams(design.LocalParams.MISC)

	nctx := context.WithValue(pctx, NodeInfoContextKey, nodeInfo)

	switch err := UpdateNodeInfoWithNewBlock(db, nodeInfo); {
	case err == nil:
	case errors.Is(err, util.ErrNotFound):
		log.Log().Debug().Err(err).Msg("nodeInfo not updated")
	default:
		log.Log().Error().Err(err).Msg("failed to update nodeInfo")
	}

	return nctx, nil
}

func UpdateNodeInfoWithNewBlock(
	db isaac.Database,
	nodeinfo *isaacnetwork.NodeInfoUpdater,
) error {
	switch m, found, err := db.LastBlockMap(); {
	case err != nil:
		return err
	case !found:
		return util.ErrNotFound.Errorf("last BlockMap")
	case !nodeinfo.SetLastManifest(m.Manifest()):
		return nil
	}

	switch proof, found, err := db.LastSuffrageProof(); {
	case err != nil:
		return errors.WithMessage(err, "last SuffrageProof not found")
	case found && nodeinfo.SetSuffrageHeight(proof.SuffrageHeight()):
		suf, err := proof.Suffrage()
		if err != nil {
			return errors.WithMessage(err, "suffrage from proof")
		}

		_ = nodeinfo.SetConsensusNodes(suf.Nodes())
	}

	_ = nodeinfo.SetNetworkPolicy(db.LastNetworkPolicy())

	return nil
}

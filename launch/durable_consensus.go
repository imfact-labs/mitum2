package launch

import (
	"bytes"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacdatabase "github.com/imfact-labs/mitum2/isaac/database"
	isaacstates "github.com/imfact-labs/mitum2/isaac/states"
	"github.com/imfact-labs/mitum2/util/logging"
	"github.com/pkg/errors"
)

type SetDurableConsensusSnapshotFunc func(isaacdatabase.DurableConsensusSnapshot) (bool, error)

func NewPersistConsensusProgressFunc(
	log *logging.Logging,
	pool *isaacdatabase.TempPool,
	db isaac.Database,
	writers ...SetDurableConsensusSnapshotFunc,
) isaacstates.PersistConsensusProgressFunc {
	write := SetDurableConsensusSnapshotFunc(pool.SetDurableConsensusSnapshot)
	if len(writers) > 0 && writers[0] != nil {
		write = writers[0]
	}
	return func(vp base.Voteproof, previous isaac.LastVoteproofs) error {
		if vp.Result() != base.VoteResultDraw ||
			(vp.Point().Stage() != base.StageINIT && vp.Point().Stage() != base.StageACCEPT) {
			return nil
		}
		if _, ok := vp.(base.StuckVoteproof); ok {
			log.Log().Warn().Object("point", vp.Point()).Msg("unsupported stuck DRAW; durable consensus snapshot unchanged")
			return nil
		}
		if _, ok := vp.(base.HasExpels); ok {
			log.Log().Warn().Object("point", vp.Point()).Msg("unsupported expel DRAW; durable consensus snapshot unchanged")
			return nil
		}
		anchor := previous.Majority()
		previousBlock := previous.PreviousBlockForNextRound(vp)
		if anchor == nil || previousBlock == nil {
			log.Log().Warn().Object("point", vp.Point()).Msg("DRAW without durable majority anchor; snapshot unchanged")
			return nil
		}
		bm, found, err := db.LastBlockMap()
		if err != nil {
			return errors.Wrap(err, "load committed manifest for consensus snapshot")
		}
		if !found || bm == nil || bm.Manifest() == nil || !previousBlock.Equal(bm.Manifest().Hash()) {
			log.Log().Warn().Object("point", vp.Point()).Msg("DRAW anchor does not match committed manifest; snapshot unchanged")
			return nil
		}
		snapshot := isaacdatabase.DurableConsensusSnapshot{
			Version: isaacdatabase.DurableConsensusSnapshotVersion, MajorityAnchor: anchor,
			ManifestHeight: bm.Manifest().Height(), ManifestHash: bm.Manifest().Hash(),
			PreviousBlock: previousBlock, SavedAt: time.Now().UTC(),
		}
		switch vp.Point().Stage() {
		case base.StageINIT:
			snapshot.INIT = vp.(base.INITVoteproof) //nolint:forcetypeassert // stage checked above
			snapshot.ACCEPT = previous.ACCEPT()
		case base.StageACCEPT:
			snapshot.INIT = previous.INIT()
			snapshot.ACCEPT = vp.(base.ACCEPTVoteproof) //nolint:forcetypeassert // stage checked above
		}
		if err := validateDurableConsensusSnapshotShape(snapshot); err != nil {
			log.Log().Warn().Err(err).Object("point", vp.Point()).Msg("unsupported DRAW snapshot; snapshot unchanged")
			return nil
		}
		_, err = write(snapshot)
		return err
	}
}

func validateDurableConsensusSnapshotShape(snapshot isaacdatabase.DurableConsensusSnapshot) error {
	if snapshot.Version != isaacdatabase.DurableConsensusSnapshotVersion {
		return errors.Errorf("unsupported snapshot version %d", snapshot.Version)
	}
	cap := snapshot.Cap()
	if snapshot.INIT == nil || snapshot.ACCEPT == nil || cap == nil || cap.Result() != base.VoteResultDraw {
		return errors.Errorf("snapshot does not contain a coherent DRAW pair")
	}
	if snapshot.MajorityAnchor == nil || snapshot.MajorityAnchor.Result() != base.VoteResultMajority {
		return errors.Errorf("snapshot has no majority anchor")
	}
	if snapshot.ManifestHash == nil || snapshot.PreviousBlock == nil ||
		!snapshot.ManifestHash.Equal(snapshot.PreviousBlock) {
		return errors.Errorf("snapshot manifest and previous-block anchors differ")
	}
	if cap.Point().Height() != snapshot.ManifestHeight+1 {
		return errors.Errorf(
			"snapshot cap height does not follow manifest: cap=%d manifest=%d",
			cap.Point().Height(), snapshot.ManifestHeight,
		)
	}
	switch cap.Point().Stage() {
	case base.StageINIT:
		if !sameVoteproofMeaning(snapshot.INIT, cap) || snapshot.ACCEPT.Point().Height() != cap.Point().Height()-1 ||
			!sameVoteproofMeaning(snapshot.MajorityAnchor, snapshot.ACCEPT) {
			return errors.Errorf("invalid INIT DRAW snapshot pair")
		}
	case base.StageACCEPT:
		if !sameVoteproofMeaning(snapshot.ACCEPT, cap) || !snapshot.INIT.Point().Point.Equal(cap.Point().Point) ||
			snapshot.INIT.Result() != base.VoteResultMajority ||
			!sameVoteproofMeaning(snapshot.MajorityAnchor, snapshot.INIT) {
			return errors.Errorf("invalid ACCEPT DRAW snapshot pair")
		}
	default:
		return errors.Errorf("unsupported snapshot cap stage %s", cap.Point().Stage())
	}
	return nil
}

func validateDurableConsensusSnapshot(
	snapshot isaacdatabase.DurableConsensusSnapshot,
	db isaac.Database,
	networkID base.NetworkID,
) error {
	if err := validateDurableConsensusSnapshotShape(snapshot); err != nil {
		return err
	}
	for name, vp := range map[string]base.Voteproof{
		"init": snapshot.INIT, "accept": snapshot.ACCEPT, "majority_anchor": snapshot.MajorityAnchor,
	} {
		if err := base.IsValidVoteproof(vp, networkID); err != nil {
			return errors.Wrapf(err, "invalid snapshot %s voteproof", name)
		}
		suf, found, err := isaac.GetSuffrageFromDatabase(db, vp.Point().Height().SafePrev())
		switch {
		case err != nil:
			return errors.Wrapf(err, "load snapshot %s voteproof suffrage", name)
		case !found:
			return errors.Errorf("snapshot %s voteproof suffrage not found", name)
		case suf == nil:
			return errors.Errorf("snapshot %s voteproof has nil suffrage", name)
		case suf.Len() < 1:
			return errors.Errorf("snapshot %s voteproof has empty suffrage", name)
		}
		if err := isaac.IsValidVoteproofWithSuffrage(vp, suf); err != nil {
			return errors.Wrapf(err, "invalid snapshot %s voteproof with suffrage", name)
		}
	}
	return nil
}

func nextLegalPoint(snapshot isaacdatabase.DurableConsensusSnapshot) (base.StagePoint, error) {
	if err := validateDurableConsensusSnapshotShape(snapshot); err != nil {
		return base.StagePoint{}, err
	}
	return base.NewStagePoint(snapshot.Cap().Point().Point.NextRound(), base.StageINIT), nil
}

func sameVoteproofMeaning(a, b base.Voteproof) bool {
	if a == nil || b == nil || !a.Point().Equal(b.Point()) || a.Result() != b.Result() || a.Threshold() != b.Threshold() {
		return false
	}
	if (a.Majority() == nil) != (b.Majority() == nil) {
		return false
	}
	if a.Majority() != nil && !a.Majority().Hash().Equal(b.Majority().Hash()) {
		return false
	}
	as, bs := a.SignFacts(), b.SignFacts()
	if len(as) != len(bs) {
		return false
	}
	for i := range as {
		if !bytes.Equal(as[i].HashBytes(), bs[i].HashBytes()) {
			return false
		}
	}
	return true
}

func recoverDurableConsensusSnapshot(
	lvps *isaac.LastVoteproofsHandler,
	committedINIT base.INITVoteproof,
	committedACCEPT base.ACCEPTVoteproof,
	snapshot isaacdatabase.DurableConsensusSnapshot,
) (base.StagePoint, error) {
	if err := validateDurableConsensusSnapshotShape(snapshot); err != nil {
		return base.StagePoint{}, err
	}
	cap := snapshot.Cap()
	switch cap.Point().Stage() {
	case base.StageINIT:
		if !sameVoteproofMeaning(snapshot.MajorityAnchor, committedACCEPT) || !lvps.Set(snapshot.INIT) {
			return base.StagePoint{}, errors.Errorf("rejected INIT DRAW recovery")
		}
	case base.StageACCEPT:
		if !lvps.Set(snapshot.INIT) || !lvps.Set(snapshot.ACCEPT) {
			return base.StagePoint{}, errors.Errorf("rejected ACCEPT DRAW recovery")
		}
	}
	last := lvps.Last()
	if !sameVoteproofMeaning(last.Cap(), cap) || !sameVoteproofMeaning(last.Majority(), snapshot.MajorityAnchor) {
		return base.StagePoint{}, errors.Errorf("recovered cap or majority differs from snapshot")
	}
	previousBlock := last.PreviousBlockForNextRound(last.Cap())
	if previousBlock == nil || !previousBlock.Equal(snapshot.PreviousBlock) {
		return base.StagePoint{}, errors.Errorf("recovered previous block differs from snapshot")
	}
	_ = committedINIT // committed steps are performed by PLoadFromDatabase before this function
	return nextLegalPoint(snapshot)
}

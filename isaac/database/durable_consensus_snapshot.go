package isaacdatabase

import (
	"encoding/json"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/imfact-labs/mitum2/util/valuehash"
	"github.com/pkg/errors"
	leveldbopt "github.com/syndtr/goleveldb/leveldb/opt"
)

const DurableConsensusSnapshotVersion uint64 = 1

type DurableConsensusSnapshot struct {
	Version        uint64
	INIT           base.INITVoteproof
	ACCEPT         base.ACCEPTVoteproof
	MajorityAnchor base.Voteproof
	ManifestHeight base.Height
	ManifestHash   util.Hash
	PreviousBlock  util.Hash
	SavedAt        time.Time
}

func (s DurableConsensusSnapshot) Cap() base.Voteproof {
	if s.INIT == nil {
		return s.ACCEPT
	}
	if s.ACCEPT == nil || s.INIT.Point().Compare(s.ACCEPT.Point()) > 0 {
		return s.INIT
	}
	return s.ACCEPT
}

func (s DurableConsensusSnapshot) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		Version        uint64               `json:"version"`
		INIT           base.INITVoteproof   `json:"init"`
		ACCEPT         base.ACCEPTVoteproof `json:"accept"`
		MajorityAnchor base.Voteproof       `json:"majority_anchor"`
		ManifestHeight base.Height          `json:"manifest_height"`
		ManifestHash   util.Hash            `json:"manifest_hash"`
		PreviousBlock  util.Hash            `json:"previous_block"`
		SavedAt        time.Time            `json:"saved_at"`
	}{s.Version, s.INIT, s.ACCEPT, s.MajorityAnchor, s.ManifestHeight, s.ManifestHash, s.PreviousBlock, s.SavedAt})
}

func (s *DurableConsensusSnapshot) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u struct {
		Version        uint64                `json:"version"`
		INIT           json.RawMessage       `json:"init"`
		ACCEPT         json.RawMessage       `json:"accept"`
		MajorityAnchor json.RawMessage       `json:"majority_anchor"`
		ManifestHeight base.Height           `json:"manifest_height"`
		ManifestHash   valuehash.HashDecoder `json:"manifest_hash"`
		PreviousBlock  valuehash.HashDecoder `json:"previous_block"`
		SavedAt        time.Time             `json:"saved_at"`
	}
	if err := enc.Unmarshal(b, &u); err != nil {
		return errors.Wrap(err, "decode durable consensus snapshot")
	}
	decode := func(raw json.RawMessage) (base.Voteproof, error) {
		if len(raw) < 1 || string(raw) == "null" {
			return nil, nil
		}
		i, err := enc.Decode(raw)
		if err != nil {
			return nil, err
		}
		vp, ok := i.(base.Voteproof)
		if !ok {
			return nil, errors.Errorf("expected voteproof, got %T", i)
		}
		return vp, nil
	}
	iv, err := decode(u.INIT)
	if err != nil {
		return errors.Wrap(err, "decode INIT voteproof")
	}
	av, err := decode(u.ACCEPT)
	if err != nil {
		return errors.Wrap(err, "decode ACCEPT voteproof")
	}
	mv, err := decode(u.MajorityAnchor)
	if err != nil {
		return errors.Wrap(err, "decode majority anchor")
	}
	if iv != nil {
		var ok bool
		s.INIT, ok = iv.(base.INITVoteproof)
		if !ok {
			return errors.Errorf("expected INIT voteproof, got %T", iv)
		}
	}
	if av != nil {
		var ok bool
		s.ACCEPT, ok = av.(base.ACCEPTVoteproof)
		if !ok {
			return errors.Errorf("expected ACCEPT voteproof, got %T", av)
		}
	}
	s.Version, s.MajorityAnchor = u.Version, mv
	s.ManifestHeight, s.ManifestHash = u.ManifestHeight, u.ManifestHash.Hash()
	s.PreviousBlock, s.SavedAt = u.PreviousBlock.Hash(), u.SavedAt
	return nil
}

func durableConsensusSnapshotKey() []byte { return leveldbKeyDurableConsensusSnapshot[:] }

func (db *TempPool) DurableConsensusSnapshot() (DurableConsensusSnapshot, bool, error) {
	db.durableConsensusSnapshotLock.Lock()
	defer db.durableConsensusSnapshotLock.Unlock()
	return db.durableConsensusSnapshotLocked()
}

func (db *TempPool) durableConsensusSnapshotLocked() (DurableConsensusSnapshot, bool, error) {
	pst, err := db.st()
	if err != nil {
		return DurableConsensusSnapshot{}, false, err
	}
	b, found, err := pst.Get(durableConsensusSnapshotKey())
	if err != nil || !found {
		return DurableConsensusSnapshot{}, found, err
	}
	var snapshot DurableConsensusSnapshot
	enchint, _, body, err := ReadFrame(b)
	if err != nil {
		return DurableConsensusSnapshot{}, true, errors.Wrap(err, "read durable consensus snapshot frame")
	}
	_, enc, found, err := db.encs.FindByString(enchint)
	if err != nil {
		return DurableConsensusSnapshot{}, true, err
	}
	if !found {
		return DurableConsensusSnapshot{}, true, errors.Errorf("snapshot encoder not found for %q", enchint)
	}
	if err := snapshot.DecodeJSON(body, enc); err != nil {
		return DurableConsensusSnapshot{}, true, errors.Wrap(err, "decode durable consensus snapshot")
	}
	return snapshot, true, nil
}

func (db *TempPool) SetDurableConsensusSnapshot(snapshot DurableConsensusSnapshot) (bool, error) {
	db.durableConsensusSnapshotLock.Lock()
	defer db.durableConsensusSnapshotLock.Unlock()
	if snapshot.Cap() == nil {
		return false, errors.Errorf("empty durable consensus snapshot cap")
	}

	if old, found, err := db.durableConsensusSnapshotLocked(); err != nil {
		return false, err
	} else if found && snapshot.Cap().Point().Compare(old.Cap().Point()) < 1 {
		return false, nil
	}
	pst, err := db.st()
	if err != nil {
		return false, err
	}
	_, b, err := EncodeFrame(db.enc, nil, snapshot)
	if err != nil {
		return false, err
	}
	if err := pst.Put(durableConsensusSnapshotKey(), b, &leveldbopt.WriteOptions{Sync: true}); err != nil {
		return false, errors.Wrap(err, "write durable consensus snapshot")
	}
	return true, nil
}

func (db *TempPool) RemoveDurableConsensusSnapshot() error {
	db.durableConsensusSnapshotLock.Lock()
	defer db.durableConsensusSnapshotLock.Unlock()
	pst, err := db.st()
	if err != nil {
		return err
	}
	return pst.Delete(durableConsensusSnapshotKey(), &leveldbopt.WriteOptions{Sync: true})
}

func (db *TempPool) RemoveDurableConsensusSnapshotIfHeight(committedHeight base.Height) (bool, error) {
	db.durableConsensusSnapshotLock.Lock()
	defer db.durableConsensusSnapshotLock.Unlock()

	snapshot, found, err := db.durableConsensusSnapshotLocked()
	if err != nil || !found {
		return false, err
	}
	cap := snapshot.Cap()
	if cap == nil {
		return false, errors.Errorf("empty durable consensus snapshot cap")
	}
	if cap.Point().Height() > committedHeight {
		return false, nil
	}
	pst, err := db.st()
	if err != nil {
		return false, err
	}
	if err := pst.Delete(durableConsensusSnapshotKey(), &leveldbopt.WriteOptions{Sync: true}); err != nil {
		return false, errors.Wrap(err, "conditionally remove durable consensus snapshot")
	}
	return true, nil
}

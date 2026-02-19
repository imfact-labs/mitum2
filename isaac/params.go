package isaac

import (
	"reflect"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/hint"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

var ParamsHint = hint.MustNewHint("isaac-params-v0.0.1")

var (
	DefaultntervalBroadcastBallot     = time.Second * 3
	DefaultWaitPreparingINITBallot    = time.Second * 5
	DefaultWaitStuckInterval          = time.Second * 33
	DefaultTimeoutRequest             = time.Second * 3
	DefaultMinWaitNextBlockINITBallot = time.Second * 2
	DefaultSyncerLastBlockMapInterval = time.Second * 2
	DefaultMinProposerWait            = time.Second * 4
	DefaultStateCacheSize             = 1 << 12
	DefaultOperationPoolCacheSize     = 1 << 12
	DefaultBroadcastTimerMult         = 5
)

type Params struct {
	*util.BaseParams
	networkID base.NetworkID
	hint.BaseHinter
	threshold                     base.Threshold
	intervalBroadcastBallot       time.Duration
	waitPreparingINITBallot       time.Duration
	ballotStuckWait               time.Duration
	ballotStuckResolveAfter       time.Duration
	minWaitNextBlockINITBallot    time.Duration
	syncerLastBlockMapInterval    time.Duration
	minProposerWait               time.Duration
	maxTryHandoverYBrokerSyncData uint64
	stateCacheSize                int
	operationPoolCacheSize        int
}

func NewParams(networkID base.NetworkID) *Params {
	return &Params{
		BaseParams: util.NewBaseParams(),
		BaseHinter: hint.NewBaseHinter(ParamsHint),
		networkID:  networkID,
	}
}

func DefaultParams(networkID base.NetworkID) *Params {
	return &Params{
		BaseParams:                    util.NewBaseParams(),
		BaseHinter:                    hint.NewBaseHinter(ParamsHint),
		networkID:                     networkID,
		threshold:                     base.DefaultThreshold,
		intervalBroadcastBallot:       DefaultntervalBroadcastBallot,
		waitPreparingINITBallot:       DefaultWaitPreparingINITBallot,
		ballotStuckWait:               time.Second * 33, //nolint:gomnd // waitPreparingINITBallot * 10
		ballotStuckResolveAfter:       time.Second * 66, //nolint:gomnd // ballotStuckWait * 2
		syncerLastBlockMapInterval:    DefaultSyncerLastBlockMapInterval,
		minProposerWait:               DefaultMinProposerWait,
		maxTryHandoverYBrokerSyncData: 33, //nolint:gomnd //...
		minWaitNextBlockINITBallot:    DefaultMinWaitNextBlockINITBallot,
		stateCacheSize:                DefaultStateCacheSize,
		operationPoolCacheSize:        DefaultOperationPoolCacheSize,
	}
}

func (p *Params) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid Params")

	if err := p.BaseParams.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := p.BaseHinter.IsValid(ParamsHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if !p.networkID.Equal(networkID) {
		return e.Errorf("network id does not match")
	}

	if err := util.CheckIsValiders(networkID, false, p.networkID, p.threshold); err != nil {
		return e.Wrap(err)
	}

	if p.intervalBroadcastBallot < 0 {
		return e.Errorf("wrong duration; invalid intervalBroadcastBallot")
	}

	if p.waitPreparingINITBallot < 0 {
		return e.Errorf("wrong duration; invalid waitPreparingINITBallot")
	}

	if p.ballotStuckWait < 0 {
		return e.Errorf("wrong duration; invalid ballotStuckWait")
	}

	if p.ballotStuckResolveAfter < 0 {
		return e.Errorf("wrong duration; invalid ballotStuckResolveAfter")
	}

	if p.minWaitNextBlockINITBallot < 0 {
		return e.Errorf("wrong duration; invalid minWaitNextBlockINITBallot")
	}

	if p.syncerLastBlockMapInterval < 0 {
		return e.Errorf("wrong duration; invalid syncerLastBlockMapInterval")
	}

	if p.minProposerWait < 0 {
		return e.Errorf("wrong duration; invalid minProposerWait")
	}

	if p.stateCacheSize < 0 {
		return e.Errorf("wrong state cache size")
	}

	if p.operationPoolCacheSize < 0 {
		return e.Errorf("wrong operation pool cache size")
	}

	return nil
}

func (p *Params) NetworkID() base.NetworkID {
	p.RLock()
	defer p.RUnlock()

	return p.networkID
}

func (p *Params) SetNetworkID(n base.NetworkID) error {
	if err := n.IsValid(nil); err != nil {
		return err
	}

	return p.Set(func() (bool, error) {
		if n == nil {
			return false, errors.Errorf("empty network id")
		}

		p.networkID = n

		return true, nil
	})
}

func (p *Params) Threshold() base.Threshold {
	p.RLock()
	defer p.RUnlock()

	return p.threshold
}

func (p *Params) SetThreshold(t base.Threshold) error {
	if err := t.IsValid(nil); err != nil {
		return err
	}

	return p.Set(func() (bool, error) {
		switch {
		case t < 1:
			return false, errors.Errorf("under zero")
		case p.threshold == t:
			return false, nil
		default:
			p.threshold = t

			return true, nil
		}
	})
}

func (p *Params) IntervalBroadcastBallot() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.intervalBroadcastBallot
}

func (p *Params) SetIntervalBroadcastBallot(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.intervalBroadcastBallot == d {
			return false, nil
		}

		p.intervalBroadcastBallot = d

		return true, nil
	})
}

func (p *Params) WaitPreparingINITBallot() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.waitPreparingINITBallot
}

func (p *Params) SetWaitPreparingINITBallot(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.waitPreparingINITBallot == d {
			return false, nil
		}

		p.waitPreparingINITBallot = d

		return true, nil
	})
}

func (p *Params) BallotStuckWait() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.ballotStuckWait
}

func (p *Params) SetBallotStuckWait(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.ballotStuckWait == d {
			return false, nil
		}

		p.ballotStuckWait = d

		return true, nil
	})
}

func (p *Params) BallotStuckResolveAfter() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.ballotStuckResolveAfter
}

func (p *Params) SetBallotStuckResolveAfter(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.ballotStuckResolveAfter == d {
			return false, nil
		}

		p.ballotStuckResolveAfter = d

		return true, nil
	})
}

func (p *Params) SyncerLastBlockMapInterval() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.syncerLastBlockMapInterval
}

func (p *Params) SetSyncerLastBlockMapInterval(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.syncerLastBlockMapInterval == d {
			return false, nil
		}

		p.syncerLastBlockMapInterval = d

		return true, nil
	})
}

func (p *Params) MinProposerWait() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.minProposerWait
}

func (p *Params) SetMinProposerWait(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.minProposerWait == d {
			return false, nil
		}

		p.minProposerWait = d

		return true, nil
	})
}

func (p *Params) MaxTryHandoverYBrokerSyncData() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.maxTryHandoverYBrokerSyncData
}

func (p *Params) SetMaxTryHandoverYBrokerSyncData(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.maxTryHandoverYBrokerSyncData == d {
			return false, nil
		}

		p.maxTryHandoverYBrokerSyncData = d

		return true, nil
	})
}

// MinWaitNextBlockINITBallot is used for waiting until the proposer creates new
// block for new proposal; Too short MinWaitNextBlockINITBallot may cause empty
// proposal.
func (p *Params) MinWaitNextBlockINITBallot() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.minWaitNextBlockINITBallot
}

func (p *Params) SetMinWaitNextBlockINITBallot(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.minWaitNextBlockINITBallot == d {
			return false, nil
		}

		p.minWaitNextBlockINITBallot = d

		return true, nil
	})
}

func (p *Params) StateCacheSize() int {
	p.RLock()
	defer p.RUnlock()

	return p.stateCacheSize
}

func (p *Params) SetStateCacheSize(d int) error {
	return p.SetInt(d, func(d int) (bool, error) {
		switch {
		case d < 0:
			return false, errors.Errorf("under zero")
		case p.stateCacheSize == d:
			return false, nil
		default:
			p.stateCacheSize = d

			return true, nil
		}
	})
}

func (p *Params) OperationPoolCacheSize() int {
	p.RLock()
	defer p.RUnlock()

	return p.operationPoolCacheSize
}

func (p *Params) SetOperationPoolCacheSize(d int) error {
	return p.SetInt(d, func(d int) (bool, error) {
		switch {
		case d < 0:
			return false, errors.Errorf("under zero")
		case p.operationPoolCacheSize == d:
			return false, nil
		default:
			p.operationPoolCacheSize = d

			return true, nil
		}
	})
}

type paramsJSONMarshaler struct {
	//revive:disable:line-length-limit
	hint.BaseHinter
	Threshold                     base.Threshold        `json:"threshold,omitempty"`
	IntervalBroadcastBallot       util.ReadableDuration `json:"interval_broadcast_ballot,omitempty"`
	WaitPreparingINITBallot       util.ReadableDuration `json:"wait_preparing_init_ballot,omitempty"`
	BallotStuckWait               util.ReadableDuration `json:"ballot_stuck_wait,omitempty"`
	BallotStuckResolveAfter       util.ReadableDuration `json:"ballot_stuck_resolve_after,omitempty"`
	MinWaitNextBlockINITBallot    util.ReadableDuration `json:"min_wait_next_block_init_ballot,omitempty"`
	SyncerLastBlockMapInterval    util.ReadableDuration `json:"syncer_last_block_map_interval,omitempty"`
	MinProposerWait               util.ReadableDuration `json:"min_proposer_wait,omitempty"`
	MaxTryHandoverYBrokerSyncData uint64                `json:"max_try_handover_y_broker_sync_data,omitempty"`
	StateCacheSize                int                   `json:"state_cache_size,omitempty"`
	OperationPoolCacheSize        int                   `json:"operation_pool_cache_size,omitempty"`
	//revive:enable:line-length-limit
}

func (p *Params) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(paramsJSONMarshaler{
		BaseHinter:                    p.BaseHinter,
		Threshold:                     p.threshold,
		IntervalBroadcastBallot:       util.ReadableDuration(p.intervalBroadcastBallot),
		WaitPreparingINITBallot:       util.ReadableDuration(p.waitPreparingINITBallot),
		BallotStuckResolveAfter:       util.ReadableDuration(p.ballotStuckResolveAfter),
		BallotStuckWait:               util.ReadableDuration(p.ballotStuckWait),
		MinWaitNextBlockINITBallot:    util.ReadableDuration(p.minWaitNextBlockINITBallot),
		SyncerLastBlockMapInterval:    util.ReadableDuration(p.syncerLastBlockMapInterval),
		MinProposerWait:               util.ReadableDuration(p.minProposerWait),
		MaxTryHandoverYBrokerSyncData: p.maxTryHandoverYBrokerSyncData,
		StateCacheSize:                p.stateCacheSize,
		OperationPoolCacheSize:        p.operationPoolCacheSize,
	})
}

type paramsJSONUnmarshaler struct {
	//revive:disable:line-length-limit
	Threshold                     interface{}            `json:"threshold"`
	IntervalBroadcastBallot       *util.ReadableDuration `json:"interval_broadcast_ballot"`
	WaitPreparingINITBallot       *util.ReadableDuration `json:"wait_preparing_init_ballot"`
	BallotStuckWait               *util.ReadableDuration `json:"ballot_stuck_wait,omitempty"`
	BallotStuckResolveAfter       *util.ReadableDuration `json:"ballot_stuck_resolve_after,omitempty"`
	MinWaitNextBlockINITBallot    *util.ReadableDuration `json:"min_wait_next_block_init_ballot,omitempty"`
	SyncerLastBlockMapInterval    *util.ReadableDuration `json:"syncer_last_block_map_interval,omitempty"`
	MinProposerWait               *util.ReadableDuration `json:"min_proposer_wait,omitempty"`
	MaxTryHandoverYBrokerSyncData *uint64                `json:"max_try_handover_y_broker_sync_data,omitempty"`
	StateCacheSize                *int                   `json:"state_cache_size,omitempty"`
	OperationPoolCacheSize        *int                   `json:"operation_pool_cache_size,omitempty"`
	hint.BaseHinter
	//revive:enable:line-length-limit
}

func (p *Params) UnmarshalJSON(b []byte) error {
	var u paramsJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "unmarshal Params")
	}

	p.BaseParams = util.NewBaseParams()
	p.BaseHinter = u.BaseHinter

	if u.MaxTryHandoverYBrokerSyncData != nil {
		p.maxTryHandoverYBrokerSyncData = *u.MaxTryHandoverYBrokerSyncData
	}

	if u.StateCacheSize != nil {
		p.stateCacheSize = *u.StateCacheSize
	}

	if u.OperationPoolCacheSize != nil {
		p.operationPoolCacheSize = *u.OperationPoolCacheSize
	}

	durargs := [][2]interface{}{
		{u.IntervalBroadcastBallot, &p.intervalBroadcastBallot},
		{u.WaitPreparingINITBallot, &p.waitPreparingINITBallot},
		{u.BallotStuckResolveAfter, &p.ballotStuckResolveAfter},
		{u.BallotStuckWait, &p.ballotStuckWait},
		{u.MinWaitNextBlockINITBallot, &p.minWaitNextBlockINITBallot},
		{u.SyncerLastBlockMapInterval, &p.syncerLastBlockMapInterval},
		{u.MinProposerWait, &p.minProposerWait},
	}

	for i := range durargs {
		v := durargs[i][0].(*util.ReadableDuration) //nolint:forcetypeassert //...
		t := durargs[i][1].(*time.Duration)         //nolint:forcetypeassert //...

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.SetInterfaceValue(time.Duration(*v), t); err != nil {
			return err
		}
	}

	switch t := u.Threshold.(type) {
	case string:
		if err := p.threshold.UnmarshalText([]byte(t)); err != nil {
			return err
		}
	case float64:
		p.threshold = base.Threshold(t)
	case int64:
		p.threshold = base.Threshold(float64(t))
	}

	return nil
}

var MiscParamsHint = hint.MustNewHint("misc-params-v0.0.1")

type MISCParams struct {
	*util.BaseParams
	hint.BaseHinter
	syncSourceCheckerInterval             time.Duration
	validProposalOperationExpire          time.Duration
	validProposalSuffrageOperationsExpire time.Duration
	blockItemReadersRemoveEmptyAfter      time.Duration
	blockItemReadersRemoveEmptyInterval   time.Duration
	maxMessageSize                        uint64
	objectCacheSize                       uint64
}

func DefaultMISCParams() *MISCParams {
	return &MISCParams{
		BaseParams:                            util.NewBaseParams(),
		BaseHinter:                            hint.NewBaseHinter(MiscParamsHint),
		syncSourceCheckerInterval:             time.Second * 30, //nolint:gomnd //...
		validProposalOperationExpire:          time.Hour * 24,   //nolint:gomnd //...
		validProposalSuffrageOperationsExpire: time.Hour * 2,
		blockItemReadersRemoveEmptyAfter:      DefaultBlockItemReadersRemoveEmptyAfter,
		blockItemReadersRemoveEmptyInterval:   DefaultBlockItemReadersRemoveEmptyInterval,
		maxMessageSize:                        1 << 18, //nolint:gomnd //...
		objectCacheSize:                       1 << 13, //nolint:gomnd // big enough
	}
}

func (p *MISCParams) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid MISCParams")

	if err := p.BaseParams.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if p.syncSourceCheckerInterval < 0 {
		return e.Errorf("wrong duration; invalid syncSourceCheckerInterval")
	}

	if p.validProposalOperationExpire < 0 {
		return e.Errorf("wrong duration; invalid validProposalOperationExpire")
	}

	if p.validProposalSuffrageOperationsExpire < 0 {
		return e.Errorf("wrong duration; invalid validProposalSuffrageOperationsExpire")
	}

	if p.blockItemReadersRemoveEmptyAfter < 0 {
		return e.Errorf("wrong duration; invalid blockItemReadersRemoveEmptyAfter")
	}

	if p.blockItemReadersRemoveEmptyInterval < 0 {
		return e.Errorf("wrong duration; invalid blockItemReadersRemoveEmptyInterval")
	}

	if p.maxMessageSize < 1 {
		return e.Errorf("wrong maxMessageSize")
	}

	if p.objectCacheSize < 1 {
		return e.Errorf("wrong objectCacheSize")
	}

	return nil
}

// SyncSourceCheckerInterval is the interval to check the liveness of sync
// sources.
func (p *MISCParams) SyncSourceCheckerInterval() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.syncSourceCheckerInterval
}

func (p *MISCParams) SetSyncSourceCheckerInterval(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.syncSourceCheckerInterval == d {
			return false, nil
		}

		p.syncSourceCheckerInterval = d

		return true, nil
	})
}

// ValidProposalOperationExpire is the maximum creation time for valid
// operation. If the creation time of operation is older than
// ValidProposalOperationExpire, it will be ignored.
func (p *MISCParams) ValidProposalOperationExpire() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.validProposalOperationExpire
}

func (p *MISCParams) SetValidProposalOperationExpire(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.validProposalOperationExpire == d {
			return false, nil
		}

		p.validProposalOperationExpire = d

		return true, nil
	})
}

// ValidProposalSuffrageOperationsExpire is the maximum creation time for valid
// suffrage operations like isaacoperation.SuffrageCandidate operation. If the
// creation time of suffrage operation is older than
// ValidProposalSuffrageOperationsExpire, it will be ignored.
func (p *MISCParams) ValidProposalSuffrageOperationsExpire() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.validProposalSuffrageOperationsExpire
}

func (p *MISCParams) SetValidProposalSuffrageOperationsExpire(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.validProposalSuffrageOperationsExpire == d {
			return false, nil
		}

		p.validProposalSuffrageOperationsExpire = d

		return true, nil
	})
}

// BlockItemReadersRemoveEmptyAfter removes empty block item directory after
// the duration. Zero duration not allowed.
func (p *MISCParams) BlockItemReadersRemoveEmptyAfter() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.blockItemReadersRemoveEmptyAfter
}

func (p *MISCParams) SetBlockItemReadersRemoveEmptyAfter(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.blockItemReadersRemoveEmptyAfter == d {
			return false, nil
		}

		p.blockItemReadersRemoveEmptyAfter = d

		return true, nil
	})
}

// BlockItemReadersRemoveEmptyInterval is the interval to remove empty block
// item directory after BlockItemReadersRemoveEmptyAfter. Zero duration not
// allowed.
func (p *MISCParams) BlockItemReadersRemoveEmptyInterval() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.blockItemReadersRemoveEmptyInterval
}

func (p *MISCParams) SetBlockItemReadersRemoveEmptyInterval(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.blockItemReadersRemoveEmptyInterval == d {
			return false, nil
		}

		p.blockItemReadersRemoveEmptyInterval = d

		return true, nil
	})
}

// MaxMessageSize is the maximum size of incoming messages like ballot or
// operation. If message size is over, it will be ignored.
func (p *MISCParams) MaxMessageSize() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.maxMessageSize
}

func (p *MISCParams) SetMaxMessageSize(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.maxMessageSize == d {
			return false, nil
		}

		p.maxMessageSize = d

		return true, nil
	})
}

// ObjectCacheSize is the cache size for various internal objects like address
// or keypair.
func (p *MISCParams) ObjectCacheSize() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.objectCacheSize
}

func (p *MISCParams) SetObjectCacheSize(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.objectCacheSize == d {
			return false, nil
		}

		p.objectCacheSize = d

		return true, nil
	})
}

type miscParamsYAMLMarshaler struct {
	//revive:disable:line-length-limit
	hint.BaseHinter
	SyncSourceCheckerInterval             util.ReadableDuration `json:"sync_source_checker_interval,omitempty" yaml:"sync_source_checker_interval,omitempty"`
	ValidProposalOperationExpire          util.ReadableDuration `json:"valid_proposal_operation_expire,omitempty" yaml:"valid_proposal_operation_expire,omitempty"`
	ValidProposalSuffrageOperationsExpire util.ReadableDuration `json:"valid_proposal_suffrage_operations_expire,omitempty" yaml:"valid_proposal_suffrage_operations_expire,omitempty"`
	BlockItemReadersRemoveEmptyAfter      util.ReadableDuration `json:"block_item_readers_remove_empty_after,omitempty" yaml:"block_item_readers_remove_empty_after,omitempty"`
	BlockItemReadersRemoveEmptyInterval   util.ReadableDuration `json:"block_item_readers_remove_empty_interval,omitempty" yaml:"block_item_readers_remove_empty_interval,omitempty"`
	MaxMessageSize                        uint64                `json:"max_message_size,omitempty" yaml:"max_message_size,omitempty"`
	ObjectCacheSize                       uint64                `json:"object_cache_size,omitempty" yaml:"object_cache_size,omitempty"`
	//revive:enable:line-length-limit
}

func (p *MISCParams) marshaler() miscParamsYAMLMarshaler {
	return miscParamsYAMLMarshaler{
		BaseHinter:                            p.BaseHinter,
		SyncSourceCheckerInterval:             util.ReadableDuration(p.syncSourceCheckerInterval),
		ValidProposalOperationExpire:          util.ReadableDuration(p.validProposalOperationExpire),
		ValidProposalSuffrageOperationsExpire: util.ReadableDuration(p.validProposalSuffrageOperationsExpire),
		BlockItemReadersRemoveEmptyAfter:      util.ReadableDuration(p.blockItemReadersRemoveEmptyAfter),
		BlockItemReadersRemoveEmptyInterval:   util.ReadableDuration(p.blockItemReadersRemoveEmptyInterval),
		MaxMessageSize:                        p.maxMessageSize,
		ObjectCacheSize:                       p.objectCacheSize,
	}
}

func (p *MISCParams) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(p.marshaler())
}

func (p *MISCParams) MarshalYAML() (interface{}, error) {
	return p.marshaler(), nil
}

type miscParamsYAMLUnmarshaler struct {
	//revive:disable:line-length-limit
	SyncSourceCheckerInterval             *util.ReadableDuration `json:"sync_source_checker_interval,omitempty" yaml:"sync_source_checker_interval,omitempty"`
	ValidProposalOperationExpire          *util.ReadableDuration `json:"valid_proposal_operation_expire,omitempty" yaml:"valid_proposal_operation_expire,omitempty"`
	ValidProposalSuffrageOperationsExpire *util.ReadableDuration `json:"valid_proposal_suffrage_operations_expire,omitempty" yaml:"valid_proposal_suffrage_operations_expire,omitempty"`
	BlockItemReadersRemoveEmptyAfter      *util.ReadableDuration `json:"block_item_readers_remove_empty_after,omitempty" yaml:"block_item_readers_remove_empty_after,omitempty"`
	BlockItemReadersRemoveEmptyInterval   *util.ReadableDuration `json:"block_item_readers_remove_empty_interval,omitempty" yaml:"block_item_readers_remove_empty_interval,omitempty"`
	MaxMessageSize                        *uint64                `json:"max_message_size,omitempty" yaml:"max_message_size,omitempty"`
	ObjectCacheSize                       *uint64                `json:"object_cache_size,omitempty" yaml:"object_cache_size,omitempty"`
	hint.BaseHinter
	//revive:enable:line-length-limit
}

func (p *MISCParams) UnmarshalJSON(b []byte) error {
	d := DefaultMISCParams()
	*p = *d

	e := util.StringError("decode MISCParams")

	var u miscParamsYAMLUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}
	p.BaseHinter = u.BaseHinter

	return e.Wrap(p.unmarshal(u))
}

func (p *MISCParams) UnmarshalYAML(y *yaml.Node) error {
	d := DefaultMISCParams()
	*p = *d

	e := util.StringError("decode MISCParams")

	var u miscParamsYAMLUnmarshaler

	if err := y.Decode(&u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *MISCParams) unmarshal(u miscParamsYAMLUnmarshaler) error {
	if u.MaxMessageSize != nil {
		p.maxMessageSize = *u.MaxMessageSize
	}

	if u.ObjectCacheSize != nil {
		p.objectCacheSize = *u.ObjectCacheSize
	}

	durargs := [][2]interface{}{
		{u.SyncSourceCheckerInterval, &p.syncSourceCheckerInterval},
		{u.ValidProposalOperationExpire, &p.validProposalOperationExpire},
		{u.ValidProposalSuffrageOperationsExpire, &p.validProposalSuffrageOperationsExpire},
		{u.BlockItemReadersRemoveEmptyAfter, &p.blockItemReadersRemoveEmptyAfter},
		{u.BlockItemReadersRemoveEmptyInterval, &p.blockItemReadersRemoveEmptyInterval},
	}

	for i := range durargs {
		v := durargs[i][0].(*util.ReadableDuration) //nolint:forcetypeassert //...
		t := durargs[i][1].(*time.Duration)         //nolint:forcetypeassert //...

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.SetInterfaceValue(time.Duration(*v), t); err != nil {
			return err
		}
	}

	return nil
}

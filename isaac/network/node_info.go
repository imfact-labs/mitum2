package isaacnetwork

import (
	"sync"
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacstates "github.com/imfact-labs/mitum2/isaac/states"
	"github.com/imfact-labs/mitum2/network/quicmemberlist"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/hint"
	"github.com/imfact-labs/mitum2/util/localtime"
	"github.com/pkg/errors"
)

var NodeInfoHint = hint.MustNewHint("node-info-v0.0.1")

type NodeInfo struct {
	lastVote         NodeInfoLastVote
	networkID        base.NetworkID
	address          base.Address
	publickey        base.Publickey
	lastManifest     base.Manifest
	networkPolicy    base.NetworkPolicy
	isaacParams      *isaac.Params
	memberlistParams *quicmemberlist.MemberlistParams
	miscParams       *isaac.MISCParams
	connInfo         string
	consensusState   isaacstates.StateType
	consensusNodes   []base.Node
	hint.BaseHinter
	startedAt      time.Time
	version        util.Version
	suffrageHeight base.Height
}

func (info NodeInfo) IsValid(networkID base.NetworkID) error {
	e := util.ErrInvalid.Errorf("invalid NodeInfo")

	if err := util.CheckIsValiders(networkID, false,
		info.consensusState,
		info.lastManifest,
		info.suffrageHeight,
		info.networkPolicy,
		info.isaacParams,
		info.memberlistParams,
		info.miscParams,
		util.DummyIsValider(func([]byte) error {
			if len(info.connInfo) < 1 {
				return errors.Errorf("empty conn info")
			}

			return nil
		}),
		info.version,
		util.DummyIsValider(func([]byte) error {
			if info.startedAt.IsZero() {
				return errors.Errorf("zero started at")
			}

			return nil
		}),
	); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiderSlice(nil, false, info.consensusNodes); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (info NodeInfo) Address() base.Address {
	return info.address
}

func (info NodeInfo) Publickey() base.Publickey {
	return info.publickey
}

func (info NodeInfo) LastManifest() base.Manifest {
	return info.lastManifest
}

func (info NodeInfo) SuffrageHeight() base.Height {
	return info.suffrageHeight
}

func (info NodeInfo) NetworkPolicy() base.NetworkPolicy {
	return info.networkPolicy
}

func (info NodeInfo) IsaacParams() *isaac.Params {
	return info.isaacParams
}

func (info NodeInfo) MemberlistParams() *quicmemberlist.MemberlistParams {
	return info.memberlistParams
}

func (info NodeInfo) MISCParams() *isaac.MISCParams {
	return info.miscParams
}

func (info NodeInfo) ConnInfo() string {
	return info.connInfo
}

func (info NodeInfo) ConsensusNodes() []base.Node {
	return info.consensusNodes
}

func (info NodeInfo) Version() util.Version {
	return info.version
}

func (info NodeInfo) ConsensusState() string {
	return info.consensusState.String()
}

func (info NodeInfo) LastVote() NodeInfoLastVote {
	return info.lastVote
}

func (info NodeInfo) StartedAt() time.Time {
	return info.startedAt
}

type NodeInfoUpdater struct {
	id string
	n  NodeInfo
	sync.RWMutex
}

func NewNodeInfoUpdater(networkID base.NetworkID, local base.Node, version util.Version) *NodeInfoUpdater {
	return &NodeInfoUpdater{
		n: NodeInfo{
			BaseHinter:     hint.NewBaseHinter(NodeInfoHint),
			networkID:      networkID,
			address:        local.Address(),
			publickey:      local.Publickey(),
			suffrageHeight: base.NilHeight,
			version:        version,
			lastVote:       EmptyNodeInfoLastVote(),
			startedAt:      localtime.Now().UTC(),
		},
	}
}

func (info *NodeInfoUpdater) ID() string {
	info.RLock()
	defer info.RUnlock()

	return info.id
}

func (info *NodeInfoUpdater) NodeInfo() NodeInfo {
	info.RLock()
	defer info.RUnlock()

	return info.n
}

func (info *NodeInfoUpdater) SetConsensusState(s isaacstates.StateType) bool {
	return info.set(func() bool {
		if info.n.consensusState == s {
			return false
		}

		info.n.consensusState = s

		return true
	})
}

func (info *NodeInfoUpdater) SetLastManifest(m base.Manifest) bool {
	return info.set(func() bool {
		switch {
		case info.n.lastManifest == nil, m == nil:
		case info.n.lastManifest.Hash().Equal(m.Hash()):
			return false
		}

		info.n.lastManifest = m

		return true
	})
}

func (info *NodeInfoUpdater) SetSuffrageHeight(h base.Height) bool {
	return info.set(func() bool {
		if info.n.suffrageHeight == h {
			return false
		}

		info.n.suffrageHeight = h

		return true
	})
}

func (info *NodeInfoUpdater) SetNetworkPolicy(p base.NetworkPolicy) bool {
	return info.set(func() bool {
		if base.IsEqualNetworkPolicy(info.n.networkPolicy, p) {
			return false
		}

		info.n.networkPolicy = p

		return true
	})
}

func (info *NodeInfoUpdater) SetIsaacParams(p *isaac.Params) bool {
	return info.set(func() bool {
		switch {
		case info.n.isaacParams == nil, p == nil:
		case info.n.isaacParams.ID() == p.ID():
			return false
		}

		info.n.isaacParams = p
		info.n.networkID = p.NetworkID()

		return true
	})
}

func (info *NodeInfoUpdater) SetMemberlistParams(p *quicmemberlist.MemberlistParams) bool {
	return info.set(func() bool {
		switch {
		case info.n.memberlistParams == nil, p == nil:
		case info.n.memberlistParams.ID() == p.ID():
			return false
		}

		info.n.memberlistParams = p

		return true
	})
}

func (info *NodeInfoUpdater) SetMISCParams(p *isaac.MISCParams) bool {
	return info.set(func() bool {
		switch {
		case info.n.miscParams == nil, p == nil:
		case info.n.miscParams.ID() == p.ID():
			return false
		}

		info.n.miscParams = p

		return true
	})
}

func (info *NodeInfoUpdater) SetConnInfo(c string) bool {
	return info.set(func() bool {
		if info.n.connInfo == c {
			return false
		}

		info.n.connInfo = c

		return true
	})
}

func (info *NodeInfoUpdater) SetConsensusNodes(nodes []base.Node) bool {
	return info.set(func() bool {
		if len(info.n.consensusNodes) == len(nodes) {
			var hasnew bool

			for i := range nodes {
				if util.CountFilteredSlice(info.n.consensusNodes, func(a base.Node) bool {
					return !base.IsEqualNode(nodes[i], a)
				}) > 0 {
					hasnew = true

					break
				}
			}

			if !hasnew {
				return false
			}
		}

		info.n.consensusNodes = nodes

		return true
	})
}

func (info *NodeInfoUpdater) SetLastVote(point base.StagePoint, result base.VoteResult) bool {
	return info.set(func() bool {
		if point.Compare(info.n.lastVote.Point) < 1 {
			return false
		}

		info.n.lastVote = NodeInfoLastVote{Point: point, Result: result}

		return true
	})
}

func (info *NodeInfoUpdater) set(f func() bool) bool {
	info.Lock()
	defer info.Unlock()

	switch {
	case f():
		info.id = util.UUID().String()

		return true
	default:
		return false
	}
}

type NodeInfoLastVote struct {
	Result base.VoteResult `json:"result"`
	Point  base.StagePoint `json:"point"`
}

func EmptyNodeInfoLastVote() NodeInfoLastVote {
	return NodeInfoLastVote{Point: base.ZeroStagePoint, Result: base.VoteResultNotYet}
}

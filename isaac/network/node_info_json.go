package isaacnetwork

import (
	"encoding/json"
	"reflect"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacstates "github.com/imfact-labs/mitum2/isaac/states"
	"github.com/imfact-labs/mitum2/network/quicmemberlist"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/imfact-labs/mitum2/util/hint"
	"github.com/imfact-labs/mitum2/util/localtime"
	"github.com/pkg/errors"
)

type NodeInfoLocalJSONMarshaler struct {
	Address          base.Address                     `json:"address"`
	Publickey        base.Publickey                   `json:"publickey"`
	IsaacParams      *isaac.Params                    `json:"isaac_parameters"` //nolint:tagliatelle //...
	MemberlistParams *quicmemberlist.MemberlistParams `json:"memberlist_parameters"`
	MISCParams       *isaac.MISCParams                `json:"misc_parameters"`
	NetworkParams    json.RawMessage                  `json:"network_parameters,omitempty"`
	ConnInfo         string                           `json:"conn_info"`
	StartedAt        localtime.Time                   `json:"started_at"`
	Version          util.Version                     `json:"version"`
}

type NodeInfoSuffrageJSONMarshaler struct {
	Nodes  []base.Node `json:"nodes"`
	Height base.Height `json:"height"`
}

type NodeInfoConsensusJSONMarshaler struct {
	LastVote NodeInfoLastVote              `json:"last_vote"`
	State    isaacstates.StateType         `json:"state"`
	Suffrage NodeInfoSuffrageJSONMarshaler `json:"suffrage"`
}

type NodeInfoJSONMarshaler struct {
	NetworkID     base.NetworkID                 `json:"network_id"`
	LastManifest  base.Manifest                  `json:"last_manifest"`
	NetworkPolicy base.NetworkPolicy             `json:"network_policy"`
	Local         NodeInfoLocalJSONMarshaler     `json:"local"`
	Consensus     NodeInfoConsensusJSONMarshaler `json:"consensus"`
	hint.BaseHinter
}

func (info NodeInfo) JSONMarshaler() NodeInfoJSONMarshaler {
	return NodeInfoJSONMarshaler{
		BaseHinter: info.BaseHinter,
		NetworkID:  info.networkID,
		Local: NodeInfoLocalJSONMarshaler{
			Address:          info.address,
			Publickey:        info.publickey,
			IsaacParams:      info.isaacParams,
			MemberlistParams: info.memberlistParams,
			MISCParams:       info.miscParams,
			NetworkParams:    info.networkParams,
			ConnInfo:         info.connInfo,
			Version:          info.version,
			StartedAt:        localtime.New(info.startedAt),
		},
		Consensus: NodeInfoConsensusJSONMarshaler{
			State: info.consensusState,
			Suffrage: NodeInfoSuffrageJSONMarshaler{
				Height: info.suffrageHeight,
				Nodes:  info.consensusNodes,
			},
			LastVote: info.lastVote,
		},
		LastManifest:  info.lastManifest,
		NetworkPolicy: info.networkPolicy,
	}
}

func (info NodeInfo) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(info.JSONMarshaler())
}

type nodeInfoJSONUnmarshaler struct {
	NetworkID     base.NetworkID                   `json:"network_id"`
	LastManifest  json.RawMessage                  `json:"last_manifest"`
	NetworkPolicy json.RawMessage                  `json:"network_policy"`
	Consensus     nodeInfoConsensusJSONUnmarshaler `json:"consensus"`
	Local         nodeInfoLocalJSONUnmarshaler     `json:"local"`
}

type nodeInfoLocalJSONUnmarshaler struct {
	Address          string          `json:"address"`
	Publickey        string          `json:"publickey"`
	ConnInfo         string          `json:"conn_info"`
	StartedAt        localtime.Time  `json:"started_at"`
	IsaacParams      json.RawMessage `json:"isaac_parameters"` //nolint:tagliatelle //...
	MemberlistParams json.RawMessage `json:"memberlist_parameters"`
	MISCParams       json.RawMessage `json:"misc_parameters"`
	NetworkParams    json.RawMessage `json:"network_parameters,omitempty"`
	Version          util.Version    `json:"version"`
}

type nodeInfoConsensusJSONUnmarshaler struct {
	LastVote NodeInfoLastVote                `json:"last_vote"`
	State    isaacstates.StateType           `json:"state"`
	Suffrage nodeInfoSuffrageJSONUnmarshaler `json:"suffrage"`
}

type nodeInfoSuffrageJSONUnmarshaler struct {
	Nodes  []json.RawMessage `json:"nodes"`
	Height base.Height       `json:"height"`
}

func (info *NodeInfo) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode NodeInfo")

	var u nodeInfoJSONUnmarshaler

	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	info.SetNetworkParams(u.Local.NetworkParams)

	info.networkID = u.NetworkID
	info.startedAt = u.Local.StartedAt.Time

	// NOTE local
	switch i, err := base.DecodeAddress(u.Local.Address, enc); {
	case err != nil:
		return e.Wrap(err)
	default:
		info.address = i
	}

	switch i, err := base.DecodePublickeyFromString(u.Local.Publickey, enc); {
	case err != nil:
		return e.Wrap(err)
	default:
		info.publickey = i
	}

	//isaacParams := isaac.NewParams(info.networkID)
	var isaacParams *isaac.Params

	ipHinter, err := enc.Decode(u.Local.IsaacParams)
	if err != nil {
		return e.Wrap(err)
	}

	if ipHinter == nil {
		return nil
	}

	i, ok := ipHinter.(*isaac.Params)
	if !ok {
		return errors.Errorf("expected %v, but %T", reflect.TypeOf(isaacParams).Elem(), ipHinter)
	}

	isaacParams = i

	if err := isaacParams.SetNetworkID(info.networkID); err != nil {
		return e.Wrap(err)
	}

	info.isaacParams = isaacParams

	var memberlistParams *quicmemberlist.MemberlistParams

	mpHinter, err := enc.Decode(u.Local.MemberlistParams)
	if err != nil {
		return e.Wrap(err)
	}

	if mpHinter == nil {
		return nil
	}

	j, ok := mpHinter.(*quicmemberlist.MemberlistParams)
	if !ok {
		return errors.Errorf("expected %v, but %T", reflect.TypeOf(memberlistParams).Elem(), mpHinter)
	}

	memberlistParams = j

	info.memberlistParams = memberlistParams

	var miscParams *isaac.MISCParams

	mscHinter, err := enc.Decode(u.Local.MISCParams)
	if err != nil {
		return e.Wrap(err)
	}

	if mscHinter == nil {
		return nil
	}

	k, ok := mscHinter.(*isaac.MISCParams)
	if !ok {
		return errors.Errorf("expected %v, but %T", reflect.TypeOf(miscParams).Elem(), mscHinter)
	}

	miscParams = k

	info.miscParams = miscParams

	info.connInfo = u.Local.ConnInfo
	info.version = u.Local.Version

	// NOTE consensus
	info.consensusState = u.Consensus.State

	// NOTE suffrage
	info.suffrageHeight = u.Consensus.Suffrage.Height

	info.consensusNodes = make([]base.Node, len(u.Consensus.Suffrage.Nodes))
	for i := range u.Consensus.Suffrage.Nodes {
		if err := encoder.Decode(enc, u.Consensus.Suffrage.Nodes[i], &info.consensusNodes[i]); err != nil {
			return e.Wrap(err)
		}
	}

	// NOTE last manifest
	if err := encoder.Decode(enc, u.LastManifest, &info.lastManifest); err != nil {
		return e.Wrap(err)
	}

	// NOTE network policy
	if err := encoder.Decode(enc, u.NetworkPolicy, &info.networkPolicy); err != nil {
		return e.Wrap(err)
	}

	info.lastVote = u.Consensus.LastVote

	return nil
}

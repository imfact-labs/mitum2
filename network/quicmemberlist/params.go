package quicmemberlist

import (
	"reflect"
	"time"

	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/hint"
	"gopkg.in/yaml.v3"
)

var ParamsHint = hint.MustNewHint("memberlist-params-v0.0.1")

type MemberlistParams struct {
	*util.BaseParams
	hint.BaseHinter
	tcpTimeout              time.Duration
	retransmitMult          int
	probeTimeout            time.Duration
	probeInterval           time.Duration
	gossipInterval          time.Duration
	suspicionMult           int
	suspicionMaxTimeoutMult int
	udpBufferSize           int
	extraSameMemberLimit    uint64
	broadcastTimerMult      int
	userMsgLoopInterval     time.Duration
	gosshipNodes            int
}

func DefaultMemberlistParams() *MemberlistParams {
	config := BasicMemberlistConfig()

	return &MemberlistParams{
		BaseParams:              util.NewBaseParams(),
		BaseHinter:              hint.NewBaseHinter(ParamsHint),
		tcpTimeout:              config.TCPTimeout,
		retransmitMult:          config.RetransmitMult,
		probeTimeout:            config.ProbeTimeout,
		probeInterval:           config.ProbeInterval,
		gossipInterval:          config.GossipInterval,
		suspicionMult:           config.SuspicionMult,
		suspicionMaxTimeoutMult: config.SuspicionMaxTimeoutMult,
		udpBufferSize:           config.UDPBufferSize,
		extraSameMemberLimit:    1, //nolint:gomnd //...
		broadcastTimerMult:      5,
		userMsgLoopInterval:     time.Millisecond * 33,
		gosshipNodes:            config.GossipNodes,
	}
}

func (*MemberlistParams) IsValid([]byte) error {
	return nil
}

func (p *MemberlistParams) TCPTimeout() time.Duration {
	return p.tcpTimeout
}

func (p *MemberlistParams) SetTCPTimeout(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.tcpTimeout == d {
			return false, nil
		}

		p.tcpTimeout = d

		return true, nil
	})
}

func (p *MemberlistParams) RetransmitMult() int {
	return p.retransmitMult
}

func (p *MemberlistParams) SetRetransmitMult(d int) error {
	return p.SetOverZeroInt(d, func(d int) (bool, error) {
		if p.retransmitMult == d {
			return false, nil
		}

		p.retransmitMult = d

		return true, nil
	})
}

func (p *MemberlistParams) ProbeTimeout() time.Duration {
	return p.probeTimeout
}

func (p *MemberlistParams) SetProbeTimeout(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.probeTimeout == d {
			return false, nil
		}

		p.probeTimeout = d

		return true, nil
	})
}

func (p *MemberlistParams) ProbeInterval() time.Duration {
	return p.probeInterval
}

func (p *MemberlistParams) SetProbeInterval(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.probeInterval == d {
			return false, nil
		}

		p.probeInterval = d

		return true, nil
	})
}

func (p *MemberlistParams) GossipInterval() time.Duration {
	return p.gossipInterval
}

func (p *MemberlistParams) SetGossipInterval(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.gossipInterval == d {
			return false, nil
		}

		p.gossipInterval = d

		return true, nil
	})
}

func (p *MemberlistParams) GosshipNodes() int {
	return p.gosshipNodes
}

func (p *MemberlistParams) SetGosshipNodes(d int) error {
	return p.SetOverZeroInt(d, func(d int) (bool, error) {
		if p.gosshipNodes == d {
			return false, nil
		}

		p.gosshipNodes = d

		return true, nil
	})
}

func (p *MemberlistParams) SuspicionMult() int {
	return p.suspicionMult
}

func (p *MemberlistParams) SetSuspicionMult(d int) error {
	return p.SetOverZeroInt(d, func(d int) (bool, error) {
		if p.suspicionMult == d {
			return false, nil
		}

		p.suspicionMult = d

		return true, nil
	})
}

func (p *MemberlistParams) SuspicionMaxTimeoutMult() int {
	return p.suspicionMaxTimeoutMult
}

func (p *MemberlistParams) SetSuspicionMaxTimeoutMult(d int) error {
	return p.SetOverZeroInt(d, func(d int) (bool, error) {
		if p.suspicionMaxTimeoutMult == d {
			return false, nil
		}

		p.suspicionMaxTimeoutMult = d

		return true, nil
	})
}

func (p *MemberlistParams) UDPBufferSize() int {
	return p.udpBufferSize
}

func (p *MemberlistParams) SetUDPBufferSize(d int) error {
	return p.SetOverZeroInt(d, func(d int) (bool, error) {
		if p.udpBufferSize == d {
			return false, nil
		}

		p.udpBufferSize = d

		return true, nil
	})
}

func (p *MemberlistParams) ExtraSameMemberLimit() uint64 {
	return p.extraSameMemberLimit
}

func (p *MemberlistParams) SetExtraSameMemberLimit(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.extraSameMemberLimit == d {
			return false, nil
		}

		p.extraSameMemberLimit = d

		return true, nil
	})
}

func (p *MemberlistParams) BroadcastTimerMult() int {
	return p.broadcastTimerMult
}

func (p *MemberlistParams) SetBroadcastTimerMult(d int) error {
	return p.SetOverZeroInt(d, func(d int) (bool, error) {
		if p.broadcastTimerMult == d {
			return false, nil
		}

		p.broadcastTimerMult = d

		return true, nil
	})
}

func (p *MemberlistParams) UserMsgLoopInterval() time.Duration {
	return p.userMsgLoopInterval
}

func (p *MemberlistParams) SetUserMsgLoopInterval(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.userMsgLoopInterval == d {
			return false, nil
		}

		p.userMsgLoopInterval = d

		return true, nil
	})
}

type memberlistParamsMarshaler struct {
	//revive:disable:line-length-limit
	hint.BaseHinter
	TCPTimeout              util.ReadableDuration `json:"tcp_timeout,omitempty" yaml:"tcp_timeout,omitempty"`
	RetransmitMult          int                   `json:"retransmit_mult,omitempty" yaml:"retransmit_mult,omitempty"`
	ProbeTimeout            util.ReadableDuration `json:"probe_timeout,omitempty" yaml:"probe_timeout,omitempty"`
	ProbeInterval           util.ReadableDuration `json:"probe_interval,omitempty" yaml:"probe_interval,omitempty"`
	GossipInterval          util.ReadableDuration `json:"gossip_interval,omitempty" yaml:"gossip_interval,omitempty"`
	GosshipNodes            int                   `json:"gossip_nodes,omitempty" yaml:"gossip_nodes,omitempty"`
	SuspicionMult           int                   `json:"suspicion_mult,omitempty" yaml:"suspicion_mult,omitempty"`
	SuspicionMaxTimeoutMult int                   `json:"suspicion_max_timeout_mult,omitempty" yaml:"suspicion_max_timeout_mult,omitempty"`
	UDPBufferSize           int                   `json:"udp_buffer_size,omitempty" yaml:"udp_buffer_size,omitempty"`
	ExtraSameMemberLimit    uint64                `json:"extra_same_member_limit,omitempty" yaml:"extra_same_member_limit,omitempty"`
	BroadcastTimerMult      int                   `json:"broadcast_timer_mult,omitempty" yaml:"broadcast_timer_mult,omitempty"`
	UserMsgLoopInterval     util.ReadableDuration `json:"user_msg_loop_interval,omitempty" yaml:"user_msg_loop_interval,omitempty"`
	//revive:enable:line-length-limit
}

func (p *MemberlistParams) marshaler() memberlistParamsMarshaler {
	return memberlistParamsMarshaler{
		BaseHinter:              p.BaseHinter,
		TCPTimeout:              util.ReadableDuration(p.tcpTimeout),
		RetransmitMult:          p.retransmitMult,
		ProbeTimeout:            util.ReadableDuration(p.probeTimeout),
		ProbeInterval:           util.ReadableDuration(p.probeInterval),
		GossipInterval:          util.ReadableDuration(p.gossipInterval),
		GosshipNodes:            p.gosshipNodes,
		SuspicionMult:           p.suspicionMult,
		SuspicionMaxTimeoutMult: p.suspicionMaxTimeoutMult,
		UDPBufferSize:           p.udpBufferSize,
		ExtraSameMemberLimit:    p.extraSameMemberLimit,
		BroadcastTimerMult:      p.broadcastTimerMult,
		UserMsgLoopInterval:     util.ReadableDuration(p.userMsgLoopInterval),
	}
}

func (p *MemberlistParams) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(p.marshaler())
}

func (p *MemberlistParams) MarshalYAML() (interface{}, error) {
	return p.marshaler(), nil
}

type memberlistParamsUnmarshaler struct {
	//revive:disable:line-length-limit
	TCPTimeout              *util.ReadableDuration `json:"tcp_timeout,omitempty" yaml:"tcp_timeout,omitempty"`
	RetransmitMult          *int                   `json:"retransmit_mult,omitempty" yaml:"retransmit_mult,omitempty"`
	ProbeTimeout            *util.ReadableDuration `json:"probe_timeout,omitempty" yaml:"probe_timeout,omitempty"`
	ProbeInterval           *util.ReadableDuration `json:"probe_interval,omitempty" yaml:"probe_interval,omitempty"`
	GossipInterval          *util.ReadableDuration `json:"gossip_interval,omitempty" yaml:"gossip_interval,omitempty"`
	GosshipNodes            *int                   `json:"gossip_nodes,omitempty" yaml:"gossip_nodes,omitempty"`
	SuspicionMult           *int                   `json:"suspicion_mult,omitempty" yaml:"suspicion_mult,omitempty"`
	SuspicionMaxTimeoutMult *int                   `json:"suspicion_max_timeout_mult,omitempty" yaml:"suspicion_max_timeout_mult,omitempty"`
	UDPBufferSize           *int                   `json:"udp_buffer_size,omitempty" yaml:"udp_buffer_size,omitempty"`
	ExtraSameMemberLimit    *uint64                `json:"extra_same_member_limit,omitempty" yaml:"extra_same_member_limit,omitempty"`
	BroadcastTimerMult      *int                   `json:"broadcast_timer_mult,omitempty" yaml:"broadcast_timer_mult,omitempty"`
	UserMsgLoopInterval     *util.ReadableDuration `json:"user_msg_loop_interval,omitempty" yaml:"user_msg_loop_interval,omitempty"`
	hint.BaseHinter
	//revive:enable:line-length-limit
}

func (p *MemberlistParams) UnmarshalJSON(b []byte) error {
	d := DefaultMemberlistParams()
	*p = *d

	e := util.StringError("unmarshal MemberlistParams")

	var u memberlistParamsUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}
	p.BaseHinter = u.BaseHinter

	return e.Wrap(p.unmarshal(u))
}

func (p *MemberlistParams) UnmarshalYAML(y *yaml.Node) error {
	d := DefaultMemberlistParams()
	*p = *d

	e := util.StringError("unmarshal MemberlistParams")

	var u memberlistParamsUnmarshaler

	if err := y.Decode(&u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *MemberlistParams) unmarshal(u memberlistParamsUnmarshaler) error {
	if u.RetransmitMult != nil {
		p.retransmitMult = *u.RetransmitMult
	}

	if u.SuspicionMult != nil {
		p.suspicionMult = *u.SuspicionMult
	}

	if u.SuspicionMaxTimeoutMult != nil {
		p.suspicionMaxTimeoutMult = *u.SuspicionMaxTimeoutMult
	}

	if u.UDPBufferSize != nil {
		p.udpBufferSize = *u.UDPBufferSize
	}

	if u.ExtraSameMemberLimit != nil {
		p.extraSameMemberLimit = *u.ExtraSameMemberLimit
	}

	if u.BroadcastTimerMult != nil {
		p.broadcastTimerMult = *u.BroadcastTimerMult
	}

	if u.GosshipNodes != nil {
		p.gosshipNodes = *u.GosshipNodes
	}

	durargs := [][2]interface{}{
		{u.TCPTimeout, &p.tcpTimeout},
		{u.ProbeTimeout, &p.probeTimeout},
		{u.ProbeInterval, &p.probeInterval},
		{u.GossipInterval, &p.gossipInterval},
		{u.UserMsgLoopInterval, &p.userMsgLoopInterval},
	}

	for i := range durargs {
		v := durargs[i][0].(*util.ReadableDuration) //nolint:forcetypeassert //...
		t := durargs[i][1].(*time.Duration)         //nolint:forcetypeassert //...

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.SetInterfaceValue[time.Duration](time.Duration(*v), t); err != nil {
			return err
		}
	}

	return nil
}

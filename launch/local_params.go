package launch

import (
	"time"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	isaacnetwork "github.com/imfact-labs/mitum2/isaac/network"
	"github.com/imfact-labs/mitum2/network/quicmemberlist"
	"github.com/imfact-labs/mitum2/network/quicstream"
	"github.com/imfact-labs/mitum2/util"
	"github.com/pkg/errors"
)

var (
	defaultHandlerTimeouts     map[quicstream.HandlerName]time.Duration
	networkHandlerPrefixMap    = map[quicstream.HandlerName]struct{}{}
	NetworkHandlerPrefixMapRev = map[quicstream.HandlerPrefix]quicstream.HandlerName{}
)

func init() {
	defaultHandlerTimeouts = map[quicstream.HandlerName]time.Duration{
		isaacnetwork.HandlerNameAskHandover:    0,
		isaacnetwork.HandlerNameCheckHandover:  0,
		isaacnetwork.HandlerNameCheckHandoverX: 0,
		isaacnetwork.HandlerNameStartHandover:  0,
	}

	for i := range networkHandlerNames {
		s := networkHandlerNames[i]
		networkHandlerPrefixMap[s] = struct{}{}
		NetworkHandlerPrefixMapRev[quicstream.HashPrefix(s)] = s
	}
}

type LocalParams struct {
	// ISAAC sets the consensus related parameters.
	ISAAC *isaac.Params `yaml:"isaac,omitempty" json:"isaac,omitempty"`
	// Memberlist sets the memberlist parameters. memberlist handles the
	// connections of suffrage nodes. For details, see
	// https://pkg.go.dev/github.com/hashicorp/memberlist#Config .
	Memberlist *quicmemberlist.MemberlistParams `yaml:"memberlist,omitempty" json:"memberlist,omitempty"`
	// Network sets the network related parameters. For details, see
	// https://pkg.go.dev/github.com/quic-go/quic-go#Config .
	Network *NetworkParams `yaml:"network,omitempty" json:"network,omitempty"`
	// MISC sets misc parameters.
	MISC *isaac.MISCParams `yaml:"misc,omitempty" json:"misc,omitempty"`
}

func defaultLocalParams(networkID base.NetworkID) *LocalParams {
	return &LocalParams{
		ISAAC:      isaac.DefaultParams(networkID),
		Memberlist: quicmemberlist.DefaultMemberlistParams(),
		MISC:       isaac.DefaultMISCParams(),
		Network:    defaultNetworkParams(),
	}
}

func (p *LocalParams) IsValid(networkID base.NetworkID) error {
	e := util.ErrInvalid.Errorf("invalid IsaacParams")

	if p.ISAAC == nil {
		return e.Errorf("empty ISAAC")
	}

	if err := p.ISAAC.SetNetworkID(networkID); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(networkID, false,
		p.ISAAC,
		p.Memberlist,
		p.MISC,
		p.Network); err != nil {
		return e.Wrap(err)
	}

	return nil
}

type NetworkParams struct {
	*util.BaseParams
	rateLimit             *NetworkRateLimitParams
	handlerTimeouts       map[quicstream.HandlerName]time.Duration
	timeoutRequest        time.Duration
	handshakeIdleTimeout  time.Duration
	maxIdleTimeout        time.Duration
	keepAlivePeriod       time.Duration
	defaultHandlerTimeout time.Duration
	connectionPoolSize    uint64
	maxIncomingStreams    uint64
	maxStreamTimeout      time.Duration
}

func defaultNetworkParams() *NetworkParams {
	handlerTimeouts := map[quicstream.HandlerName]time.Duration{}
	for i := range defaultHandlerTimeouts {
		handlerTimeouts[i] = defaultHandlerTimeouts[i]
	}

	d := DefaultServerQuicConfig()

	return &NetworkParams{
		BaseParams:            util.NewBaseParams(),
		timeoutRequest:        isaac.DefaultTimeoutRequest,
		handshakeIdleTimeout:  d.HandshakeIdleTimeout,
		maxIdleTimeout:        d.MaxIdleTimeout,
		keepAlivePeriod:       d.KeepAlivePeriod,
		defaultHandlerTimeout: time.Second * 6, //nolint:gomnd //...
		handlerTimeouts:       handlerTimeouts,
		connectionPoolSize:    1 << 13, //nolint:gomnd // big enough
		maxIncomingStreams:    uint64(d.MaxIncomingStreams),
		maxStreamTimeout:      time.Second * 30, //nolint:gomnd //...
		rateLimit:             NewNetworkRateLimitParams(),
	}
}

func (p *NetworkParams) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NetworkParams")

	if err := p.BaseParams.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if p.timeoutRequest < 0 {
		return e.Errorf("wrong duration; invalid timeoutRequest")
	}

	if p.handshakeIdleTimeout < 0 {
		return e.Errorf("wrong duration; invalid handshakeIdleTimeout")
	}

	if p.maxIdleTimeout < 0 {
		return e.Errorf("wrong duration; invalid maxIdleTimeout")
	}

	if p.keepAlivePeriod < 0 {
		return e.Errorf("wrong duration; invalid keepAlivePeriod")
	}

	if p.defaultHandlerTimeout < 0 {
		return e.Errorf("wrong duration; invalid defaultHandlerTimeout")
	}

	for i := range p.handlerTimeouts {
		if _, found := networkHandlerPrefixMap[i]; !found {
			return e.Errorf("unknown handler timeout, %q", i)
		}

		if p.handlerTimeouts[i] < 0 {
			return e.Errorf("wrong duration; invalid %q", i)
		}
	}

	if p.connectionPoolSize < 1 {
		return e.Errorf("invalid connectionPoolSize")
	}

	if p.maxIncomingStreams < 1 {
		return e.Errorf("invalid maxIncomingStreams")
	}

	if p.maxStreamTimeout < 0 {
		return e.Errorf("wrong duration; invalid maxStreamTimeout")
	}

	if err := p.rateLimit.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

// TimeoutRequest is the default timeout to request the other nodes; see
// https://pkg.go.dev/github.com/quic-go/quic-go#Config .
func (p *NetworkParams) TimeoutRequest() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.timeoutRequest
}

func (p *NetworkParams) SetTimeoutRequest(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.timeoutRequest == d {
			return false, nil
		}

		p.timeoutRequest = d

		return true, nil
	})
}

// HandshakeIdleTimeout; see https://pkg.go.dev/github.com/quic-go/quic-go#Config .
func (p *NetworkParams) HandshakeIdleTimeout() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.handshakeIdleTimeout
}

func (p *NetworkParams) SetHandshakeIdleTimeout(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.handshakeIdleTimeout == d {
			return false, nil
		}

		p.handshakeIdleTimeout = d

		return true, nil
	})
}

// MaxIdleTimeout; see https://pkg.go.dev/github.com/quic-go/quic-go#Config .
func (p *NetworkParams) MaxIdleTimeout() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.maxIdleTimeout
}

func (p *NetworkParams) SetMaxIdleTimeout(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.maxIdleTimeout == d {
			return false, nil
		}

		p.maxIdleTimeout = d

		return true, nil
	})
}

// KeepAlivePeriod; see https://pkg.go.dev/github.com/quic-go/quic-go#Config .
func (p *NetworkParams) KeepAlivePeriod() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.keepAlivePeriod
}

func (p *NetworkParams) SetKeepAlivePeriod(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.keepAlivePeriod == d {
			return false, nil
		}

		p.keepAlivePeriod = d

		return true, nil
	})
}

// DefaultHandlerTimeout is the default timeout for network handlers. If
// handling request is over timeout, the request will be canceled by server.
func (p *NetworkParams) DefaultHandlerTimeout() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.defaultHandlerTimeout
}

func (p *NetworkParams) SetDefaultHandlerTimeout(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.defaultHandlerTimeout == d {
			return false, nil
		}

		p.defaultHandlerTimeout = d

		return true, nil
	})
}

// HandlerTimeout is the map of timeouts for each handler. If not set in
// HandlerTimeout, DefaultHandlerTimeout will be used.
func (p *NetworkParams) HandlerTimeout(i quicstream.HandlerName) (time.Duration, error) {
	if _, found := networkHandlerPrefixMap[i]; !found {
		return 0, util.ErrNotFound.Errorf("unknown handler timeout, %q", i)
	}

	return p.handlerTimeout(i), nil
}

func (p *NetworkParams) SetHandlerTimeout(i quicstream.HandlerName, d time.Duration) error {
	if _, found := networkHandlerPrefixMap[i]; !found {
		return util.ErrNotFound.Errorf("unknown handler timeout, %q", i)
	}

	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		switch prev, found := p.handlerTimeouts[i]; {
		case found && prev == d:
			return false, nil
		case found && p.defaultHandlerTimeout == d:
			delete(p.handlerTimeouts, i)

			return false, nil
		}

		p.handlerTimeouts[i] = d

		return true, nil
	})
}

func (p *NetworkParams) HandlerTimeoutFunc(i quicstream.HandlerName) (func() time.Duration, error) {
	if _, found := networkHandlerPrefixMap[i]; !found {
		return nil, util.ErrNotFound.Errorf("unknown handler timeout, %q", i)
	}

	return func() time.Duration {
		return p.handlerTimeout(i)
	}, nil
}

func (p *NetworkParams) handlerTimeout(i quicstream.HandlerName) time.Duration {
	p.RLock()
	defer p.RUnlock()

	switch d, found := p.handlerTimeouts[i]; {
	case !found:
		return p.defaultHandlerTimeout
	default:
		return d
	}
}

// ConnectionPoolSize is the sharded map size for connection pool.
func (p *NetworkParams) ConnectionPoolSize() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.connectionPoolSize
}

func (p *NetworkParams) SetConnectionPoolSize(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.connectionPoolSize == d {
			return false, nil
		}

		p.connectionPoolSize = d

		return true, nil
	})
}

// MaxIncomingStreams; see https://pkg.go.dev/github.com/quic-go/quic-go#Config .
func (p *NetworkParams) MaxIncomingStreams() uint64 {
	p.RLock()
	defer p.RUnlock()

	return p.maxIncomingStreams
}

func (p *NetworkParams) SetMaxIncomingStreams(d uint64) error {
	return p.SetUint64(d, func(d uint64) (bool, error) {
		if p.maxIncomingStreams == d {
			return false, nil
		}

		p.maxIncomingStreams = d

		return true, nil
	})
}

// MaxStreamTimeout; see https://pkg.go.dev/github.com/quic-go/quic-go#Config .
func (p *NetworkParams) MaxStreamTimeout() time.Duration {
	p.RLock()
	defer p.RUnlock()

	return p.maxStreamTimeout
}

func (p *NetworkParams) SetMaxStreamTimeout(d time.Duration) error {
	return p.SetDuration(d, func(d time.Duration) (bool, error) {
		if p.maxStreamTimeout == d {
			return false, nil
		}

		p.maxStreamTimeout = d

		return true, nil
	})
}

func (p *NetworkParams) RateLimit() *NetworkRateLimitParams {
	p.RLock()
	defer p.RUnlock()

	return p.rateLimit
}

func (p *NetworkParams) SetRateLimit(r *RateLimiterRules) error {
	p.Lock()
	defer p.Unlock()

	p.rateLimit = &NetworkRateLimitParams{RateLimiterRules: r}

	return nil
}

type NetworkRateLimitParams struct {
	*RateLimiterRules
}

func NewNetworkRateLimitParams() *NetworkRateLimitParams {
	return &NetworkRateLimitParams{
		RateLimiterRules: NewRateLimiterRules(),
	}
}

func (p *NetworkRateLimitParams) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NetworkRateLimitParams")

	if err := p.RateLimiterRules.IsValid(nil); err != nil {
		return e.WithMessage(err, "NetworkRateLimitParams")
	}

	// NOTE check handler string is valid
	checkRateLimitHandler := func(r RateLimiterRuleMap) error {
		for i := range r.m {
			if _, found := networkHandlerPrefixMap[quicstream.HandlerName(i)]; !found {
				return errors.Errorf("unknown network handler prefix, %q", i)
			}
		}

		return nil
	}

	if err := checkRateLimitHandler(p.DefaultRuleMap()); err != nil {
		return e.Wrap(err)
	}

	if rs, ok := p.SuffrageRuleSet().(*SuffrageRateLimiterRuleSet); ok && rs != nil {
		if err := checkRateLimitHandler(rs.rules); err != nil {
			return e.Wrap(err)
		}
	}

	if rs, ok := p.NetRuleSet().(NetRateLimiterRuleSet); ok {
		for i := range rs.rules {
			if err := checkRateLimitHandler(rs.rules[i]); err != nil {
				return e.Wrap(err)
			}
		}
	}

	if rs, ok := p.NodeRuleSet().(NodeRateLimiterRuleSet); ok {
		for i := range rs.rules {
			if err := checkRateLimitHandler(rs.rules[i]); err != nil {
				return e.Wrap(err)
			}
		}
	}

	return nil
}

var networkHandlerNames = []quicstream.HandlerName{
	isaacnetwork.HandlerNameAskHandover,
	isaacnetwork.HandlerNameBlockMap,
	isaacnetwork.HandlerNameBlockItem,
	isaacnetwork.HandlerNameBlockItemFiles,
	isaacnetwork.HandlerNameCancelHandover,
	isaacnetwork.HandlerNameCheckHandover,
	isaacnetwork.HandlerNameCheckHandoverX,
	isaacnetwork.HandlerNameExistsInStateOperation,
	isaacnetwork.HandlerNameHandoverMessage,
	isaacnetwork.HandlerNameLastBlockMap,
	isaacnetwork.HandlerNameLastSuffrageProof,
	isaacnetwork.HandlerNameNodeChallenge,
	isaacnetwork.HandlerNameNodeInfo,
	isaacnetwork.HandlerNameNodeMetrics,
	isaacnetwork.HandlerNameOperation,
	isaacnetwork.HandlerNameProposal,
	isaacnetwork.HandlerNameRequestProposal,
	isaacnetwork.HandlerNameSendBallots,
	isaacnetwork.HandlerNameSendOperation,
	isaacnetwork.HandlerNameSetAllowConsensus,
	isaacnetwork.HandlerNameStartHandover,
	isaacnetwork.HandlerNameState,
	isaacnetwork.HandlerNameStreamOperations,
	isaacnetwork.HandlerNameSuffrageNodeConnInfo,
	isaacnetwork.HandlerNameSuffrageProof,
	isaacnetwork.HandlerNameSyncSourceConnInfo,
	HandlerNameMemberlist,
	HandlerNameMemberlistCallbackBroadcastMessage,
	HandlerNameMemberlistEnsureBroadcastMessage,
	HandlerNameNodeRead,
	HandlerNameNodeWrite,
	HandlerNameEventLogging,
}

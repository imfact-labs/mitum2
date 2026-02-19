package launch

import (
	"reflect"
	"time"

	"github.com/imfact-labs/mitum2/isaac"
	"github.com/imfact-labs/mitum2/network/quicmemberlist"
	"github.com/imfact-labs/mitum2/network/quicstream"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/imfact-labs/mitum2/util/hint"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

func (p *LocalParams) MarshalYAML() (interface{}, error) {
	m := map[string]interface{}{}

	if p.ISAAC != nil {
		switch b, err := util.MarshalJSON(p.ISAAC); {
		case err != nil:
			return nil, err
		default:
			var i map[string]interface{}

			if err := util.UnmarshalJSON(b, &i); err != nil {
				return nil, err
			}

			delete(i, "_hint")

			m["isaac"] = i
		}
	}

	if p.Memberlist != nil {
		m["memberlist"] = p.Memberlist
	}

	if p.MISC != nil {
		m["misc"] = p.MISC
	}

	if p.Network != nil {
		m["network"] = p.Network
	}

	return m, nil
}

type LocalParamsYAMLUnmarshaler struct {
	ISAAC      map[string]interface{}           `yaml:"isaac"`
	Memberlist *quicmemberlist.MemberlistParams `yaml:"memberlist,omitempty"`
	MISC       *isaac.MISCParams                `yaml:"misc,omitempty"`
	Network    *NetworkParams                   `yaml:"network,omitempty"`
}

func (p *LocalParams) DecodeYAML(b []byte, jsonencoder encoder.Encoder) error {
	if len(b) < 1 {
		return nil
	}

	e := util.StringError("decode IsaacParams")

	nb, err := util.ReplaceEnvVariables(b)
	if err != nil {
		return e.Wrap(err)
	}

	u := LocalParamsYAMLUnmarshaler{
		Memberlist: p.Memberlist,
		MISC:       p.MISC,
		Network:    p.Network,
	}

	if err := yaml.Unmarshal(nb, &u); err != nil {
		return e.Wrap(err)
	}

	switch lb, err := jsonencoder.Marshal(u.ISAAC); {
	case err != nil:
		return e.Wrap(err)
	default:
		if err := jsonencoder.Unmarshal(lb, p.ISAAC); err != nil {
			return e.Wrap(err)
		}

		p.ISAAC.BaseHinter = hint.NewBaseHinter(isaac.ParamsHint)
	}

	if u.Memberlist != nil {
		p.Memberlist = u.Memberlist
	}

	if u.MISC != nil {
		p.MISC = u.MISC
	}

	if u.Network != nil {
		p.Network = u.Network
	}

	return nil
}

type networkParamsYAMLMarshaler struct {
	//revive:disable:line-length-limit
	RateLimit             *NetworkRateLimitParams                          `json:"ratelimit,omitempty" yaml:"ratelimit,omitempty"` //nolint:tagliatelle //...
	HandlerTimeout        map[quicstream.HandlerName]util.ReadableDuration `json:"handler_timeout,omitempty" yaml:"handler_timeout,omitempty"`
	TimeoutRequest        util.ReadableDuration                            `json:"timeout_request,omitempty" yaml:"timeout_request,omitempty"`
	HandshakeIdleTimeout  util.ReadableDuration                            `json:"handshake_idle_timeout,omitempty" yaml:"handshake_idle_timeout,omitempty"`
	MaxIdleTimeout        util.ReadableDuration                            `json:"max_idle_timeout,omitempty" yaml:"max_idle_timeout,omitempty"`
	KeepAlivePeriod       util.ReadableDuration                            `json:"keep_alive_period,omitempty" yaml:"keep_alive_period,omitempty"`
	DefaultHandlerTimeout util.ReadableDuration                            `json:"default_handler_timeout,omitempty" yaml:"default_handler_timeout,omitempty"`
	ConnectionPoolSize    uint64                                           `json:"connection_pool_size,omitempty" yaml:"connection_pool_size,omitempty"`
	MaxIncomingStreams    uint64                                           `json:"max_incoming_streams,omitempty" yaml:"max_incoming_streams,omitempty"`
	MaxStreamTimeout      util.ReadableDuration                            `json:"max_stream_timeout,omitempty" yaml:"max_stream_timeout,omitempty"`
	//revive:enable:line-length-limit
}

func (p *NetworkParams) marshaler() networkParamsYAMLMarshaler {
	handlerTimeouts := map[quicstream.HandlerName]util.ReadableDuration{}

	for i := range p.handlerTimeouts {
		v := p.handlerTimeouts[i]

		// NOTE skip default value
		if d, found := defaultHandlerTimeouts[i]; found && v == d {
			continue
		}

		if v == p.defaultHandlerTimeout {
			continue
		}

		handlerTimeouts[i] = util.ReadableDuration(v)
	}

	return networkParamsYAMLMarshaler{
		TimeoutRequest:        util.ReadableDuration(p.timeoutRequest),
		HandshakeIdleTimeout:  util.ReadableDuration(p.handshakeIdleTimeout),
		MaxIdleTimeout:        util.ReadableDuration(p.maxIdleTimeout),
		KeepAlivePeriod:       util.ReadableDuration(p.keepAlivePeriod),
		DefaultHandlerTimeout: util.ReadableDuration(p.defaultHandlerTimeout),
		HandlerTimeout:        handlerTimeouts,
		ConnectionPoolSize:    p.connectionPoolSize,
		MaxIncomingStreams:    p.maxIncomingStreams,
		MaxStreamTimeout:      util.ReadableDuration(p.maxStreamTimeout),
		RateLimit:             p.rateLimit,
	}
}

func (p *NetworkParams) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(p.marshaler())
}

func (p *NetworkParams) MarshalYAML() (interface{}, error) {
	return p.marshaler(), nil
}

type networkParamsYAMLUnmarshaler struct {
	//revive:disable:line-length-limit
	TimeoutRequest        *util.ReadableDuration                           `json:"timeout_request,omitempty" yaml:"timeout_request,omitempty"`
	HandshakeIdleTimeout  *util.ReadableDuration                           `json:"handshake_idle_timeout,omitempty" yaml:"handshake_idle_timeout,omitempty"`
	MaxIdleTimeout        *util.ReadableDuration                           `json:"max_idle_timeout,omitempty" yaml:"max_idle_timeout,omitempty"`
	KeepAlivePeriod       *util.ReadableDuration                           `json:"keep_alive_period,omitempty" yaml:"keep_alive_period,omitempty"`
	DefaultHandlerTimeout *util.ReadableDuration                           `json:"default_handler_timeout,omitempty" yaml:"default_handler_timeout,omitempty"`
	HandlerTimeout        map[quicstream.HandlerName]util.ReadableDuration `json:"handler_timeout,omitempty" yaml:"handler_timeout,omitempty"`
	ConnectionPoolSize    *uint64                                          `json:"connection_pool_size,omitempty" yaml:"connection_pool_size,omitempty"`
	MaxIncomingStreams    *uint64                                          `json:"max_incoming_streams,omitempty" yaml:"max_incoming_streams,omitempty"`
	MaxStreamTimeout      *util.ReadableDuration                           `json:"max_stream_timeout,omitempty" yaml:"max_stream_timeout,omitempty"`
	RateLimit             *NetworkRateLimitParams                          `json:"ratelimit,omitempty" yaml:"ratelimit,omitempty"` //nolint:tagliatelle //...
	//revive:enable:line-length-limit
}

func (p *NetworkParams) UnmarshalJSON(b []byte) error {
	d := defaultNetworkParams()
	*p = *d

	e := util.StringError("decode NetworkParams")

	var u networkParamsYAMLUnmarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *NetworkParams) UnmarshalYAML(y *yaml.Node) error {
	d := defaultNetworkParams()
	*p = *d

	e := util.StringError("decode NetworkParams")

	u := networkParamsYAMLUnmarshaler{
		RateLimit: p.rateLimit,
	}

	if err := y.Decode(&u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *NetworkParams) unmarshal(u networkParamsYAMLUnmarshaler) error {
	durargs := [][2]interface{}{
		{u.TimeoutRequest, &p.timeoutRequest},
		{u.HandshakeIdleTimeout, &p.handshakeIdleTimeout},
		{u.MaxIdleTimeout, &p.maxIdleTimeout},
		{u.KeepAlivePeriod, &p.keepAlivePeriod},
		{u.DefaultHandlerTimeout, &p.defaultHandlerTimeout},
		{u.MaxStreamTimeout, &p.maxStreamTimeout},
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

	for i := range u.HandlerTimeout {
		p.handlerTimeouts[i] = time.Duration(u.HandlerTimeout[i])
	}

	if u.ConnectionPoolSize != nil {
		p.connectionPoolSize = *u.ConnectionPoolSize
	}

	if u.MaxIncomingStreams != nil {
		p.maxIncomingStreams = *u.MaxIncomingStreams
	}

	if u.RateLimit != nil {
		p.rateLimit = u.RateLimit
	}

	return nil
}

type networkRateLimitParamsMarshaler struct {
	Suffrage RateLimiterRuleSet `json:"suffrage,omitempty" yaml:"suffrage,omitempty"`
	Node     RateLimiterRuleSet `json:"node,omitempty" yaml:"node,omitempty"`
	Net      RateLimiterRuleSet `json:"net,omitempty" yaml:"net,omitempty"`
	Default  RateLimiterRuleMap `json:"default,omitempty" yaml:"default,omitempty"`
}

func (p *NetworkRateLimitParams) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(networkRateLimitParamsMarshaler{
		Suffrage: p.SuffrageRuleSet(),
		Node:     p.NodeRuleSet(),
		Net:      p.NetRuleSet(),
		Default:  p.DefaultRuleMap(),
	})
}

func (p *NetworkRateLimitParams) MarshalYAML() (interface{}, error) {
	switch b, err := p.MarshalJSON(); {
	case err != nil:
		return nil, err
	default:
		var u map[string]interface{}
		if err := yaml.Unmarshal(b, &u); err != nil {
			return nil, errors.WithStack(err)
		}

		return u, nil
	}
}

type NetworkRateLimitParamsUnmarshaler struct {
	Suffrage *SuffrageRateLimiterRuleSet `json:"suffrage,omitempty" yaml:"suffrage,omitempty"`
	Node     *NodeRateLimiterRuleSet     `json:"node,omitempty" yaml:"node,omitempty"`
	Net      *NetRateLimiterRuleSet      `json:"net,omitempty" yaml:"net,omitempty"`
	Default  *RateLimiterRuleMap         `json:"default,omitempty" yaml:"default,omitempty"`
}

func (p *NetworkRateLimitParams) unmarshal(u NetworkRateLimitParamsUnmarshaler) error {
	if p.RateLimiterRules == nil {
		p.RateLimiterRules = &RateLimiterRules{}
	}

	if u.Suffrage != nil {
		if err := p.SetSuffrageRuleSet(u.Suffrage); err != nil {
			return err
		}
	}

	if u.Node != nil {
		if err := p.SetNodeRuleSet(*u.Node); err != nil {
			return err
		}
	}

	if u.Net != nil {
		if err := p.SetNetRuleSet(*u.Net); err != nil {
			return err
		}
	}

	if u.Default != nil && !u.Default.IsEmpty() {
		if err := p.SetDefaultRuleMap(*u.Default); err != nil {
			return err
		}
	}

	return nil
}

func (p *NetworkRateLimitParams) UnmarshalJSON(b []byte) error {
	e := util.StringError("decode NetworkRateLimitParams")

	var u NetworkRateLimitParamsUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(p.unmarshal(u))
}

func (p *NetworkRateLimitParams) UnmarshalYAML(y *yaml.Node) error {
	e := util.StringError("decode NetworkRateLimitParams")

	var u map[string]interface{}
	if err := y.Decode(&u); err != nil {
		return e.Wrap(err)
	}

	defer clear(u)

	switch b, err := util.MarshalJSON(u); {
	case err != nil:
		return e.Wrap(err)
	default:
		return p.UnmarshalJSON(b)
	}
}

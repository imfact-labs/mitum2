package quicstream

import (
	"net"

	nutil "github.com/imfact-labs/mitum2/network/util"
	"github.com/imfact-labs/mitum2/util"
	"github.com/pkg/errors"
)

type ConnInfo struct {
	addr        *net.UDPAddr
	tlsInsecure bool
}

func UnsafeConnInfo(addr *net.UDPAddr, tlsInsecure bool) ConnInfo {
	return ConnInfo{addr: addr, tlsInsecure: tlsInsecure}
}

func NewConnInfo(addr *net.UDPAddr, tlsInsecure bool) (ConnInfo, error) {
	ci := UnsafeConnInfo(addr, tlsInsecure)

	return ci, ci.IsValid(nil)
}

func MustConnInfo(addr *net.UDPAddr, tlsInsecure bool) ConnInfo {
	ci, err := NewConnInfo(addr, tlsInsecure)
	if err != nil {
		panic(err)
	}

	return ci
}

func NewConnInfoFromFullString(s string) (ConnInfo, error) {
	as, tlsInsecure := nutil.ParseTLSInsecure(s)

	return NewConnInfoFromStringAddr(as, tlsInsecure)
}

func MustNewConnInfoFromFullString(s string) ConnInfo {
	as, tlsInsecure := nutil.ParseTLSInsecure(s)

	ci, err := NewConnInfoFromStringAddr(as, tlsInsecure)
	if err != nil {
		panic(err)
	}

	return ci
}

func NewConnInfoFromStringAddr(s string, tlsInsecure bool) (ci ConnInfo, _ error) {
	addr, err := net.ResolveUDPAddr("udp", s)
	if err == nil {
		return NewConnInfo(addr, tlsInsecure)
	}

	var dnserr *net.DNSError

	if errors.As(err, &dnserr) {
		return ci, errors.Wrap(err, "parse ConnInfo")
	}

	return ci, util.ErrInvalid.WithMessage(err, "parse ConnInfo")
}

func (c ConnInfo) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid ConnInfo")

	switch {
	case c.addr == nil:
		return e.Errorf("empty addr")
	case len(c.addr.IP) < 1, c.addr.IP.IsUnspecified():
		return e.Errorf("empty addr ip")
	case c.addr.Port < 1:
		return e.Errorf("empty addr port")
	}

	return nil
}

func (c ConnInfo) Addr() net.Addr {
	return c.addr
}

func (c ConnInfo) TLSInsecure() bool {
	return c.tlsInsecure
}

func (c ConnInfo) String() string {
	var addr string
	if c.addr != nil {
		addr = c.addr.String()
	}

	return nutil.ConnInfoToString(addr, c.tlsInsecure)
}

func (c ConnInfo) UDPAddr() *net.UDPAddr {
	return c.addr
}

func (c ConnInfo) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

func (c *ConnInfo) UnmarshalText(b []byte) error {
	ci, err := NewConnInfoFromFullString(string(b))
	if err != nil {
		return errors.WithMessage(err, "unmarshal ConnInfo")
	}

	*c = ci

	return nil
}

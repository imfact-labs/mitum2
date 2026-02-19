package network

import (
	"fmt"
	"net"

	"github.com/imfact-labs/mitum2/util"
	"github.com/rs/zerolog"
)

type ConnInfo interface {
	fmt.Stringer
	util.IsValider
	Addr() net.Addr
	TLSInsecure() bool
}

func EqualConnInfo(a, b ConnInfo) bool {
	switch {
	case a == nil, b == nil:
		return false
	case a.Addr() == nil, b.Addr() == nil:
		return false
	case a.Addr().String() != b.Addr().String():
		return false
	default:
		return true
	}
}

func DeepEqualConnInfo(a, b ConnInfo) bool {
	switch {
	case !EqualConnInfo(a, b):
		return false
	case a.String() != b.String():
		return false
	default:
		return true
	}
}

func ConnInfoLog(ci ConnInfo) *zerolog.Event {
	return zerolog.Dict().
		Str("type", fmt.Sprintf("%T", ci)).
		Stringer("addr", ci.Addr()).
		Bool("tls_insecure", ci.TLSInsecure())
}

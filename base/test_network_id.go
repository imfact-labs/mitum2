//go:build test
// +build test

package base

import "github.com/imfact-labs/mitum2/util"

func RandomNetworkID() NetworkID {
	return NetworkID(util.UUID().Bytes())
}

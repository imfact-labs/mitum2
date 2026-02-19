//go:build test
// +build test

package base

import "github.com/imfact-labs/mitum2/util"

func RandomAddress(prefix string) Address {
	return NewStringAddress(prefix + util.UUID().String())
}

func SimpleAddress(name string) Address {
	if len(name) < 3 {
		name = name + util.UUID().String()
	}

	return NewStringAddress(name)
}

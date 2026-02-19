package base

import "github.com/imfact-labs/mitum2/util"

var objcache util.GCache[string, any]

func init() {
	objcache = util.NewLRUGCache[string, any](1 << 13) //nolint:gomnd //...
}

func SetObjCache(c util.GCache[string, any]) {
	objcache = c
}

package launch

import (
	"context"

	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	jsonenc "github.com/imfact-labs/mitum2/util/encoder/json"
	"github.com/imfact-labs/mitum2/util/ps"
)

var (
	PNameEncoder       = ps.Name("encoder")
	PNameAddHinters    = ps.Name("add-hinters")
	EncodersContextKey = util.ContextKey("encoders")
)

func PEncoder(pctx context.Context) (context.Context, error) {
	enc := jsonenc.NewEncoder()
	encs := encoder.NewEncoders(enc, enc)

	return context.WithValue(pctx, EncodersContextKey, encs), nil
}

func PAddHinters(pctx context.Context) (context.Context, error) {
	e := util.StringError("add hinters")

	var encs *encoder.Encoders
	if err := util.LoadFromContextOK(pctx, EncodersContextKey, &encs); err != nil {
		return pctx, e.Wrap(err)
	}

	if err := LoadHinters(encs); err != nil {
		return pctx, e.Wrap(err)
	}

	return pctx, nil
}

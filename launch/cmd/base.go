package launchcmd

import (
	"context"

	"github.com/imfact-labs/mitum2/launch"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/imfact-labs/mitum2/util/logging"
	"github.com/imfact-labs/mitum2/util/ps"
	"github.com/rs/zerolog"
)

type BaseCommand struct {
	Encoders    *encoder.Encoders `kong:"-"`
	JSONEncoder encoder.Encoder   `kong:"-"`
	Log         *zerolog.Logger   `kong:"-"`
}

func (cmd *BaseCommand) prepare(pctx context.Context) (context.Context, error) {
	pps := ps.NewPS("cmd")

	_ = pps.
		AddOK(launch.PNameEncoder, launch.PEncoder, nil)

	_ = pps.POK(launch.PNameEncoder).
		PostAddOK(launch.PNameAddHinters, launch.PAddHinters)

	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		return pctx, err
	}

	cmd.Log = log.Log()

	nctx, err := pps.Run(pctx)
	if err != nil {
		return nctx, err
	}

	if err := util.LoadFromContextOK(nctx, launch.EncodersContextKey, &cmd.Encoders); err != nil {
		return nctx, err
	}

	cmd.JSONEncoder = cmd.Encoders.JSON()

	return nctx, nil
}

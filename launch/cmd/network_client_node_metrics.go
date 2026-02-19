package launchcmd

import (
	"context"
	"os"

	isaacnetwork "github.com/imfact-labs/mitum2/isaac/network"
	quicstreamheader "github.com/imfact-labs/mitum2/network/quicstream/header"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/pkg/errors"
)

type NetworkClientNodeMetricsCommand struct { //nolint:govet //...
	BaseNetworkClientCommand
	Interval string `arg:"" name:"interval" default:"1m" help:"metrics interval window (e.g. 1s, 1m, 5m)"`
}

func (cmd *NetworkClientNodeMetricsCommand) Run(pctx context.Context) error {
	if err := cmd.Prepare(pctx); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(pctx, cmd.Timeout)
	defer cancel()

	stream, _, err := cmd.Client.Dial(ctx, cmd.Remote.ConnInfo())
	if err != nil {
		return err
	}

	defer func() {
		_ = cmd.Client.Close()
	}()

	header := isaacnetwork.NewNodeMetricsRequestHeader(cmd.Interval)
	header.SetClientID(cmd.ClientID)

	return stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		var enc encoder.Encoder

		switch renc, rh, err := broker.ReadResponseHead(ctx); {
		case err != nil:
			return err
		case rh.Err() != nil:
			return rh.Err()
		case !rh.OK():
			return errors.Errorf("not ok")
		default:
			enc = renc
		}

		switch bodyType, bodyLength, r, err := broker.ReadBodyErr(ctx); {
		case err != nil:
			return err
		case bodyType == quicstreamheader.EmptyBodyType,
			bodyType == quicstreamheader.FixedLengthBodyType && bodyLength < 1:
			return errors.Errorf("empty body")
		default:
			var v interface{}

			if err := enc.StreamDecoder(r).Decode(&v); err != nil {
				return err
			}

			return cmd.Print(v, os.Stdout)
		}
	})
}

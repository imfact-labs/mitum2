package launch

import (
	"context"

	"github.com/imfact-labs/mitum2/isaac"
	isaacblock "github.com/imfact-labs/mitum2/isaac/block"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/imfact-labs/mitum2/util/logging"
	"github.com/imfact-labs/mitum2/util/ps"
)

var (
	PNameBlockItemReadersDecompressFunc      = ps.Name("block-item-readers-decompress-func")
	PNameBlockItemReaders                    = ps.Name("block-item-readers")
	PNameRemotesBlockItemReaderFunc          = ps.Name("remotes-block-item-reader-func")
	BlockItemReadersDecompressFuncContextKey = util.ContextKey("block-item-readers-decompress-func")
	NewBlockItemReadersFuncContextKey        = util.ContextKey("new-block-item-readers-func")
	BlockItemReadersContextKey               = util.ContextKey("block-item-readers")
	RemotesBlockItemReaderFuncContextKey     = util.ContextKey("remotes-block-item-reader-func")
)

func PBlockItemReaders(pctx context.Context) (context.Context, error) {
	var log *logging.Logging
	var encs *encoder.Encoders
	var decompress util.DecompressReaderFunc

	if err := util.LoadFromContextOK(pctx,
		LoggingContextKey, &log,
		EncodersContextKey, &encs,
		BlockItemReadersDecompressFuncContextKey, &decompress,
	); err != nil {
		return pctx, err
	}

	newReaders := func(
		pctx context.Context, root string, args *isaac.BlockItemReadersArgs,
	) (*isaac.BlockItemReaders, error) {
		var log *logging.Logging
		var encs *encoder.Encoders

		if err := util.LoadFromContextOK(pctx,
			LoggingContextKey, &log,
			EncodersContextKey, &encs,
		); err != nil {
			return nil, err
		}

		if args == nil {
			var decompress util.DecompressReaderFunc

			if err := util.LoadFromContextOK(pctx,
				BlockItemReadersDecompressFuncContextKey, &decompress,
			); err != nil {
				return nil, err
			}

			args = isaac.NewBlockItemReadersArgs()
			args.DecompressReaderFunc = decompress
		}

		readers := isaac.NewBlockItemReaders(root, encs, args)
		_ = readers.Add(isaacblock.LocalFSWriterHint, isaacblock.NewDefaultItemReaderFunc(1<<6)) //nolint:gomnd //...

		_ = readers.SetLogging(log)

		return readers, nil
	}

	return context.WithValue(pctx, NewBlockItemReadersFuncContextKey, newReaders), nil
}

func PBlockItemReadersDecompressFunc(pctx context.Context) (context.Context, error) {
	return context.WithValue(pctx,
		BlockItemReadersDecompressFuncContextKey,
		util.DecompressReaderFunc(util.DefaultDecompressReaderFunc),
	), nil
}

func PRemotesBlockItemReaderFunc(pctx context.Context) (context.Context, error) {
	return context.WithValue(pctx,
		RemotesBlockItemReaderFuncContextKey,
		isaac.NewDefaultRemotesBlockItemReadFunc(),
	), nil
}

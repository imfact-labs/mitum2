package isaac

import (
	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/util/hint"
)

type OperationProcessHandler interface {
	Hints() []hint.Hint
	PreProcess(operation base.Operation) (bool, error)
	Process(operation base.Operation) ([]base.State, error)
}

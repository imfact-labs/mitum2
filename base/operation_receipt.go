package base

import (
	"encoding/json"

	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/imfact-labs/mitum2/util/hint"
	"github.com/imfact-labs/mitum2/util/valuehash"
)

var (
	BaseOperationReceiptHint   = hint.MustNewHint("operation-receipt-v0.0.1")
	OperationReceiptRecordHint = hint.MustNewHint("operation-receipt-record-v0.0.1")
)

type OperationReceipt interface {
	hint.Hinter
	util.IsValider
}

// OperationReceiptProvider optionally exposes the latest receipt produced while
// processing an operation. Implementers should clear stale values themselves
// when an operation does not emit a receipt.
type OperationReceiptProvider interface {
	OperationReceipt() OperationReceipt
}

type BaseOperationReceipt struct {
	hint.BaseHinter
}

func NewBaseOperationReceipt() BaseOperationReceipt {
	return BaseOperationReceipt{
		BaseHinter: hint.NewBaseHinter(BaseOperationReceiptHint),
	}
}

func (r BaseOperationReceipt) IsValid([]byte) error {
	return r.BaseHinter.IsValid(BaseOperationReceiptHint.Type().Bytes())
}

func (r BaseOperationReceipt) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		hint.BaseHinter
	}{
		BaseHinter: r.BaseHinter,
	})
}

func (r *BaseOperationReceipt) DecodeJSON([]byte, encoder.Encoder) error {
	r.BaseHinter = hint.NewBaseHinter(BaseOperationReceiptHint)

	return nil
}

type OperationReceiptRecord struct {
	operation util.Hash
	facthash  util.Hash
	receipt   OperationReceipt
	hint.BaseHinter
}

func NewOperationReceiptRecord(
	operation, facthash util.Hash,
	receipt OperationReceipt,
) OperationReceiptRecord {
	return OperationReceiptRecord{
		BaseHinter: hint.NewBaseHinter(OperationReceiptRecordHint),
		operation:  operation,
		facthash:   facthash,
		receipt:    receipt,
	}
}

func (r OperationReceiptRecord) IsValid([]byte) error {
	if err := r.BaseHinter.IsValid(OperationReceiptRecordHint.Type().Bytes()); err != nil {
		return err
	}

	if err := util.CheckIsValiders(nil, false, r.operation, r.facthash); err != nil {
		return err
	}

	if r.receipt != nil {
		if err := r.receipt.IsValid(nil); err != nil {
			return err
		}
	}

	return nil
}

func (r OperationReceiptRecord) OperationHash() util.Hash {
	return r.operation
}

func (r OperationReceiptRecord) FactHash() util.Hash {
	return r.facthash
}

func (r OperationReceiptRecord) Receipt() OperationReceipt {
	return r.receipt
}

func (r OperationReceiptRecord) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		Operation util.Hash        `json:"operation"`
		FactHash  util.Hash        `json:"fact_hash"`
		Receipt   OperationReceipt `json:"receipt,omitempty"`
		hint.BaseHinter
	}{
		BaseHinter: r.BaseHinter,
		Operation:  r.operation,
		FactHash:   r.facthash,
		Receipt:    r.receipt,
	})
}

type operationReceiptRecordJSONUnmarshaler struct {
	Operation valuehash.HashDecoder `json:"operation"`
	FactHash  valuehash.HashDecoder `json:"fact_hash"`
	Receipt   json.RawMessage       `json:"receipt"`
}

func (r *OperationReceiptRecord) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u operationReceiptRecordJSONUnmarshaler

	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	r.BaseHinter = hint.NewBaseHinter(OperationReceiptRecordHint)
	r.operation = u.Operation.Hash()
	r.facthash = u.FactHash.Hash()

	switch {
	case len(u.Receipt) < 1:
		r.receipt = nil
	case string(u.Receipt) == "null":
		r.receipt = nil
	default:
		if err := encoder.Decode(enc, u.Receipt, &r.receipt); err != nil {
			return err
		}
	}

	return nil
}

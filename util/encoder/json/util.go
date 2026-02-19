package jsonenc

import "github.com/imfact-labs/mitum2/util/encoder"

type Decodable interface {
	DecodeJSON([]byte, encoder.Encoder) error
}

func init() {
	if _, err := encoder.AddEncodersExtension(JSONEncoderHint.Type(), "json"); err != nil {
		panic(err)
	}
}

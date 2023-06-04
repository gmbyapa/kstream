package encoding

type NoopEncoder struct{}

func (_ NoopEncoder) Encode(_ interface{}) ([]byte, error) {
	return nil, nil
}

func (_ NoopEncoder) Decode(_ []byte) (interface{}, error) {
	return nil, nil
}

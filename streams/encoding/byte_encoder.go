package encoding

type ByteEncoder struct{}

func (b ByteEncoder) Encode(v interface{}) ([]byte, error) {
	return v.([]byte), nil
}

func (b ByteEncoder) Decode(data []byte) (interface{}, error) {
	return data, nil
}

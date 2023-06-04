package encoding

import (
	"fmt"
	"github.com/gmbyapa/kstream/pkg/errors"
	"reflect"
)

type StringEncoder struct{}

func (b StringEncoder) Encode(v interface{}) ([]byte, error) {
	str, ok := v.(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf(`data is [%s] not a string`, reflect.TypeOf(v)))
	}

	return []byte(str), nil
}

func (b StringEncoder) Decode(data []byte) (interface{}, error) {
	return string(data), nil
}

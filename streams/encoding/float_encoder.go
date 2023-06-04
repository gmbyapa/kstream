package encoding

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/gmbyapa/kstream/pkg/errors"
)

type FloatEncoder struct{}

func (b FloatEncoder) Encode(v interface{}) ([]byte, error) {
	switch f := v.(type) {
	case float32, float64:
		return []byte(fmt.Sprint(f)), nil
	default:
		return nil, errors.Errorf(`incorrect type expected (float32, float64) have (%s)`, reflect.TypeOf(v))
	}
}

func (b FloatEncoder) Decode(data []byte) (interface{}, error) {
	i, err := strconv.ParseFloat(string(data), 64)
	if err != nil {
		return nil, errors.Wrap(err, `invalid integer`)
	}

	return i, nil
}

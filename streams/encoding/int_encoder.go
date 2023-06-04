package encoding

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/gmbyapa/kstream/pkg/errors"
)

type IntEncoder struct{}

func (b IntEncoder) Encode(data interface{}) ([]byte, error) {
	if i, ok := data.(int); ok {
		return []byte(fmt.Sprint(i)), nil
	}

	return nil, errors.Errorf(`incorrect type expected (int) have (%s)`, reflect.TypeOf(data))
}

func (b IntEncoder) Decode(data []byte) (interface{}, error) {
	i, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, errors.Wrap(err, `invalid integer`)
	}

	return i, nil
}

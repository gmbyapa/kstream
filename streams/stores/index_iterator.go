package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/pkg/errors"
)

type indexIterator struct {
	store         ReadOnlyStore
	idx           Index
	indexIterator backend.Iterator
}

func (i *indexIterator) SeekToFirst() {
	i.indexIterator.SeekToFirst()
}

func (i *indexIterator) Next() {
	i.indexIterator.Next()
}

func (i *indexIterator) Close() {
	i.indexIterator.Close()
}

func (i *indexIterator) Key() (interface{}, error) {
	return i.indexIterator.Key(), nil
}

func (i *indexIterator) Value() (interface{}, error) {
	key, err := i.store.KeyEncoder().Decode(i.indexIterator.Value())
	if err != nil {
		return nil, errors.Wrapf(err, `key decode error, index: %s`, i.idx)
	}

	value, err := i.store.Get(context.Background(), key)
	if err != nil {
		return nil, errors.Wrapf(err, `value get failed, store: %s`, i.store)
	}

	return value, nil
}

func (i *indexIterator) Valid() bool {
	return i.indexIterator.Valid()
}

func (i *indexIterator) Error() error {
	return i.indexIterator.Error()
}

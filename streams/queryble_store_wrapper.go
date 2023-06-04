package streams

import (
	"context"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
)

type QueryableStoreWrapper interface {
	Instances() []topology.LoggableStateStore
	stores.ReadOnlyStore
}

type LocalQueryableStoreWrapper struct {
	storeBuilder topology.LoggableStoreBuilder
	taskManager  tasks.TaskManager
}

func (l *LocalQueryableStoreWrapper) Name() string {
	return l.storeBuilder.Name()
}

func (l *LocalQueryableStoreWrapper) KeyEncoder() encoding.Encoder {
	return l.storeBuilder.KeyEncoder()
}

func (l *LocalQueryableStoreWrapper) ValEncoder() encoding.Encoder {
	return l.storeBuilder.ValEncoder()
}

func (l *LocalQueryableStoreWrapper) String() string {
	return l.storeBuilder.Name()
}

func (l *LocalQueryableStoreWrapper) Instances() []topology.StateStore {
	return l.taskManager.StoreInstances(l.Name())
}

func (l *LocalQueryableStoreWrapper) Get(ctx context.Context, key interface{}) (interface{}, error) {
	// search in all the stores for the key
	for _, stor := range l.Instances() {
		v, err := stor.Get(ctx, key)
		if err != nil {
			return nil, err // TODO wrap error
		}

		if v != nil {
			return v, nil
		}
	}

	return nil, nil
}

func (l *LocalQueryableStoreWrapper) Close() error { return nil }

func (l *LocalQueryableStoreWrapper) Iterator(ctx context.Context) (stores.Iterator, error) {
	return l.iterator(ctx, nil, nil)
}

func (l *LocalQueryableStoreWrapper) PrefixedIterator(ctx context.Context, keyPrefix interface{}, prefixEncoder encoding.Encoder) (stores.Iterator, error) {
	return l.iterator(ctx, keyPrefix, prefixEncoder)
}

func (l *LocalQueryableStoreWrapper) iterator(ctx context.Context, prefix interface{}, prefixEncoder encoding.Encoder) (stores.Iterator, error) {
	var iterators []stores.Iterator
	for _, stor := range l.Instances() {
		if prefix != nil {
			itr, err := stor.PrefixedIterator(ctx, prefix, prefixEncoder)
			if err != nil {
				return nil, err // TODO wrap error
			}
			iterators = append(iterators, itr)
		} else {
			itr, err := stor.Iterator(ctx)
			if err != nil {
				return nil, err // TODO wrap error
			}
			iterators = append(iterators, itr)
		}

	}

	return &MultiStoreIterator{
		iterators: iterators,
	}, nil
}

type MultiStoreIterator struct {
	current   int
	iterators []stores.Iterator
}

func (i *MultiStoreIterator) SeekToFirst() {
	i.iterators[0].SeekToFirst()
}

func (i *MultiStoreIterator) Next() {
	if !i.iterators[i.current].Valid() {
		i.current++
		i.iterators[i.current].SeekToFirst()
	}

	i.iterators[i.current].Next()
}

func (i *MultiStoreIterator) Close() {
	for _, i := range i.iterators {
		i.Close()
	}
}

func (i *MultiStoreIterator) Key() (interface{}, error) {
	return i.iterators[i.current].Key()
}

func (i *MultiStoreIterator) Value() (interface{}, error) {
	return i.iterators[i.current].Value()
}

func (i *MultiStoreIterator) Valid() bool {
	if i.iterators[i.current].Valid() {
		return true
	}

	// when current iterator is no longer is valid and more iterators exists switch to a secondary one
	if i.current < len(i.iterators)-1 {
		i.current++
		return i.Valid()
	}

	return false
}

func (i *MultiStoreIterator) Error() error {
	return i.iterators[i.current].Error()
}

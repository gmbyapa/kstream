package stores

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/v2/backend"
	"github.com/gmbyapa/kstream/v2/pkg/errors"
	"github.com/gmbyapa/kstream/v2/streams/encoding"

	"sync"
	"time"
)

type Index interface {
	sync.Locker
	String() string
	Write(key, value interface{}) error
	KeyPrefixer(key, value interface{}) (hash string)
	Delete(key, value interface{}) error
	Read(index string) (backend.Iterator, error)
	Values(key string) ([]string, error)
	Keys() ([]string, error)
	Compare(key interface{}, valBefore, valAfter interface{}) (bool, error)
	KeyIndexed(index string, key interface{}) (bool, error)
	Backend() backend.Backend
	Close() error
}

type IndexedStore interface {
	Store
	GetIndex(ctx context.Context, name string) (Index, error)
	Indexes() []Index
	GetIndexedRecords(ctx context.Context, indexName, key string) (Iterator, error)
	RebuildIndexes() error
}

type indexedStore struct {
	Store
	indexes map[string]Index

	mu *sync.Mutex
}

func NewIndexedStore(name string, keyEncoder, valEncoder encoding.Encoder, indexes []IndexBuilder, options ...Option) (IndexedStore, error) {
	str, err := NewStore(name, keyEncoder, valEncoder, options...)
	if err != nil {
		return nil, err
	}

	// Build indexes
	idxs := make(map[string]Index)
	for _, builder := range indexes {
		var bkBuilder backend.Builder
		switch v := str.(type) {
		case *store:
			bkBuilder = v.opts.backendBuilder
		}

		if _, ok := idxs[builder.Name()]; ok {
			return nil, errors.Errorf(`index(%s) already exists`, builder.Name())
		}

		idx, err := builder.Build(str, bkBuilder, str.KeyEncoder())
		if err != nil {
			return nil, err
		}

		idxs[idx.String()] = idx
	}

	return &indexedStore{
		Store:   str,
		indexes: idxs,
		mu:      new(sync.Mutex),
	}, nil
}

func (i *indexedStore) Set(ctx context.Context, key, val interface{}, expiry time.Duration) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if err := UpdateIndexes(ctx, i, key, val); err != nil {
		return errors.Wrapf(err, `store %s indexes update failed`, i.Name())
	}

	return i.Store.Set(ctx, key, val, expiry)
}

func (i *indexedStore) Delete(ctx context.Context, key interface{}) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if err := DeleteIndexes(ctx, i, key); err != nil {
		return errors.Wrapf(err, `store %s indexes delete failed`, i.Name())
	}

	return i.Store.Delete(ctx, key)
}

func (i *indexedStore) GetIndex(_ context.Context, name string) (Index, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	idx, ok := i.indexes[name]
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, name)
	}

	return idx, nil
}

func (i *indexedStore) Indexes() []Index {
	var idxs []Index
	for _, idx := range i.indexes {
		idxs = append(idxs, idx)
	}

	return idxs
}

func (i *indexedStore) GetIndexedRecords(ctx context.Context, indexName, key string) (Iterator, error) {
	i.mu.Lock()
	idx, ok := i.indexes[indexName]
	i.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf(`index [%s] does not exist`, indexName)
	}

	idxBkItr, err := idx.Read(key)
	if err != nil {
		return nil, err
	}

	itr := &indexIterator{
		store:         i,
		idx:           idx,
		indexIterator: idxBkItr,
	}

	return itr, nil
}

func (i *indexedStore) RebuildIndexes() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Delete existing index values
	for _, idx := range i.Indexes() {
		if err := idx.Backend().DeleteAll(); err != nil {
			return err
		}
	}

	itr, err := i.Iterator(nil)
	if err != nil {
		return errors.Wrapf(err, `rebuild indexes failed. store iterator failed`)
	}

	defer itr.Close()

	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		key, err := itr.Key()
		if err != nil {
			return errors.Wrapf(err, `rebuild indexes failed. itr key failed`)
		}

		val, err := itr.Value()
		if err != nil {
			return errors.Wrapf(err, `rebuild indexes failed. itr value failed`)
		}

		if err := UpdateIndexes(nil, i, key, val); err != nil {
			return errors.Wrapf(err, `rebuild indexes failed. cannot update indexes`)
		}
	}

	return nil
}

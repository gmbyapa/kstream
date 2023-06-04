package stores

import (
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/encoding"
	"sync"
)

type KeyMapper func(key, val interface{}) (idx string)

type IndexBuilder interface {
	Build(store ReadOnlyStore, backend backend.Builder, keyEncoder encoding.Encoder) (Index, error)
}

type IdxBuilder struct {
	name   string
	mapper KeyMapper
}

func (idb *IdxBuilder) Build(store ReadOnlyStore, backend backend.Builder, keyEncoder encoding.Encoder) (Index, error) {
	idx := &index{
		mapper:     idb.mapper,
		name:       idb.name,
		keyEncoder: keyEncoder,
		mu:         sync.Mutex{},
	}

	bk, err := backend(fmt.Sprintf(`%s-idx-%s`, store.Name(), idx.name))
	if err != nil {
		return nil, errors.Wrapf(err, `index create failed, store:%s, index:%s`, store, idx.name)
	}

	idx.backend = bk

	return idx, nil
}

type index struct {
	mapper     KeyMapper
	name       string
	backend    backend.Backend
	keyEncoder encoding.Encoder
	mu         sync.Mutex
}

func NewIndex(name string, mapper KeyMapper) *IdxBuilder {
	idb := new(IdxBuilder)
	idb.mapper = mapper
	idb.name = name

	return idb
}

func (i *index) String() string {
	return i.name
}

func (i *index) Write(key, value interface{}) error {
	keyByt, err := i.keyEncoder.Encode(key)
	if err != nil {
		return err
	}
	hashKey := i.mapper(key, value)
	return i.backend.Set([]byte(hashKey+string(keyByt)), keyByt, 0)
}

func (i *index) KeyIndexed(index string, key interface{}) (bool, error) {
	keyByt, err := i.keyEncoder.Encode(key)
	if err != nil {
		return false, err
	}

	kIdxed, err := i.backend.Get(append([]byte(index), keyByt...))
	if err != nil {
		return false, err
	}

	return kIdxed != nil, nil
}

func (i *index) Compare(key interface{}, valBefore, valAfter interface{}) (bool, error) {
	prefix := i.KeyPrefixer(key, valBefore)

	keyByt, err := i.keyEncoder.Encode(key)
	if err != nil {
		return false, err
	}

	kIdxed, err := i.backend.Get(append([]byte(prefix), keyByt...))
	if err != nil {
		return false, err
	}

	// Value is not yet indexed. nothing to compare
	if len(kIdxed) < 1 {
		return false, nil
	}

	prefixNew := i.KeyPrefixer(key, valAfter)

	return prefix+string(keyByt) == prefixNew+string(keyByt), nil
}

func (i *index) KeyPrefixer(key, val interface{}) (hash string) {
	return i.mapper(key, val)
}

func (i *index) Delete(key, value interface{}) error {
	hashKey := i.mapper(key, value)

	keyByt, err := i.keyEncoder.Encode(key)
	if err != nil {
		return err
	}

	return i.backend.Delete(append([]byte(hashKey), keyByt...))
}

func (i *index) Values(key string) ([]string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	itr := i.backend.PrefixedIterator([]byte(key))
	var vals []string

	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		vals = append(vals, string(itr.Value()))
	}

	return vals, nil
}

func (i *index) Keys() ([]string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	itr := i.backend.Iterator()
	var keys []string

	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		keys = append(keys, string(itr.Key()))
	}

	return keys, nil
}

func (i *index) Read(key string) (backend.Iterator, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.backend.PrefixedIterator([]byte(key)), nil
}

func (i *index) Lock() {
	i.mu.Lock()
}

func (i *index) Unlock() {
	i.mu.Unlock()
}

func (i *index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.backend.Close()
}

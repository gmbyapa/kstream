package state_stores

import (
	"context"
	"github.com/gmbyapa/kstream/streams/encoding"
	"time"

	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type StateStore struct {
	stores.Store
	topology.ChangelogSyncer
	cache stores.StoreCache
}

func (str *StateStore) Set(ctx context.Context, key, value interface{}, expiry time.Duration) error {
	return str.cache.Set(ctx, key, value, expiry)
}

func (str *StateStore) Get(ctx context.Context, key interface{}) (interface{}, error) {
	return str.cache.Get(ctx, key)
}

func (str *StateStore) Iterator(ctx context.Context) (stores.Iterator, error) {
	return str.cache.Iterator(ctx)
}

func (str *StateStore) PrefixedIterator(ctx context.Context, keyPrefix interface{}, prefixEncoder encoding.Encoder) (stores.Iterator, error) {
	return str.cache.PrefixedIterator(ctx, keyPrefix, prefixEncoder)
}

func (str *StateStore) Delete(ctx context.Context, key interface{}) error {
	return str.cache.Delete(ctx, key)
}

func (str *StateStore) Flush() error {
	return str.cache.Flush()
}

func (str *StateStore) ResetCache() {
	str.cache.Reset()
}

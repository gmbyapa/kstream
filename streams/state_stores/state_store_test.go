package state_stores

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/mock"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/stores"
	"reflect"
	"sort"
	"testing"
)

func newMockStore() *StateStore {
	str, err := stores.NewStore(
		`test`,
		encoding.StringEncoder{},
		encoding.StringEncoder{},
		stores.WithBackendBuilder(func(name string) (backend.Backend, error) {
			return mock.NewMockBackend(name, 0), nil
		}))
	if err != nil {
		panic(err)
	}
	return &StateStore{
		Store: str,
		cache: str.Cache(),
	}
}

type testRec struct {
	key, value interface{}
}

type scenario struct {
	store, cache, stateStore []testRec
	searchPrefix             string
}

const cacheDeleted = `cacheDeleted`
const cacheDoesNotExist = `cacheDoesNotExist`

const storeDeleted = `storeDeleted`
const storeDoesNotExist = `storeDoesNotExist`

func newScenario(t *testing.T, store, cache, final []testRec, searchPrefix string) {
	stateStr := newMockStore()
	defer stateStr.Close()

	for _, rec := range store {
		switch rec.value {
		case storeDeleted:
			if err := stateStr.Store.Delete(nil, rec.key); err != nil {
				t.Error(err)
			}
		case storeDoesNotExist:
		default:
			if err := stateStr.Store.Set(nil, rec.key, rec.value, 0); err != nil {
				t.Error(err)
			}
		}
	}

	for _, rec := range cache {
		switch rec.value {
		case cacheDoesNotExist:
		case cacheDeleted:
			stateStr.cache.Backend().Delete([]byte(rec.key.(string)))
		default:
			stateStr.cache.Backend().Set([]byte(rec.key.(string)), []byte(rec.value.(string)), 0)
		}
	}
	var i stores.Iterator
	var err error
	if searchPrefix != `` {
		i, err = stateStr.PrefixedIterator(context.Background(), searchPrefix, encoding.StringEncoder{})
	} else {
		i, err = stateStr.Iterator(context.Background())
	}

	if err != nil {
		t.Error(err)
	}

	var recs []testRec
	for i.SeekToFirst(); i.Valid(); i.Next() {
		v, err := i.Value()
		if err != nil {
			t.Error(err)
		}

		k, err := i.Key()
		if err != nil {
			t.Error(err)
		}
		recs = append(recs, testRec{key: k, value: v})
	}

	sort.Slice(recs, func(i, j int) bool {
		return recs[i].key.(string) < recs[j].key.(string)
	})

	sort.Slice(final, func(i, j int) bool {
		return final[i].key.(string) < final[j].key.(string)
	})

	if !reflect.DeepEqual(recs, final) {
		t.Logf(`expected: %v, got: %v`, final, recs)
		t.Fail()
	}
}

func TestStateStore_All(t *testing.T) {
	tests := []scenario{
		// New record version available in cache
		{
			store:      []testRec{{`1`, `999`}},
			cache:      []testRec{{`1`, `100`}},
			stateStore: []testRec{{`1`, `100`}},
		},
		// Record doesn't exist in cache
		{
			store:      []testRec{{`1`, `999`}},
			cache:      []testRec{{`1`, cacheDoesNotExist}},
			stateStore: []testRec{{`1`, `999`}},
		},
		// Record deleted in cache
		{
			store:      []testRec{{`1`, `999`}},
			cache:      []testRec{{`1`, cacheDeleted}},
			stateStore: nil,
		},
		// Record value nil in the store
		{
			store:      []testRec{{`1`, nil}},
			cache:      []testRec{{`1`, cacheDoesNotExist}},
			stateStore: nil,
		},
		// Cached record doesn't exist in store
		{
			store:      []testRec{{`1`, storeDoesNotExist}},
			cache:      []testRec{{`1`, `100`}},
			stateStore: []testRec{{`1`, `100`}},
		},
		// Record doesn't exist in cache or the store
		{
			store:      []testRec{{`1`, storeDoesNotExist}},
			cache:      []testRec{{`1`, cacheDoesNotExist}},
			stateStore: nil,
		},
		// Multiple record scenarios
		{
			store:      []testRec{{`1`, `100`}, {`2`, `200`}},
			cache:      []testRec{{`2`, `999`}},
			stateStore: []testRec{{`1`, `100`}, {`2`, `999`}},
		},
		{
			store:      []testRec{{`1`, `100`}, {`2`, `200`}},
			cache:      []testRec{{`2`, `999`}, {`2`, cacheDeleted}},
			stateStore: []testRec{{`1`, `100`}},
		},
		{
			store:      []testRec{{`1`, `100`}},
			cache:      []testRec{{`2`, `999`}},
			stateStore: []testRec{{`1`, `100`}, {`2`, `999`}},
		},
		{
			store:      []testRec{{`1`, `100`}},
			cache:      []testRec{{`2`, `999`}},
			stateStore: []testRec{{`1`, `100`}, {`2`, `999`}},
		},
		{
			store:      []testRec{{`1`, `100`}, {`2`, `200`}},
			cache:      []testRec{{`1`, cacheDeleted}, {`2`, cacheDeleted}},
			stateStore: nil,
		},
		{
			store:      []testRec{{`1`, `100`}, {`2`, `200`}},
			cache:      nil,
			stateStore: []testRec{{`1`, `100`}, {`2`, `200`}},
		},

		// Search by key prefix
		{
			store: []testRec{
				{`1`, `100`},
				{`22`, `2200`},
				{`21`, `2100`},
				{`33`, `200`},
				{`44`, `200`},
			},
			cache:        []testRec{{`21`, `3100`}, {`33`, `999`}},
			searchPrefix: `2`,
			stateStore:   []testRec{{`22`, `2200`}, {`21`, `3100`}},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf(`Store: %v | Cache: %v | Expected: %v`, tt.store, tt.cache, tt.stateStore), func(t *testing.T) {
			newScenario(t, tt.store, tt.cache, tt.stateStore, tt.searchPrefix)
		})
	}
}

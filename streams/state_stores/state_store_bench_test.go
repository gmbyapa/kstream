package state_stores

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/mock"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/stores"
	"testing"
)

var benchReadOnlyStore stores.Store
var benchReadOnlyRecCount = 10000000
var benchCachedRecCount = 1000

func seedBenchReadOnly() {
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

	ctx := context.Background()
	for i := 1; i <= benchReadOnlyRecCount; i++ {
		if err := str.Set(ctx, fmt.Sprint(i), `100`, 0); err != nil {
			panic(err)
		}
	}

	benchReadOnlyStore = &StateStore{
		Store: str,
		cache: str.Cache(),
	}

	for i := 1; i <= benchCachedRecCount; i++ {
		if err := benchReadOnlyStore.Set(ctx, fmt.Sprint(i), `100`, 0); err != nil {
			panic(err)
		}
	}
}

func BenchmarkStateStore_Iterator(b *testing.B) {
	seedBenchReadOnly()
	defer benchReadOnlyStore.Close()
	b.ResetTimer()
	b.ReportAllocs()
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i, err := benchReadOnlyStore.Iterator(ctx)
			if err != nil {
				b.Error(err)
			}
			var c int
			for i.SeekToFirst(); i.Valid(); i.Next() {
				c++
			}

			if c != benchReadOnlyRecCount {
				b.Error(`count`, c)
				b.Fail()
			}
		}
	})
}

func BenchmarkStateStore_PrefixedIterator(b *testing.B) {
	seedBenchReadOnly()
	defer benchReadOnlyStore.Close()
	b.ResetTimer()
	b.ReportAllocs()
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i, err := benchReadOnlyStore.PrefixedIterator(ctx, `500000`, encoding.StringEncoder{})
			if err != nil {
				b.Error(err)
			}
			var c int
			for i.SeekToFirst(); i.Valid(); i.Next() {
				c++
			}
			// Expected 50000, 500000, 500001, 500002, 500003, 500004, 500005, 500006, 500007, 500008, 500009
			if c != 11 {
				b.Errorf(`incorrect count: expected 11, got %d`, c)
				b.Fail()
			}
		}
	})
}

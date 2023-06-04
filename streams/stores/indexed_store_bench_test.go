package stores

import (
	"context"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/pebble"
	"github.com/gmbyapa/kstream/streams/encoding"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func setupStore(b *testing.B) IndexedStore {
	idx := buildIndexB(b)

	var bkBuilder backend.Builder = func(name string) (backend.Backend, error) {
		conf := pebble.NewConfig()
		dir, err := os.MkdirTemp(os.TempDir(), `*`)
		if err != nil {
			return nil, err
		}
		conf.Dir = dir

		return pebble.NewPebbleBackend(name, conf)

	}

	st, err := NewIndexedStore(
		`foo`,
		encoding.IntEncoder{},
		encoding.StringEncoder{},
		[]IndexBuilder{idx},
		WithBackendBuilder(bkBuilder))
	if err != nil {
		b.Error(err)
	}

	return st
}

func BenchmarkIndexedStore_Set(b *testing.B) {
	st := setupStore(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := st.Set(context.Background(), rand.Intn(99999)+1, `111,222`, 0); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkIndexedStore_GetIndexedRecords(b *testing.B) {
	st := setupStore(b)

	for i := 1; i < 99909; i++ {
		compKey := strconv.Itoa(rand.Intn(4)+1) + `:` + strconv.Itoa(i)
		if err := st.Set(context.Background(), i, compKey, 0); err != nil {
			b.Error(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := st.GetIndexedRecords(context.Background(), `foo`, strconv.Itoa(rand.Intn(4)+1)); err != nil {
				b.Error(err)
			}
		}
	})
}

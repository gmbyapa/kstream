package stores

import (
	"context"
	"strings"
	"testing"
)

func buildIndexB(b *testing.B) IndexBuilder {
	idx := NewIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	return idx
}

func buildIndexT(t *testing.T) IndexBuilder {
	idx := NewIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	return idx
}

func BenchmarkUpdateIndexes(b *testing.B) {
	st := setupStore(b)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := UpdateIndexes(context.Background(), st, 1, `111,222`); err != nil {
				b.Error(err)
			}

			if err := st.Set(context.Background(), 1, `111,222`, 0); err != nil {
				b.Error(err)
			}
		}
	})
}

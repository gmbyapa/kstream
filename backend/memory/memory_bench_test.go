/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package memory

import (
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"math/rand"
	"os"
	"testing"
)

var benchReadOnlyBackend backend.Backend
var benchReadOnlyRecCount = 10000000

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	seedBenchReadOnly()
	code := m.Run()
	benchReadOnlyBackend.Close()
	os.Exit(code)
}

func seedBenchReadOnly() {
	conf := NewConfig()
	benchReadOnlyBackend = NewMemoryBackend(`test`, conf)
	for i := 1; i <= benchReadOnlyRecCount; i++ {
		if err := benchReadOnlyBackend.Set([]byte(fmt.Sprint(i)), []byte(`100`), 0); err != nil {
			panic(err)
		}
	}
}

func BenchmarkMemory_Set(b *testing.B) {
	conf := NewConfig()
	conf.MetricsReporter = metrics.NoopReporter()
	bkend := NewMemoryBackend(`test`, conf)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bkend.Set([]byte(`100`), []byte(`100`), 0); err != nil {
				log.Fatal(err)
			}
		}
	})
}

func BenchmarkMemory_Get(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := fmt.Sprint(rand.Intn(benchReadOnlyRecCount-1) + 1)

			if _, err := benchReadOnlyBackend.Get([]byte(k)); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkMemory_GetSet(b *testing.B) {
	conf := NewConfig()
	conf.MetricsReporter = metrics.NoopReporter()
	bkend := NewMemoryBackend(`test`, conf)

	for i := 1; i <= 99999; i++ {
		if err := bkend.Set([]byte(fmt.Sprint(rand.Intn(1000)+1)), []byte(`100`), 0); err != nil {
			log.Fatal(err)
		}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := bkend.Get([]byte(fmt.Sprint(rand.Intn(1000) + 1))); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkMemory_Iterator(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := benchReadOnlyBackend.Iterator()
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

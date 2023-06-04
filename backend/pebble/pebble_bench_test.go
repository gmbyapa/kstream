/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package pebble_test

import (
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/pebble"
	"github.com/tryfix/log"
	"math/rand"
	"os"
	"testing"
)

var benchReadOnlyBackend backend.Backend
var benchReadOnlyRecCount = 10000000

func TestMain(m *testing.M) {
	seedBenchReadOnly()
	code := m.Run()
	benchReadOnlyBackend.Close()
	os.Exit(code)
}

func seedBenchReadOnly() {
	conf := pebble.NewConfig()
	tmp, err := os.MkdirTemp(``, `*`)
	if err != nil {
		panic(err)
	}
	conf.Dir = tmp
	bkend, err := pebble.NewPebbleBackend(`test`, conf)
	if err != nil {
		panic(err)
	}

	benchReadOnlyBackend = bkend
	for i := 1; i <= benchReadOnlyRecCount; i++ {
		if err := benchReadOnlyBackend.Set([]byte(fmt.Sprint(i)), []byte(`100`), 0); err != nil {
			panic(err)
		}
	}
}

func BenchmarkBadger_Set(b *testing.B) {
	conf := pebble.NewConfig()
	tmp, err := os.MkdirTemp(``, `*`)
	if err != nil {
		panic(err)
	}
	conf.Dir = tmp
	bkend, err := pebble.NewPebbleBackend(`test`, conf)
	if err != nil {
		panic(err)
	}

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

func BenchmarkBadger_Get(b *testing.B) {
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

func BenchmarkBadger_GetSet(b *testing.B) {
	conf := pebble.NewConfig()
	tmp, err := os.MkdirTemp(``, `*`)
	if err != nil {
		panic(err)
	}
	conf.Dir = tmp
	bkend, err := pebble.NewPebbleBackend(`test`, conf)
	if err != nil {
		panic(err)
	}

	for i := 1; i <= 99999; i++ {
		if err := bkend.Set([]byte(fmt.Sprint(rand.Intn(1000)+1)), []byte(`100`), 0); err != nil {
			log.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := bkend.Get([]byte(fmt.Sprint(rand.Intn(1000) + 1))); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkBadger_Iterator(b *testing.B) {
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

func BenchmarkBadger_PrefixedIterator(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := benchReadOnlyBackend.PrefixedIterator([]byte(`500000`))
			var c int
			for i.SeekToFirst(); i.Valid(); i.Next() {
				c++
			}
			// Expected 50000, 500000, 500001, 500002, 500003, 500004, 500005, 500006, 500007, 500008, 500009
			if c != 11 {
				b.Error(`count`, c)
				b.Fail()
			}
		}
	})
}

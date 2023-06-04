/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package pebble

import (
	"bytes"
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/tryfix/log"
	"os"
	"reflect"
	"sort"
	"testing"
)

func makeBackend(t *testing.T) backend.Backend {
	conf := NewConfig()
	tmp, err := os.MkdirTemp(``, `*`)
	if err != nil {
		t.Error(err)
	}
	conf.Dir = tmp
	backend, err := NewPebbleBackend(`test`, conf)
	if err != nil {
		t.Error(err)
	}

	return backend
}

func TestPebble_Set(t *testing.T) {
	backend := makeBackend(t)
	defer backend.Close()

	if err := backend.Set([]byte(`100`), []byte(`100`), 0); err != nil {
		log.Fatal(err)
	}

	r, err := backend.Get([]byte(`100`))
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(r, []byte(`100`)) {
		t.Error(`record exist`)
	}
}

func TestPebble_Get(t *testing.T) {
	backend := makeBackend(t)
	defer backend.Close()

	for i := 1; i <= 1000; i++ {
		if err := backend.Set([]byte(fmt.Sprint(i)), []byte(`100`), 0); err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= 1000; i++ {
		val, err := backend.Get([]byte(fmt.Sprint(i)))
		if err != nil {
			t.Error(err)
		}

		if string(val) != `100` {
			t.Fail()
		}
	}

}

func TestPebble_GetAll(t *testing.T) {
	backend := makeBackend(t)
	defer backend.Close()

	var keyVals []string
	for i := 1; i <= 1000; i++ {
		key := []byte(fmt.Sprint(i))
		val := []byte(fmt.Sprintf(`%d`, i*10))
		keyVals = append(keyVals, fmt.Sprintf(`%s-%s`, key, val))
		if err := backend.Set(key, val, 0); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(keyVals)

	i := backend.Iterator()
	defer i.Close()

	var keyValsHave []string
	for i.SeekToFirst(); i.Valid(); i.Next() {
		keyValsHave = append(keyValsHave, fmt.Sprintf(`%s-%s`, i.Key(), i.Value()))
	}

	sort.Strings(keyValsHave)
	if !reflect.DeepEqual(keyVals, keyValsHave) {
		t.Fail()
	}

}

func TestPebble_Delete(t *testing.T) {
	backend := makeBackend(t)
	defer backend.Close()

	if err := backend.Set([]byte(`100`), []byte(`100`), 0); err != nil {
		t.Fatal(err)
	}

	if err := backend.Delete([]byte(`100`)); err != nil {
		t.Fatal(err)
	}

	val, err := backend.Get([]byte(`100`))
	if err != nil {
		t.Error(err)
	}

	if val != nil {
		t.Fail()
	}
}

func TestPebble_PrefixedIterator(t *testing.T) {
	backend := makeBackend(t)
	defer backend.Close()

	recC := 100
	for i := 1; i <= recC; i++ {
		k := fmt.Sprint(i)
		if err := backend.Set([]byte(k), []byte(`0`), 0); err != nil {
			log.Fatal(err)
		}
	}

	i := backend.PrefixedIterator([]byte(`5`))
	var recs []string
	for i.SeekToFirst(); i.Valid(); i.Next() {
		recs = append(recs, string(i.Key()))
	}

	sort.Strings(recs)

	expected := []string{`5`, `50`, `51`, `52`, `53`, `54`, `55`, `56`, `57`, `58`, `59`}
	if !reflect.DeepEqual(recs, expected) {
		t.Errorf(`expected : %v, got: %v`, expected, recs)
		t.Fail()
	}
}

func TestPebble_Iterator(t *testing.T) {
	backend := makeBackend(t)
	defer backend.Close()

	recC := 10
	for i := 1; i <= recC; i++ {
		k := fmt.Sprint(i)
		if err := backend.Set([]byte(k), []byte(`0`), 0); err != nil {
			log.Fatal(err)
		}
	}

	i := backend.Iterator()
	var recs []string
	for i.SeekToFirst(); i.Valid(); i.Next() {
		recs = append(recs, string(i.Key()))
	}

	sort.Strings(recs)

	expected := []string{`1`, `10`, `2`, `3`, `4`, `5`, `6`, `7`, `8`, `9`}
	if !reflect.DeepEqual(recs, expected) {
		t.Errorf(`expected : %v, got: %v`, expected, recs)
		t.Fail()
	}
}

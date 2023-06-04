/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package memory

import (
	"fmt"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestMemory_Set_Expiry(t *testing.T) {
	conf := NewConfig()
	conf.RecordExpiryEnabled = true
	conf.ExpiredRecordCleanupInterval = 1 * time.Millisecond
	conf.MetricsReporter = metrics.NoopReporter()
	backend := NewMemoryBackend(`test`, conf)
	if err := backend.Set([]byte(`100`), []byte(`100`), 10*time.Millisecond); err != nil {
		log.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	r, err := backend.Get([]byte(`100`))
	if err != nil {
		t.Error(err)
	}

	if r != nil {
		t.Error(`record exist`)
	}
}

func TestMemory_Get(t *testing.T) {
	conf := NewConfig()
	conf.MetricsReporter = metrics.NoopReporter()
	backend := NewMemoryBackend(`test`, conf)

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

func TestMemory_GetAll(t *testing.T) {
	conf := NewConfig()
	conf.MetricsReporter = metrics.NoopReporter()
	backend := NewMemoryBackend(`test`, conf)

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

func TestMemory_Delete(t *testing.T) {
	conf := NewConfig()
	conf.MetricsReporter = metrics.NoopReporter()
	backend := NewMemoryBackend(`test`, conf)

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

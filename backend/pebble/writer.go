package pebble

import (
	pebbleDB "github.com/cockroachdb/pebble"
	"github.com/gmbyapa/kstream/backend"
	"github.com/tryfix/metrics"
	"time"
)

type Writer struct {
	pebble  *pebbleDB.DB
	metrics struct {
		updateLatency metrics.Observer
		deleteLatency metrics.Observer
	}
}

func (w *Writer) Set(key []byte, value []byte, expiry time.Duration) error {
	defer func(begin time.Time) {
		w.metrics.updateLatency.Observe(
			float64(time.Since(begin).Nanoseconds()/1e3), nil)
	}(time.Now())

	return w.pebble.Set(key, value, pebbleDB.NoSync)
}
func (w *Writer) SetAll(kayVals []backend.KeyVal, expiry time.Duration) error {
	defer func(begin time.Time) {
		w.metrics.updateLatency.Observe(
			float64(time.Since(begin).Nanoseconds()/1e3), nil)
	}(time.Now())

	batch := w.pebble.NewBatch()
	for _, keyVal := range kayVals {
		if err := batch.Set(keyVal.Key, keyVal.Val, pebbleDB.NoSync); err != nil {
			return err
		}
	}

	return w.pebble.Apply(batch, pebbleDB.NoSync)
}
func (w *Writer) Delete(key []byte) error {
	defer func(begin time.Time) {
		w.metrics.deleteLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), nil)
	}(time.Now())

	return w.pebble.Delete(key, pebbleDB.NoSync)
}
func (w *Writer) Flush() error {
	return w.pebble.Flush()
}

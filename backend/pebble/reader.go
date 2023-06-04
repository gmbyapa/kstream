package pebble

import (
	pebbleDB "github.com/cockroachdb/pebble"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/tryfix/metrics"
	"time"
)

type Reader struct {
	pebble  *pebbleDB.DB
	name    string
	metrics struct {
		readLatency             metrics.Observer
		iteratorLatency         metrics.Observer
		prefixedIteratorLatency metrics.Observer
	}
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	defer func(begin time.Time) {
		r.metrics.readLatency.Observe(
			float64(time.Since(begin).Nanoseconds()/1e3), nil)
	}(time.Now())

	valP, buf, err := r.pebble.Get(key)
	if err != nil {
		if errors.Is(err, pebbleDB.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	val := make([]byte, len(valP))

	copy(val, valP)

	if err := buf.Close(); err != nil {
		return nil, err
	}

	return val, nil
}

func (r *Reader) PrefixedIterator(keyPrefix []byte) backend.Iterator {
	defer func(begin time.Time) {
		r.metrics.prefixedIteratorLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), nil)
	}(time.Now())

	opts := new(pebbleDB.IterOptions)
	opts.LowerBound = keyPrefix
	opts.UpperBound = keyUpperBound(keyPrefix)
	return &Iterator{itr: r.pebble.NewIter(opts)}
}

func (r *Reader) Iterator() backend.Iterator {
	defer func(begin time.Time) {
		r.metrics.prefixedIteratorLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), nil)
	}(time.Now())

	return &Iterator{itr: r.pebble.NewIter(new(pebbleDB.IterOptions))}
}

func (r *Reader) Close() error {
	return r.pebble.Close()
}

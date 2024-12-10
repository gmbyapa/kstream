package pebble

import (
	pebbleDB "github.com/cockroachdb/pebble"
	"github.com/gmbyapa/kstream/v2/backend"
	"github.com/gmbyapa/kstream/v2/pkg/errors"
	"github.com/tryfix/metrics/v2"
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
	itr, err := r.pebble.NewIter(opts)
	if err != nil {
		panic(err)
	}
	return &Iterator{itr: itr}
}

func (r *Reader) Iterator() backend.Iterator {
	defer func(begin time.Time) {
		r.metrics.prefixedIteratorLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), nil)
	}(time.Now())

	itr, err := r.pebble.NewIter(new(pebbleDB.IterOptions))
	if err != nil {
		panic(err)
	}

	return &Iterator{itr: itr}
}

func (r *Reader) Close() error {
	return r.pebble.Close()
}

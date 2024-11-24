package pebble

import (
	"github.com/cockroachdb/pebble"
	"github.com/gmbyapa/kstream/v2/backend"
	"github.com/gmbyapa/kstream/v2/pkg/errors"
	"time"
)

type Cache struct {
	batch *pebble.Batch
}

func (c *Cache) Set(key []byte, value []byte, expiry time.Duration) error {
	return c.batch.Set(key, value, pebble.NoSync)
}

func (c *Cache) Get(key []byte) ([]byte, error) {
	valP, buf, err := c.batch.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
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

func (c *Cache) PrefixedIterator(keyPrefix []byte) backend.Iterator {
	opts := new(pebble.IterOptions)
	opts.LowerBound = keyPrefix
	opts.UpperBound = keyUpperBound(keyPrefix)
	itr, err := c.batch.NewIter(opts)
	if err != nil {
		panic(err)
	}
	return &Iterator{itr: itr}
}

func (c *Cache) Iterator() backend.Iterator {
	itr, err := c.batch.NewIter(new(pebble.IterOptions))
	if err != nil {
		panic(err)
	}
	return &Iterator{itr: itr}
}

func (c *Cache) Delete(key []byte) error {
	return c.batch.Delete(key, pebble.NoSync)
}

func (c *Cache) Flush() error {
	return c.batch.Commit(pebble.NoSync)
}

func (c *Cache) DeleteAll() error {
	return c.batch.DeleteRange(nil, nil, pebble.NoSync)
}

func (c *Cache) Reset() {
	c.batch.Reset()
}

func (c *Cache) Close() error {
	return c.batch.Close()
}

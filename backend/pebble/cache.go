package pebble

import (
	"github.com/cockroachdb/pebble"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/pkg/errors"
	//"sync"
	"time"
)

type Cache struct {
	batch *pebble.Batch
	//mu    *sync.Mutex
}

func (c *Cache) Set(key []byte, value []byte, expiry time.Duration) error {
	//c.mu.Lock()
	//defer c.mu.Unlock()

	return c.batch.Set(key, value, pebble.NoSync)
}

func (c *Cache) Get(key []byte) ([]byte, error) {
	//c.mu.Lock()
	//defer c.mu.Unlock()

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
	//c.mu.Lock()
	//defer c.mu.Unlock()

	opts := new(pebble.IterOptions)
	opts.LowerBound = keyPrefix
	opts.UpperBound = keyUpperBound(keyPrefix)
	return &Iterator{itr: c.batch.NewIter(opts)}
}

func (c *Cache) Iterator() backend.Iterator {
	//c.mu.Lock()
	//defer c.mu.Unlock()

	return &Iterator{itr: c.batch.NewIter(new(pebble.IterOptions))}
}

func (c *Cache) Delete(key []byte) error {
	//c.mu.Lock()
	//defer c.mu.Unlock()

	return c.batch.Delete(key, pebble.NoSync)
}

func (c *Cache) Flush() error {
	//c.mu.Lock()
	//defer c.mu.Unlock()

	return c.batch.Commit(pebble.NoSync)
}

func (c *Cache) Reset() {
	//c.mu.Lock()
	//defer c.mu.Unlock()

	c.batch.Reset()
}

func (c *Cache) Close() error {
	//c.mu.Lock()
	//defer c.mu.Unlock()

	return c.batch.Close()
}

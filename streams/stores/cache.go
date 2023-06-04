package stores

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/encoding"
	"time"
)

type Cache struct {
	backendCache backend.Cache
	keyEncoder   encoding.Encoder
	valEncoder   encoding.Encoder
	storeName    string
}

func (c *Cache) Flush() error {
	return c.backendCache.Flush()
}

func (c *Cache) Close() error {
	return c.backendCache.Close()
}

func (c *Cache) Reset() {
	c.backendCache.Reset()
}

func (c *Cache) Backend() backend.Cache {
	return c.backendCache
}

func (c *Cache) Set(ctx context.Context, key, value interface{}, expiry time.Duration) error {
	k, err := c.keyEncoder.Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode error`, c.storeName))
	}

	// if value is null remove from store (tombstone) TODO may be we dont need it here
	if value == nil {
		return c.backendCache.Delete(k)
	}

	v, err := c.valEncoder.Encode(value)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] value encode err `, c.storeName))
	}

	return c.backendCache.Set(k, v, expiry)
}

func (c *Cache) Delete(ctx context.Context, key interface{}) error {
	k, err := c.keyEncoder.Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode err `, c.storeName))
	}

	return c.backendCache.Delete(k)
}

func (c *Cache) Get(ctx context.Context, key interface{}) (value interface{}, err error) {
	k, err := c.keyEncoder.Encode(key)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`store [%s] key encode err `, c.storeName))
	}

	byt, err := c.backendCache.Get(k)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`store [%s] value dose not exist `, c.storeName))
	}

	if len(byt) < 1 {
		return nil, nil
	}

	v, err := c.valEncoder.Decode(byt)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`store [%s] value decode err `, c.storeName))
	}

	return v, nil
}

func (c *Cache) Iterator(ctx context.Context) (Iterator, error) {
	i := c.backendCache.Iterator()
	i.SeekToFirst()

	return &iterator{
		i:          i,
		keyEncoder: c.keyEncoder,
		valEncoder: c.valEncoder,
	}, nil
}

func (c *Cache) PrefixedIterator(ctx context.Context, keyPrefix interface{}, prefixEncoder encoding.Encoder) (Iterator, error) {
	prefix, err := prefixEncoder.Encode(keyPrefix)
	if err != nil {
		return nil, errors.Wrapf(err, `prefix encode error`)
	}

	i := c.backendCache.PrefixedIterator(prefix)
	i.SeekToFirst()

	return &iterator{
		i:          i,
		keyEncoder: c.keyEncoder,
		valEncoder: c.valEncoder,
	}, nil
}

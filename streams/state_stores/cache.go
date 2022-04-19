package state_stores

// Cache is a simple key value cache used to keep data before flushing to state stores.
type Cache struct {
	records map[string][]byte
}

func newCache() *Cache {
	return &Cache{records: map[string][]byte{}}
}

func (c *Cache) Write(k, v []byte) {
	c.records[string(k)] = v
}

func (c *Cache) Read(key []byte) []byte {
	return c.records[string(key)]
}

func (c *Cache) Has(key []byte) bool {
	_, ok := c.records[string(key)]
	return ok
}

func (c *Cache) Delete(key []byte) {
	c.records[string(key)] = nil
}

func (c *Cache) Purge() {
	c.records = map[string][]byte{}
}

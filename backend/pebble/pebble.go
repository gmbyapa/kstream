/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package pebble

import (
	"fmt"
	pebbleDB "github.com/cockroachdb/pebble"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	//"sync"
)

type Config struct {
	MetricsReporter metrics.Reporter
	Dir             string
	Options         *pebbleDB.Options
}

func NewConfig() *Config {
	conf := new(Config)
	conf.Dir = `storage/pebble`
	conf.Options = &pebbleDB.Options{}
	conf.parse()

	return conf
}

func (c *Config) parse() {
	if c.MetricsReporter == nil {
		c.MetricsReporter = metrics.NoopReporter()
	}
}

type Pebble struct {
	logger log.Logger
	pebble *pebbleDB.DB
	*Reader
	*Writer
	metrics struct {
		storageSize metrics.Gauge
	}
}

func Builder(config *Config) backend.Builder {
	return func(name string) (backend backend.Backend, err error) {
		return NewPebbleBackend(name, config)
	}
}

func NewPebbleBackend(name string, config *Config) (*Pebble, error) {
	dbName := fmt.Sprintf(`%s/pebble/%s`, config.Dir, name)

	pb, err := pebbleDB.Open(dbName, config.Options)
	if err != nil {
		return nil, errors.Wrapf(err, `db open error, backend:%s`, dbName)
	}

	m := &Pebble{}
	m.pebble = pb
	m.Reader = &Reader{pebble: pb, name: dbName}
	m.Writer = &Writer{pebble: pb}

	constLabels := map[string]string{`name`: name, `type`: `memory`}
	m.Reader.metrics.readLatency = config.MetricsReporter.Observer(
		metrics.MetricConf{Path: `backend_read_latency_microseconds`, ConstLabels: constLabels})
	m.Reader.metrics.iteratorLatency = config.MetricsReporter.Observer(
		metrics.MetricConf{Path: `backend_read_iterator_latency_microseconds`, ConstLabels: constLabels})
	m.Reader.metrics.prefixedIteratorLatency = config.MetricsReporter.Observer(
		metrics.MetricConf{Path: `backend_read_prefix_iterator_latency_microseconds`, ConstLabels: constLabels})
	m.Writer.metrics.updateLatency = config.MetricsReporter.Observer(
		metrics.MetricConf{Path: `backend_update_latency_microseconds`, ConstLabels: constLabels})
	m.metrics.storageSize = config.MetricsReporter.Gauge(
		metrics.MetricConf{Path: `backend_storage_size`, ConstLabels: constLabels})
	m.Writer.metrics.deleteLatency = config.MetricsReporter.Observer(
		metrics.MetricConf{Path: `backend_delete_latency_microseconds`, ConstLabels: constLabels})

	return m, nil
}

func (p *Pebble) Name() string {
	return `pebble`
}

func (p *Pebble) String() string {
	return `pebble`
}

func (p *Pebble) Persistent() bool {
	return false
}

func (p *Pebble) Cache() backend.Cache {
	return &Cache{
		batch: p.pebble.NewIndexedBatch(),
		//mu:    &sync.Mutex{},
	}
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil // no upper-bound
}

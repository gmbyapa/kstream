package stores

import (
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/memory"
)

type StoreOptions struct {
	storageDir        string
	backend           backend.Backend
	backendBuilder    backend.Builder
	versionExtractor  RecordVersionExtractor
	versionWriter     RecordVersionWriter
	buffered          bool
	bufferSize        int
	compactionEnabled bool
	cachingEnabled    bool
}

type Option func(config *StoreOptions)

func (c *StoreOptions) applyDefault() {
	c.backendBuilder = memory.Builder(memory.NewConfig())
}

func (c *StoreOptions) apply(options ...Option) {
	for _, opt := range options {
		opt(c)
	}
}

func WithVersionExtractor(etc RecordVersionExtractor) Option {
	return func(config *StoreOptions) {
		config.versionExtractor = etc
	}
}

func WithCachingEnabled() Option {
	return func(config *StoreOptions) {
		config.cachingEnabled = true
	}
}

func WithVersionWriter(wr RecordVersionWriter) Option {
	return func(config *StoreOptions) {
		config.versionWriter = wr
	}
}

func Compacated() Option {
	return func(options *StoreOptions) {
		options.compactionEnabled = true
	}
}

func Buffered(size int) Option {
	return func(options *StoreOptions) {
		options.buffered = true
		options.bufferSize = size
	}
}

func WithBackend(backend backend.Backend) Option {
	return func(config *StoreOptions) {
		config.backend = backend
	}
}

func WithBackendBuilder(builder backend.Builder) Option {
	return func(config *StoreOptions) {
		config.backendBuilder = builder
	}
}

package streams

import (
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/state_stores"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
)

// Starting offset for the global table partition.
//type GlobalTableOffset consumer.Offset

// GlobalTableOffsetDefault defines the starting offset for the GlobalTable when GlobalTable stream syncing started.
const GlobalTableOffsetDefault = kafka.OffsetEarliest

// GlobalTableOffsetLatest defines the beginning of the partition.
// Suitable for topics with retention policy delete since the topic can contain historical data.
const GlobalTableOffsetLatest = kafka.OffsetLatest

type globalTableOptions struct {
	initialOffset    kafka.Offset
	logger           log.Logger
	storeOptions     []stores.Option
	store            stores.Store
	changelogOptions []state_stores.ChangelogBuilderOption
	storeBuilder     topology.ChangelogSyncerBuilder
}

func newGlobalTableOptsApplier(config *Config) *globalTableOptions {
	opts := new(globalTableOptions)
	opts.logger = config.Logger
	opts.initialOffset = GlobalTableOffsetDefault
	return opts
}

func (opts *globalTableOptions) apply(options ...GlobalTableOption) {
	opts.initialOffset = GlobalTableOffsetDefault
	for _, o := range options {
		o(opts)
	}
}

type GlobalTableOption func(options *globalTableOptions)

// GlobalTableWithOffset overrides the default starting offset when GlobalTable syncing started.
func GlobalTableWithOffset(offset kafka.Offset) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.initialOffset = offset
	}
}

// GlobalTableWithLogger overrides the default logger for the GlobalTable (default is log.NoopLogger).
func GlobalTableWithLogger(logger log.Logger) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.logger = logger
	}
}

func GlobalTableWithStoreOptions(opts ...stores.Option) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.storeOptions = opts
	}
}

func GlobalTableWithStoreChangelogOptions(opts ...state_stores.ChangelogBuilderOption) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.changelogOptions = opts
	}
}

func GlobalTableWithStore(store stores.Store) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.store = store
	}
}

type globalKTableStream struct {
	*kStream
	store topology.LoggableStoreBuilder
}

func (g *globalKTableStream) Store() topology.LoggableStoreBuilder {
	return g.store
}

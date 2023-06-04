package state_stores

import (
	"fmt"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type StoreBuilderOption func(builder *stateStoreBuilder)

func WithChangelogSyncDisabled() StoreBuilderOption {
	return func(builder *stateStoreBuilder) {
		builder.changelog.syncerEnabled = false
	}
}

func ChangelogSyncEnabled() StoreBuilderOption {
	return func(builder *stateStoreBuilder) {
		builder.changelog.syncerEnabled = true
	}
}

func LoggingDisabled() StoreBuilderOption {
	return func(builder *stateStoreBuilder) {
		builder.changelog.loggingEnabled = false
	}
}

func WithNameFunc(fn topology.StateStoreNameFunc) StoreBuilderOption {
	return func(builder *stateStoreBuilder) {
		builder.nameFormatter = fn
	}
}

func WithChangelogOptions(options ...ChangelogBuilderOption) StoreBuilderOption {
	return func(builder *stateStoreBuilder) {
		builder.changelog.options = append(builder.changelog.options, options...)
	}
}

func UseStoreBuilder(nativeBuilder stores.StoreBuilder) StoreBuilderOption {
	return func(builder *stateStoreBuilder) {
		builder.store = nativeBuilder
	}
}

func StoreBuilderWithStoreOption(options ...stores.Option) StoreBuilderOption {
	return func(builder *stateStoreBuilder) {
		builder.options = options
	}
}

func StoreBuilderWithKeyEncoder(encoder encoding.Encoder) StoreBuilderOption {
	return func(builder *stateStoreBuilder) {
		builder.keyEnc = encoder
	}
}

func StoreBuilderWithValEncoder(encoder encoding.Encoder) StoreBuilderOption {
	return func(builder *stateStoreBuilder) {
		builder.valEnc = encoder
	}
}

type stateStoreBuilder struct {
	keyEnc, valEnc encoding.Encoder
	options        []stores.Option
	store          stores.StoreBuilder
	nameFormatter  topology.StateStoreNameFunc
	changelog      struct {
		syncerEnabled  bool
		loggingEnabled bool
		options        []ChangelogBuilderOption
		builder        topology.ChangelogSyncerBuilder
	}
}

func NewStoreBuilder(name string, keyEncoder encoding.Encoder, valEncoder encoding.Encoder, options ...StoreBuilderOption) topology.LoggableStoreBuilder {
	b := &stateStoreBuilder{
		keyEnc: keyEncoder,
		valEnc: valEncoder,
		nameFormatter: func(store string) string {
			return store
		},
	}

	b.changelog.syncerEnabled = true
	b.changelog.loggingEnabled = true

	for _, opt := range options {
		opt(b)
	}

	if b.store == nil {
		b.store = stores.NewDefaultStoreBuilder(name, keyEncoder, valEncoder, b.options...)
	}

	b.changelog.builder = NewChangelogBuilder(
		b.store,
		b.changelog.options...,
	)

	return b
}

func (d *stateStoreBuilder) Name() string {
	return d.store.Name()
}

func (d *stateStoreBuilder) NameFormatter(ctx topology.SubTopologyContext) topology.StateStoreNameFunc {
	return func(store string) string {
		return fmt.Sprintf(`%s-%d`, store, ctx.Partition())
	}
}

func (d *stateStoreBuilder) Changelog() topology.ChangelogSyncerBuilder {
	return d.changelog.builder
}

func (d *stateStoreBuilder) KeyEncoder() encoding.Encoder {
	return d.keyEnc
}

func (d *stateStoreBuilder) ValEncoder() encoding.Encoder {
	return d.keyEnc
}

func (d *stateStoreBuilder) Build(ctx topology.SubTopologyContext) (topology.StateStore, error) {
	storeName := d.NameFormatter(ctx)(d.store.Name())
	store, err := d.store.Build(storeName, append(d.options, stores.WithCachingEnabled())...)
	if err != nil {
		return nil, errors.Wrap(err, `store build failed`)
	}

	syncer, err := d.changelog.builder.Build(ctx, store)
	if err != nil {
		return nil, errors.Wrap(err, `changelogSyncer initiate failed`)
	}

	stor := &StateStore{
		Store:           store,
		ChangelogSyncer: syncer,
		cache:           store.Cache(),
	}

	if !d.changelog.loggingEnabled {
		return stor, nil
	}

	logger, err := d.changelog.builder.BuildLogger(ctx, d.store.Name())
	if err != nil {
		return nil, errors.Wrap(err, `changelogSyncer build failed`)
	}

	return &loggableStateStoreInstance{
		StateStore:   stor,
		ChangeLogger: logger,
	}, nil
}

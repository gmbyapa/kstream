package streams

import (
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/processors"
	"github.com/gmbyapa/kstream/streams/state_stores"
	"github.com/gmbyapa/kstream/streams/topology"
)

type StreamOptions struct {
	nodeName           string
	disableRepartition bool
}

func applyNodeOptions(node topology.NodeBuilder, options []StreamOption) *StreamOptions {
	opts := new(StreamOptions)
	opts.apply(options...)
	node.NameAs(opts.nodeName)

	return opts
}

type StreamOption func(options *StreamOptions)

func (opt *StreamOptions) apply(options ...StreamOption) {
	for _, option := range options {
		option(opt)
	}
}

func Named(name string) StreamOption {
	return func(options *StreamOptions) {
		options.nodeName = name
	}
}

// DisableRepartition disables stream repartitioning
// even if it's marked for repartitioning. Usefull when
// dealing with custom partitioners and joiners where
// partition assignment doesn't rely on keys.
func DisableRepartition() StreamOption {
	return func(options *StreamOptions) {
		options.disableRepartition = true
	}
}

type JoinOptions struct {
	streamOptions []StreamOption
	lookupFunc    processors.ValueLookupFunc
	oneToMany     bool
}

type JoinOption func(options *JoinOptions)

type JoinOpts interface {
	apply(options ...JoinOption)
}

func (opt *JoinOptions) apply(options ...JoinOption) {
	for _, option := range options {
		option(opt)
	}
}

func JoinWithStreamOptions(opts ...StreamOption) JoinOption {
	return func(options *JoinOptions) {
		options.streamOptions = append(options.streamOptions, opts...)
	}
}

func JoinWithValueLookupFunc(fn processors.ValueLookupFunc) JoinOption {
	return func(options *JoinOptions) {
		options.lookupFunc = fn
	}
}

type TableOpts struct {
	streamOpts       []StreamOption
	storeBuilderOpts []state_stores.StoreBuilderOption
	encoders         struct {
		key, val encoding.Encoder
	}
	useSourceAsChangelog bool
}

func newTableOptsApplier(config *Config) *TableOpts {
	opts := new(TableOpts)
	opts.storeBuilderOpts = append(
		opts.storeBuilderOpts,
		state_stores.WithChangelogOptions(
			state_stores.WithChangelogTopicReplicaCount(config.Store.Changelog.ReplicaCount),
		))
	return opts
}

func (tblOpts *TableOpts) apply(opts ...TableOpt) {
	for _, opt := range opts {
		opt(tblOpts)
	}
}

type TableOpt func(opts *TableOpts)

func TableWithStoreOptions(options ...state_stores.StoreBuilderOption) TableOpt {
	return func(opts *TableOpts) {
		for _, opt := range options {
			opts.storeBuilderOpts = append(opts.storeBuilderOpts, opt)
		}
	}
}

func TableWithStreamOptions(options ...StreamOption) TableOpt {
	return func(opts *TableOpts) {
		opts.streamOpts = append(opts.streamOpts, options...)
	}
}

func TableWithSourceAsChangelog() TableOpt {
	return func(opts *TableOpts) {
		opts.useSourceAsChangelog = true
	}
}

func TableWithKeyEncoder(enc encoding.Encoder) TableOpt {
	return func(opts *TableOpts) {
		opts.encoders.key = enc
	}
}

func TableWithValEncoder(enc encoding.Encoder) TableOpt {
	return func(opts *TableOpts) {
		opts.encoders.val = enc
	}
}

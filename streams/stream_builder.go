package streams

import (
	"fmt"
	"github.com/gmbyapa/kstream/backend"
	"github.com/gmbyapa/kstream/backend/pebble"
	"github.com/gmbyapa/kstream/kafka"
	librd3Adpt "github.com/gmbyapa/kstream/kafka/adaptors/librd"
	"github.com/gmbyapa/kstream/kafka/adaptors/sarama"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/state_stores"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type StreamBuilder struct {
	tpBuilder topology.Builder
	config    *Config
	providers struct {
		groupConsumer kafka.GroupConsumerProvider
		consumer      kafka.ConsumerProvider
		producer      kafka.ProducerProvider
	}
	builders struct {
		backend     backend.Builder
		stores      stores.Builder
		indexStores stores.IndexedStoreBuilder
	}
	kafkaAdmin    kafka.Admin
	storeRegistry stores.Registry
	builderCtx    topology.BuilderContext
}

type BuilderOpt func(config *StreamBuilder)

func BuilderWithBackendBuilder(builder backend.Builder) BuilderOpt {
	return func(config *StreamBuilder) {
		config.builders.backend = builder
	}
}

func BuilderWithStoreBuilder(builder stores.Builder) BuilderOpt {
	return func(config *StreamBuilder) {
		config.builders.stores = builder
	}
}

func BuilderWithProducerProvider(provider kafka.ProducerProvider) BuilderOpt {
	return func(config *StreamBuilder) {
		config.providers.producer = provider
	}
}

func BuilderWithAdminClient(admin kafka.Admin) BuilderOpt {
	return func(config *StreamBuilder) {
		config.kafkaAdmin = admin
	}
}

func BuilderWithConsumerProvider(provider kafka.GroupConsumerProvider) BuilderOpt {
	return func(config *StreamBuilder) {
		config.providers.groupConsumer = provider
	}
}

func BuilderWithConsumerAdaptor(provider kafka.ConsumerProvider) BuilderOpt {
	return func(config *StreamBuilder) {
		config.providers.consumer = provider
	}
}

func NewStreamBuilder(config *Config, opts ...BuilderOpt) *StreamBuilder {
	config.setUp()
	if err := config.validate(); err != nil {
		panic(err)
	}

	b := &StreamBuilder{
		config: config,
	}

	// Apply builders
	config.Logger = config.Logger.NewLog(log.Prefixed(`kStream`))

	b.setupOpts(opts...)

	b.storeRegistry = stores.NewRegistry(&stores.RegistryConfig{
		Host:                config.Store.Http.Host,
		HttpEnabled:         config.Store.Http.Enabled,
		StoreBuilder:        b.builders.stores,
		IndexedStoreBuilder: b.builders.indexStores,
		Logger:              b.config.Logger,
		MetricsReporter:     config.MetricsReporter,
	})

	b.tpBuilder = NewKTopologyBuilder(b.storeRegistry, b.config.Logger.NewLog(log.Prefixed(`KTopologyBuilder`)))

	return b
}

func (b *StreamBuilder) KStream(topic string, keyEnc, valEnc encoding.Encoder, opts ...KSourceOption) Stream {
	// Check if the topic is already registered
	tps := append(
		b.tpBuilder.SubTopologies().SourceTopicsFor(topology.KindStream),
		b.tpBuilder.SubTopologies().SourceTopicsFor(topology.KindTable)...,
	)

	for _, tp := range tps {
		if tp == topic {
			panic(fmt.Sprintf(`Topic [%s] already registered with another stream topology`, topic))
		}
	}

	source := NewKSource(topic,
		append(
			opts,
			ConsumeWithKeyEncoder(keyEnc),
			ConsumeWithValEncoder(valEnc))...)
	sTp := b.tpBuilder.NewKSubTopologyBuilder(topology.KindStream)

	sTp.AddSource(source)

	return &kStream{
		tpBuilder:  b.tpBuilder,
		stpBuilder: sTp,
		kSource:    source,
		rootNode:   source,
		builder:    b,
		encoders:   struct{ key, val encoding.Encoder }{key: keyEnc, val: valEnc},
	}
}

func (b *StreamBuilder) GlobalTable(topic string, keyEnc, valEnc encoding.Encoder, storeName string, options ...GlobalTableOption) GlobalTable {
	gTableOpts := newGlobalTableOptsApplier(b.config)
	gTableOpts.changelogOptions = append(gTableOpts.changelogOptions,
		state_stores.ChangelogWithSourceTopic(topic))
	gTableOpts.apply(options...)

	src := NewKSource(topic,
		ConsumeWithKeyEncoder(keyEnc),
		ConsumeWithValEncoder(valEnc),
		markAsGlobal(),
	)

	if gTableOpts.store == nil {
		store, err := b.storeRegistry.CreateOrReturn(storeName, keyEnc, valEnc, gTableOpts.storeOptions...)
		if err != nil {
			panic(err)
		}

		gTableOpts.store = store.(stores.Store)
	}

	storeBuilder := state_stores.NewStoreBuilder(
		storeName,
		keyEnc,
		valEnc,
		state_stores.UseStoreBuilder(&GlobalStoreBuilderWrapper{store: gTableOpts.store}),
		state_stores.WithChangelogOptions(gTableOpts.changelogOptions...),
		state_stores.LoggingDisabled(),
	)

	sTp := b.tpBuilder.NewKSubTopologyBuilder(topology.KindGlobalTable)

	sTp.AddSource(src)
	sTp.AddStore(storeBuilder)
	sTp.AddNodeWithEdge(src, &GlobalTableNode{
		StoreName: storeName,
	})

	return &globalKTableStream{
		kStream: &kStream{
			tpBuilder:  b.tpBuilder,
			stpBuilder: sTp,
			kSource:    src,
			rootNode:   src,
			builder:    b,
		},
		store: storeBuilder,
	}
}

func (b *StreamBuilder) Build() (topology.Topology, error) {
	b.builderCtx = b.newBuilderCtx()
	return b.tpBuilder.Build(b.builderCtx)
}

func (b *StreamBuilder) StoreRegistry() stores.Registry {
	return b.storeRegistry
}

func (b *StreamBuilder) NewRunner() Runner {
	return &streamRunner{
		groupConsumer:     b.providers.groupConsumer.NewBuilder(b.config.Consumer),
		consumerCount:     b.config.Processing.ConsumerCount,
		partitionConsumer: b.providers.consumer.NewBuilder(b.config.Consumer.ConsumerConfig),
		logger:            b.config.Logger.NewLog(log.Prefixed(`StreamRunner`)),
		taskManagerBuilder: func(logger log.Logger, topologies topology.SubTopologyBuilders) (tasks.TaskManager, error) {
			partitionConsumer, err := b.providers.consumer.NewBuilder(b.config.Consumer.ConsumerConfig)(func(config *kafka.ConsumerConfig) {
				config.Logger = logger
				config.MetricsReporter = b.config.MetricsReporter
			})
			if err != nil {
				return nil, errors.Wrap(err, `TaskManager build error`)
			}

			return tasks.NewTaskManager(
				b.builderCtx,
				logger,
				partitionConsumer,
				topologies,
				b.config.Processing.Guarantee == ExactlyOnce,
				tasks.WithBufferSize(b.config.Processing.Buffer.Size),
				tasks.WithBufferFlushInterval(b.config.Processing.Buffer.FlushInterval),
				tasks.WithFailedMessageHandler(b.config.Processing.FailedMessageHandler),
			)
		},
		ctx: b.builderCtx,
	}
}

func (b *StreamBuilder) Topology() topology.Builder {
	return b.tpBuilder
}

func (b *StreamBuilder) newBuilderCtx() topology.BuilderContext {
	return topology.NewBuilderContext(
		b.config.ApplicationId,
		b.storeRegistry,
		b.providers.producer.NewBuilder(b.config.Producer),
		b.kafkaAdmin,
		b.config.Logger,
		b.config.MetricsReporter,
	)
}

func (b *StreamBuilder) setupOpts(opts ...BuilderOpt) {
	// default backend builder will be pebble
	backendBuilderConfig := pebble.NewConfig()
	backendBuilderConfig.Dir = b.config.Store.StateDir
	b.builders.backend = pebble.Builder(backendBuilderConfig)

	backendBuilderConfig.MetricsReporter = b.config.MetricsReporter.Reporter(metrics.ReporterConf{
		Subsystem: "kstream_backends",
	})
	b.builders.stores = func(name string, keyEncoder, valEncoder encoding.Encoder, options ...stores.Option) (stores.Store, error) {
		return stores.NewStore(name, keyEncoder, valEncoder, append(
			options,
			stores.WithBackendBuilder(b.builders.backend),
		)...)
	}

	b.builders.indexStores = func(name string, keyEncoder, valEncoder encoding.Encoder, indexes []stores.IndexBuilder, options ...stores.Option) (stores.IndexedStore, error) {
		return stores.NewIndexedStore(name, keyEncoder, valEncoder, indexes, append(
			options,
			stores.WithBackendBuilder(b.builders.backend),
		)...)
	}

	admin, err := sarama.NewAdmin(b.config.BootstrapServers, sarama.WithLogger(b.config.Logger))
	if err != nil {
		panic(err)
	}

	b.kafkaAdmin = admin

	// Apply default adaptors
	b.providers.groupConsumer = librd3Adpt.NewGroupConsumerProvider(librd3Adpt.NewGroupConsumerConfig())
	b.providers.consumer = librd3Adpt.NewConsumerProvider(librd3Adpt.NewConsumerConfig())
	b.providers.producer = librd3Adpt.NewProducerProvider(librd3Adpt.NewProducerConfig())

	for _, opt := range opts {
		opt(b)
	}
}

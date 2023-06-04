package state_stores

import (
	"fmt"
	"sync"

	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/topology"
	logger "github.com/tryfix/log"
)

type ChangelogTopicFormatter func(storeName string) func(ctx topology.BuilderContext) string

type topicConfig struct {
	Name          string
	Configs       map[string]string
	Replicas      int16
	NameFormatter ChangelogTopicFormatter
	internal      bool
}

type ChangelogBuilderOption func(store *changelogBuilder)

func ChangelogWithTopicConfigs(config map[string]string) ChangelogBuilderOption {
	return func(builder *changelogBuilder) {
		builder.topic.Configs = config
	}
}

func ChangelogWithTopicTopicNameFormatter(fn ChangelogTopicFormatter) ChangelogBuilderOption {
	return func(builder *changelogBuilder) {
		builder.topic.NameFormatter = fn
	}
}

func ChangelogWithTopicReplicaCount(count int16) ChangelogBuilderOption {
	return func(builder *changelogBuilder) {
		builder.topic.Replicas = count
	}
}

func ChangelogWithSourceTopic(topic string) ChangelogBuilderOption {
	return func(builder *changelogBuilder) {
		builder.topic.Name = topic
		builder.topic.internal = false
	}
}

type changelogBuilder struct {
	topic       topicConfig
	logger      logger.Logger
	storeName   string
	offsetStore OffsetStore
}

func NewChangelogBuilder(store stores.StoreBuilder, opts ...ChangelogBuilderOption) topology.ChangelogSyncerBuilder {
	b := &changelogBuilder{
		storeName: store.Name(),
		topic: topicConfig{
			Configs: map[string]string{},
		},
	}

	// Apply default options
	for _, opt := range opts {
		opt(b)
	}

	// Make sure changelog topics are always compacted
	b.topic.Configs[`cleanup.policy`] = `compact`

	return b
}

func (b *changelogBuilder) Setup(ctx topology.SubTopologyBuilderContext) error {
	b.logger = ctx.Logger().NewLog(logger.Prefixed(fmt.Sprintf(`StateStore(%s).ChangelogBuilder`, b.storeName)))

	// If the changelog topic is not provided create it
	if b.topic.Name == `` {
		if err := b.createChangelogTopic(ctx); err != nil {
			return err
		}
	}

	b.logger.Info(fmt.Sprintf(`Using [%s] as changelog topic`, b.topic.Name))

	// Create and register an OffsetStore
	b.logger.Info(`Offset store creating...`)
	store, err := ctx.StoreRegistry().Create(
		fmt.Sprintf(`%s-offsets`, b.storeName),
		encoding.StringEncoder{},
		encoding.IntEncoder{})
	if err != nil {
		return errors.Wrap(err, `cannot create offset store`)
	}
	b.logger.Info(`Offset store created`)

	b.offsetStore = newOffsetStore(store)

	return nil
}

func (b *changelogBuilder) Internal() bool {
	return b.topic.internal
}

func (b *changelogBuilder) Topic() string {
	return b.topic.Name
}

func (b *changelogBuilder) Build(ctx topology.SubTopologyContext, store stores.Store) (topology.ChangelogSyncer, error) {
	tp := kafka.TopicPartition{
		Topic:     b.topic.Name,
		Partition: ctx.Partition(),
	}

	if b.offsetStore == nil {
		return nil, errors.New(fmt.Sprintf(`Offset store [%s] for changelog %s is empty`, b.storeName, tp))
	}

	return &changelogSyncer{
		tp:          tp,
		offsetStore: b.offsetStore,
		consumer:    ctx.PartitionConsumer(),
		logger:      ctx.Logger().NewLog(logger.Prefixed(fmt.Sprintf(`StateStore(%s).ChangelogSyncer(%s)`, store.Name(), tp.String()))),
		store:       store,
		stopping:    make(chan struct{}, 1),
		running:     make(chan struct{}, 1),
		mu:          new(sync.Mutex),
	}, nil
}

func (b *changelogBuilder) BuildLogger(ctx topology.SubTopologyContext, store string) (topology.ChangeLogger, error) {
	return NewChangeLogger(ctx.Producer(), kafka.TopicPartition{
		Topic:     b.topic.Name,
		Partition: ctx.Partition(),
	}), nil
}

func (b *changelogBuilder) createChangelogTopic(ctx topology.SubTopologyBuilderContext) error {
	b.logger.Info(`Creating changelog topic`)
	b.topic.Name = b.topic.NameFormatter(b.storeName)(ctx)
	b.topic.internal = true

	// Create the changelog topic
	if err := ctx.Admin().StoreConfigs([]*kafka.Topic{
		{
			Name:              b.topic.Name,
			NumPartitions:     ctx.MaxPartitionCount(),
			ReplicationFactor: b.topic.Replicas,
			ConfigEntries:     b.topic.Configs,
		},
	}); err != nil {
		return errors.Wrap(err, `cannot create changelog topic`)
	}

	b.logger.Info(`Changelog topic created`)

	return nil
}

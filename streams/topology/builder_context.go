package topology

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type BuilderContext interface {
	StoreRegistry() stores.Registry
	ProducerBuilder() kafka.ProducerBuilder
	Admin() kafka.Admin
	ApplicationId() string
	Logger() log.Logger
	MetricsReporter() metrics.Reporter
}

type builderContext struct {
	appId           string
	producer        kafka.ProducerBuilder
	storeRegistry   stores.Registry
	admin           kafka.Admin
	logger          log.Logger
	metricsReporter metrics.Reporter
}

func (bc *builderContext) StoreRegistry() stores.Registry {
	return bc.storeRegistry
}

func (bc *builderContext) ProducerBuilder() kafka.ProducerBuilder {
	return bc.producer
}

func (bc *builderContext) Admin() kafka.Admin {
	return bc.admin
}

func (bc *builderContext) ApplicationId() string {
	return bc.appId
}

func (bc *builderContext) Logger() log.Logger {
	return bc.logger
}

func (bc *builderContext) MetricsReporter() metrics.Reporter {
	return bc.metricsReporter
}

func NewBuilderContext(
	appId string,
	registry stores.Registry,
	producer kafka.ProducerBuilder,
	kafkaAdmin kafka.Admin,
	logger log.Logger,
	metricsReporter metrics.Reporter,
) BuilderContext {
	return &builderContext{
		producer:        producer,
		storeRegistry:   registry,
		admin:           kafkaAdmin,
		appId:           appId,
		logger:          logger,
		metricsReporter: metricsReporter,
	}
}

type LoggableStoreBuilder interface {
	Name() string
	NameFormatter(ctx SubTopologyContext) StateStoreNameFunc
	KeyEncoder() encoding.Encoder
	ValEncoder() encoding.Encoder
	Build(ctx SubTopologyContext) (StateStore, error)
	Changelog() ChangelogSyncerBuilder
}

type ChangelogSyncerBuilder interface {
	// Setup setups the changelog by creating changelog topics and offset stores
	Setup(ctx SubTopologyBuilderContext) error
	Build(ctx SubTopologyContext, store stores.Store) (ChangelogSyncer, error)
	BuildLogger(ctx SubTopologyContext, store string) (ChangeLogger, error)
	Internal() bool
	Topic() string
}

type ChangelogSyncer interface {
	Sync(ctx context.Context, synced chan struct{}) error
	Stop() error
}

type StateStore interface {
	stores.Store
	// Flush flashes the records in buffer to stores.Store
	Flush() error
	ResetCache()
	ChangelogSyncer
}

type LoggableStateStore interface {
	StateStore
	ChangeLogger
}

type StateStoreProvider interface {
	Store(ctx SubTopologyContext) LoggableStateStore
}

type ChangeLogger interface {
	Log(ctx context.Context, key, value []byte) error
}

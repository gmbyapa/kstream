package kafka

import (
	"context"
	"fmt"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"time"
)

// TopicPartition represents a kafka topic partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string {
	return fmt.Sprintf(`%s-%d`, tp.Topic, tp.Partition)
}

type TopicMeta []TopicPartition

// GroupMeta wraps consumer group metadata used in transactional producer commits.
type GroupMeta struct {
	Meta interface{}
}

type GroupConsumerStatus string

const (
	ConsumerPending     GroupConsumerStatus = `Pending`
	ConsumerRebalancing GroupConsumerStatus = `Rebalancing`
	ConsumerReady       GroupConsumerStatus = `Ready`
)

// GroupConsumer is a wrapper for a kafka group consumer adaptor.
type GroupConsumer interface {
	// Subscribe subscribes to a list of topic with a user provided RebalanceHandler
	Subscribe(tps []string, handler RebalanceHandler) error
	// Unsubscribe signals the consumer to unsubscribe from group
	Unsubscribe() error
	Errors() <-chan error
}

type PartitionConsumer interface {
	ConsumeTopic(ctx context.Context, topic string, offset Offset) (map[int32]Partition, error)
	Partitions(ctx context.Context, topic string) ([]int32, error)
	ConsumePartition(ctx context.Context, topic string, partition int32, offset Offset) (Partition, error)
	OffsetValid(topic string, partition int32, offset int64) (isValid bool, err error)
	GetOffsetLatest(topic string, partition int32) (offset int64, err error)
	GetOffsetOldest(topic string, partition int32) (offset int64, err error)
	Close() error
}

type Partition interface {
	Events() <-chan Event
	BeginOffset() Offset
	EndOffset() Offset
	Close() error
}

type Offset int64

const (
	OffsetEarliest Offset = -2
	OffsetLatest   Offset = -1
	OffsetStored   Offset = -3
	OffsetUnknown  Offset = -4
)

func (o Offset) String() string {
	switch o {
	case -4:
		return `Unknown`
	case -3:
		return `Stored`
	case -2:
		return `Earliest`
	case -1:
		return `Latest`
	default:
		return fmt.Sprint(int(o))
	}
}

type IsolationLevel int8

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
)

// RecordContextBinderFunc binds a user given context to a consumer records
// When consuming this happens before the consumer record is sent to the per partition chan
type RecordContextBinderFunc func(record Record) context.Context

// GroupConsumerConfig defines configurations for high level Group Consumer
type GroupConsumerConfig struct {
	*ConsumerConfig
	GroupId string // Consumers Group ID
	Offsets struct {
		// Fallback offset then there no initial stored offset in kafka
		// This is equivalent to auto.offset.reset in java consumer
		Initial Offset
		Commit  struct {
			Auto     bool          // Equivalent to enable.auto.commit in java consumer
			Interval time.Duration // Equivalent to auto.commit.interval.ms  in java consumer
		}
	}
}

func (conf *GroupConsumerConfig) Copy() *GroupConsumerConfig {
	return &GroupConsumerConfig{
		ConsumerConfig: conf.ConsumerConfig.Copy(),
		GroupId:        conf.GroupId,
		Offsets:        conf.Offsets,
	}
}

type GroupConsumerProvider interface {
	NewBuilder(config *GroupConsumerConfig) GroupConsumerBuilder
}

type ConsumerProvider interface {
	NewBuilder(config *ConsumerConfig) ConsumerBuilder
}

type ProducerFactory interface {
	NewBuilder(config *GroupConsumerConfig) GroupConsumerBuilder
}

func NewConfig() *GroupConsumerConfig {
	conf := &GroupConsumerConfig{
		ConsumerConfig: NewPartitionConsumerConfig(),
	}

	conf.Offsets.Commit.Auto = true
	conf.Offsets.Commit.Interval = 5000 * time.Millisecond
	conf.Offsets.Initial = OffsetLatest

	return conf
}

func NewPartitionConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		IsolationLevel:          ReadCommitted,
		TopicMetaFetchTimeout:   10 * time.Second,
		EOSEnabled:              true,
		Logger:                  log.NewNoopLogger(),
		MetricsReporter:         metrics.NoopReporter(),
		MaxPollInterval:         1 * time.Second,
		ConsumerMessageChanSize: 1000,
	}
}

func NewOffsetManagerConfig() *OffsetManagerConfig {
	return &OffsetManagerConfig{
		Logger:          log.NewNoopLogger(),
		MetricsReporter: metrics.NoopReporter(),
	}
}

// ConsumerConfig defines configurations for simple(partition) Consumer
type ConsumerConfig struct {
	// Client ID of the consumer (client.id)
	Id string

	// List of kafka BootstrapServers
	BootstrapServers []string

	// Read Isolation level of the consumer. Supported values - ReadCommited(default), ReadUncommited
	IsolationLevel IsolationLevel

	// Topic meta fetch interval for admin operation such as fetching partition counts
	TopicMetaFetchTimeout time.Duration

	// Exactly Once Support. When this is enabled IsolationLevel will automatically set to ReadCommited
	EOSEnabled bool

	// MaxPollInterval represents the period of time the consumer waits before checking for new messages.
	MaxPollInterval time.Duration

	// ConsumerMessageChanSize defines the buffer size of the channels that receive messages for each partition.
	// When the consumer's Poll method is called, it fetches all eligible messages for the current consumer assignment.
	// These messages are then fanned out into per-partition channels, and this configuration sets the size of those
	// buffers.
	ConsumerMessageChanSize int

	Logger          log.Logger
	MetricsReporter metrics.Reporter

	ContextExtractor RecordContextBinderFunc
}

func (conf *ConsumerConfig) Copy() *ConsumerConfig {
	return &ConsumerConfig{
		Id:                      conf.Id,
		BootstrapServers:        conf.BootstrapServers,
		IsolationLevel:          conf.IsolationLevel,
		EOSEnabled:              conf.EOSEnabled,
		TopicMetaFetchTimeout:   conf.TopicMetaFetchTimeout,
		Logger:                  conf.Logger,
		MetricsReporter:         conf.MetricsReporter,
		ContextExtractor:        conf.ContextExtractor,
		MaxPollInterval:         conf.MaxPollInterval,
		ConsumerMessageChanSize: conf.ConsumerMessageChanSize,
	}
}

type OffsetManagerConfig struct {
	Id               string
	BootstrapServers []string

	Logger          log.Logger
	MetricsReporter metrics.Reporter
}

type OffsetManager interface {
	OffsetValid(topic string, partition int32, offset int64) (isValid bool, err error)
	GetOffsetLatest(topic string, partition int32) (offset int64, err error)
	GetOffsetOldest(topic string, partition int32) (offset int64, err error)
	Close() error
}

type GroupConsumerBuilder func(func(config *GroupConsumerConfig)) (GroupConsumer, error)

type ConsumerBuilder func(func(config *ConsumerConfig)) (PartitionConsumer, error)

type OffsetManagerBuilder func(func(config *OffsetManagerConfig)) (OffsetManager, error)

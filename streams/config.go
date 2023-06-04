/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package streams

import (
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/state_stores"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"time"
)

type KafkaVersion string

type ProcessingGuarantee int8

const (
	AtLeastOnce ProcessingGuarantee = iota
	ExactlyOnce
)

type Config struct {
	// ApplicationId will be used as
	// 	1 - a consumer group when using streams, and tables
	// 	2 - topic prefix when creating repartition topics
	// 	3 - transaction ID prefix when using producer transactions
	ApplicationId string
	// BootstrapServers a list of kafka Brokers
	BootstrapServers []string
	Store            struct {
		Http struct {
			// Enabled enable state stores http server(debug purposes only)
			Enabled bool
			// Host stores http server host(eg: http://localhost:8080)
			Host string
		}
		// StateDir directory to store Persistable state stores
		StateDir  string
		Changelog struct {
			// ReplicaCount store changelog topic(auto generated) replica count
			ReplicaCount int16
		}
	}
	// InternalTopicsDefaultReplicaCount default replica count for auto generated topic(eg: repartition topics)
	InternalTopicsDefaultReplicaCount int16
	Processing                        struct {
		// ConsumerCount number of stream consumers(default:1) to run. This can be used to scale application vertically
		ConsumerCount int
		// Guarantee end to end processing guarantee. Supported values are ExactlyOnce(default) and AtLeastOnce
		Guarantee ProcessingGuarantee
		// Buffer defines min time or min mum of records before the flush starts
		Buffer tasks.BufferConfig
		// FailedMessageHandler used to handle failed messages(Process failures and serialization errors)
		FailedMessageHandler tasks.FailedMessageHandler
	}
	// Consumer default consumer properties
	Consumer *kafka.GroupConsumerConfig
	// Host application host(used to identify application instances)
	Host string
	// Producer default producer configs
	Producer *kafka.ProducerConfig
	// MetricsReporter default metrics reporter(default: NoopReporter)
	MetricsReporter metrics.Reporter
	// MetricsReporter default logger(default: NoopLogger)
	Logger log.Logger

	// RepartitionTopicFormatter repartition topic name formatter function
	// (default ApplicationId-${storeName}-repartitioned)
	RepartitionTopicNameFormatter TopicNameFormatter

	// DefaultPartitioner if defined will be used in Sink operators. Eg: To, Repartition
	DefaultPartitioner Partitioner

	// ChangelogTopicNameFormatter changelog topic name formatter function
	// (default ApplicationId-${storeName}-store-changelog)
	ChangelogTopicNameFormatter state_stores.ChangelogTopicFormatter
}

func NewStreamBuilderConfig() *Config {
	config := &Config{}
	config.Consumer = kafka.NewConfig()
	config.Producer = kafka.NewProducerConfig()

	config.Processing.Guarantee = ExactlyOnce
	config.Processing.ConsumerCount = 1
	config.Processing.Buffer.FlushInterval = time.Second
	config.Processing.Buffer.Size = 90000
	config.Consumer.IsolationLevel = kafka.ReadCommitted
	config.Consumer.Offsets.Initial = kafka.OffsetLatest
	config.Producer.Acks = kafka.WaitForAll
	config.Producer.Idempotent = true
	config.Producer.Transactional.Enabled = true
	config.Consumer.Offsets.Commit.Auto = false
	config.Store.StateDir = `storage`

	config.Store.Changelog.ReplicaCount = 1
	config.InternalTopicsDefaultReplicaCount = 1

	config.RepartitionTopicNameFormatter = func(topic string) func(ctx topology.BuilderContext, nodeId topology.NodeId) string {
		return func(ctx topology.BuilderContext, nodeId topology.NodeId) string {
			return fmt.Sprintf(`%s-%s-repartition`, ctx.ApplicationId(), topic)
		}
	}

	config.ChangelogTopicNameFormatter = func(storeName string) func(ctx topology.BuilderContext) string {
		return func(ctx topology.BuilderContext) string {
			return fmt.Sprintf(`%s-%s-store-changelog`, ctx.ApplicationId(), storeName)
		}
	}

	// default metrics reporter
	config.MetricsReporter = metrics.NoopReporter()
	config.Logger = log.NewNoopLogger()

	return config
}

func (c *Config) setUp() {
	c.Consumer.GroupId = c.ApplicationId
	c.Consumer.BootstrapServers = c.BootstrapServers
	c.Consumer.Logger = c.Logger
	c.Consumer.MetricsReporter = c.MetricsReporter

	c.Producer.BootstrapServers = c.BootstrapServers
	c.Producer.Logger = c.Logger
	c.Producer.MetricsReporter = c.MetricsReporter

	if c.Processing.Guarantee == ExactlyOnce {
		c.Consumer.IsolationLevel = kafka.ReadCommitted
		c.Producer.Acks = kafka.WaitForAll
		c.Producer.Idempotent = true
		c.Producer.Transactional.Enabled = true
		c.Consumer.Offsets.Commit.Auto = false
	}

	if c.Processing.FailedMessageHandler == nil {
		c.Processing.FailedMessageHandler = func(err error, record kafka.Record) {
			c.Logger.ErrorContext(record.Ctx(), err.Error())
		}
	}
}

func (c *Config) validate() error {
	if c.ApplicationId == `` {
		return errors.New(`[ApplicationId] cannot be empty`)
	}

	if len(c.BootstrapServers) < 1 {
		return errors.New(`[BootstrapServers] cannot be empty`)
	}

	if c.Store.Changelog.ReplicaCount < 1 {
		return errors.New(`[Store.Changelog.ReplicaCount] needs to be greater than zero`)
	}

	if c.InternalTopicsDefaultReplicaCount < 1 {
		return errors.New(`[InternalTopicsDefaultReplicaCount] needs to be greater than zero`)
	}

	if c.Processing.Guarantee == ExactlyOnce {
		if c.Consumer.IsolationLevel != kafka.ReadCommitted {
			return errors.New(`[GroupConsumer.IsolationLevel] needs to be ReadCommitted 
					when Processing.Guarantee == ExactlyOnce`)
		}

		if c.Producer.Acks != kafka.WaitForAll {
			return errors.New(`[Producer.Acks] needs to be WaitForAll 
					when Processing.Guarantee == ExactlyOnce`)
		}

		if !c.Producer.Idempotent {
			return errors.New(`[Producer.Idempotent] needs to be true 
					when Processing.Guarantee == ExactlyOnce`)
		}

		if !c.Producer.Transactional.Enabled {
			return errors.New(`[Producer.Transactional.Enabled] needs to be true 
					when Processing.Guarantee == ExactlyOnce`)
		}

		if c.Consumer.Offsets.Commit.Auto {
			return errors.New(`[GroupConsumer.Offsets.Commit.Auto] needs to be false 
					when Processing.Guarantee == ExactlyOnce`)
		}
	}

	if c.Store.StateDir == `` {
		return errors.New(`[Store.StateDir] cannot be empty`)
	}

	return nil
}

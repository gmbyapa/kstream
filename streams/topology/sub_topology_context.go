package topology

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/tryfix/log"
)

type subTopologyContext struct {
	partition int32
	BuilderContext
	context.Context
	partitionConsumer kafka.PartitionConsumer
	producer          kafka.Producer
	logger            log.Logger
	subTopology       SubTopology
	topicMeta         map[string]*kafka.Topic
}

//func (t *subTopologyContext) Assignment() kafka.Assignment {
//	panic("implement me")
//}

func (t *subTopologyContext) Producer() kafka.Producer {
	return t.producer
}

func (t *subTopologyContext) Logger() log.Logger {
	return t.logger
}

func (t *subTopologyContext) Partition() int32 {
	return t.partition
}

func (t *subTopologyContext) TopicMeta() map[string]*kafka.Topic {
	return t.topicMeta
}

func (t *subTopologyContext) PartitionConsumer() kafka.PartitionConsumer {
	return t.partitionConsumer
}

func (t *subTopologyContext) Store(name string) StateStore {
	return t.subTopology.Store(name)
}

func NewSubTopologyContext(
	parent context.Context,
	partition int32,
	builderCtx BuilderContext,
	producer kafka.Producer,
	partitionConsumer kafka.PartitionConsumer,
	logger log.Logger,
	topicMeta map[string]*kafka.Topic,
) SubTopologyContext {
	return &subTopologyContext{
		partition:         partition,
		BuilderContext:    builderCtx,
		Context:           parent,
		producer:          producer,
		partitionConsumer: partitionConsumer,
		logger:            logger,
		topicMeta:         topicMeta,
	}
}

type SubTopologyContext interface {
	BuilderContext
	context.Context
	PartitionConsumer() kafka.PartitionConsumer
	Partition() int32
	Producer() kafka.Producer
	Logger() log.Logger
	TopicMeta() map[string]*kafka.Topic
}

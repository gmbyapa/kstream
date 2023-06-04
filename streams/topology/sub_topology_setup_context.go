package topology

import (
	"github.com/gmbyapa/kstream/kafka"
)

type subTopologySetupContext struct {
	BuilderContext
	partitionCount int32
	topicMeta      map[string]*kafka.TopicConfig
}

func (t *subTopologySetupContext) MaxPartitionCount() int32 {
	return t.partitionCount
}

func (t *subTopologySetupContext) TopicMeta() map[string]*kafka.TopicConfig {
	return t.topicMeta
}

func NewSubTopologySetupContext(
	builderCtx BuilderContext,
	topicMeta map[string]*kafka.TopicConfig,
	maxPartitions int32,
) SubTopologySetupContext {
	return &subTopologySetupContext{
		BuilderContext: builderCtx,
		topicMeta:      topicMeta,
		partitionCount: maxPartitions,
	}
}

type SubTopologySetupContext interface {
	BuilderContext
	MaxPartitionCount() int32
	TopicMeta() map[string]*kafka.TopicConfig
}

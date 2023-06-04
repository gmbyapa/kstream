package streams

import "github.com/gmbyapa/kstream/streams/topology"

type AutoTopicOpts struct {
	nameFormatter  TopicNameFormatter
	partitionAs    topology.Source
	partitionCount int32
	replicaCount   int16
	configEntries  map[string]string
}

func (rpOpts *AutoTopicOpts) apply(opts ...TopicOpt) {
	for _, opt := range opts {
		opt(rpOpts)
	}
}

type TopicOpt func(tpOpts *AutoTopicOpts)

func PartitionAs(src topology.Source) TopicOpt {
	return func(rpOpts *AutoTopicOpts) {
		rpOpts.partitionAs = src
	}
}

func WithPartitionCount(count int32) TopicOpt {
	return func(rpOpts *AutoTopicOpts) {
		rpOpts.partitionCount = count
	}
}

func WithReplicaCount(count int16) TopicOpt {
	return func(rpOpts *AutoTopicOpts) {
		rpOpts.replicaCount = count
	}
}

func WithTopicConfigs(configs map[string]string) TopicOpt {
	return func(rpOpts *AutoTopicOpts) {
		rpOpts.configEntries = configs
	}
}

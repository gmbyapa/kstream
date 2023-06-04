package streams

import (
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/topology"
)

type RepartitionOpts struct {
	topicName          string
	topicNameFormatter TopicNameFormatter
	dueTo              struct {
		node   topology.NodeBuilder
		source topology.Source
	}
	sourceOpts []KSourceOption
	sinkOpts   []KSinkOption
}

func (rpOpts *RepartitionOpts) apply(opts ...RepartitionOpt) {
	for _, opt := range opts {
		opt(rpOpts)
	}
}

type RepartitionOpt func(rpOpts *RepartitionOpts)

func RePartitionAs(topic string) RepartitionOpt {
	return func(rpOpts *RepartitionOpts) {
		rpOpts.topicName = topic
	}
}

func RePartitionWithKeyEncoder(enc encoding.Encoder) RepartitionOpt {
	return func(rpOpts *RepartitionOpts) {
		rpOpts.sinkOpts = append(rpOpts.sinkOpts, ProduceWithKeyEncoder(enc))
		rpOpts.sourceOpts = append(rpOpts.sourceOpts, ConsumeWithKeyEncoder(enc))
	}
}

func RePartitionWithValEncoder(enc encoding.Encoder) RepartitionOpt {
	return func(rpOpts *RepartitionOpts) {
		rpOpts.sinkOpts = append(rpOpts.sinkOpts, ProduceWithValEncoder(enc))
		rpOpts.sourceOpts = append(rpOpts.sourceOpts, ConsumeWithValEncoder(enc))
	}
}

func RePartitionWithPartitioner(partitioner Partitioner) RepartitionOpt {
	return func(rpOpts *RepartitionOpts) {
		rpOpts.sinkOpts = append(rpOpts.sinkOpts, ProduceWithPartitioner(partitioner))
	}
}

func rePartitionedDueTo(node topology.NodeBuilder, source topology.Source) RepartitionOpt {
	return func(rpOpts *RepartitionOpts) {
		rpOpts.dueTo.node = node
		rpOpts.dueTo.source = source
	}
}

func rePartitionedWithSourceOpts(opts ...KSourceOption) RepartitionOpt {
	return func(rpOpts *RepartitionOpts) {
		rpOpts.sourceOpts = append(rpOpts.sourceOpts, opts...)
	}
}

func RePartitionWithTopicNameFormatter(formatter TopicNameFormatter) RepartitionOpt {
	return func(rpOpts *RepartitionOpts) {
		rpOpts.topicNameFormatter = formatter
	}
}

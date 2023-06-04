package streams

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/encoding"
	"github.com/gmbyapa/kstream/streams/topology"
)

// SourceCtxParamExtractor extracts a key:val pair from a record. Used to bind key:val pairs into the record Context.
type SourceCtxParamExtractor func(record kafka.Record) (key string, val interface{})

// KSourceOption is used to customize the KSource
type KSourceOption func(*KSource)

// ConsumeWithContextParamExtractors adds a list of SourceCtxParamBinder func to the topology.Source
// and each binder will be called in a loop for each record
func ConsumeWithContextParamExtractors(fn ...SourceCtxParamExtractor) KSourceOption {
	return func(source *KSource) {
		source.sourceCtxParamBinders = fn
	}
}

func ConsumeWithAutoTopicCreateEnabled(options ...TopicOpt) KSourceOption {
	return func(source *KSource) {
		source.autoCreate.enabled = true
		for _, opt := range options {
			opt(source.autoCreate.AutoTopicOpts)
		}
	}
}

func ConsumeWithTopicNameFormatterFunc(fn TopicNameFormatter) KSourceOption {
	return func(source *KSource) {
		source.topicNameFormatter = fn
	}
}

func ConsumeWithOffset(offset kafka.Offset) KSourceOption {
	return func(source *KSource) {
		source.offsetReset = offset
	}
}

func ConsumeWithKeyEncoder(encoder encoding.Encoder) KSourceOption {
	return func(source *KSource) {
		source.encoder.Key = encoder
	}
}

func ConsumeWithValEncoder(encoder encoding.Encoder) KSourceOption {
	return func(source *KSource) {
		source.encoder.Value = encoder
	}
}

func markAsInternal() KSourceOption {
	return func(source *KSource) {
		source.internal = true
	}
}

func markAsGlobal() KSourceOption {
	return func(source *KSource) {
		source.global = true
	}
}

type KSource struct {
	encoder               topology.SourceEncoder
	topic                 string
	offsetReset           kafka.Offset
	coPartitionedWith     topology.Source
	sourceCtxParamBinders []SourceCtxParamExtractor
	topicConfigs          struct {
		name    string
		configs kafka.TopicConfig
	}
	autoCreate struct {
		enabled bool
		*AutoTopicOpts
	}
	internal           bool
	global             bool
	topicNameFormatter TopicNameFormatter
	topology.DefaultNode
}

func NewKSource(topic string, opts ...KSourceOption) topology.Source {
	src := &KSource{
		topic: topic,
	}

	src.autoCreate.AutoTopicOpts = new(AutoTopicOpts)
	src.offsetReset = kafka.OffsetStored

	// apply options
	for _, opt := range opts {
		opt(src)
	}

	return src
}

func (s *KSource) Type() topology.Type {
	return topology.Type{
		Name: `kSource`,
		Attrs: map[string]string{
			`topic`: s.topic,
		},
	}
}

func (s *KSource) Build(_ topology.SubTopologyContext) (topology.Node, error) {
	// validate mandatory options
	if s.encoder.Key == nil {
		return nil, s.Err(`key encoder not provided`)
	}

	if s.encoder.Value == nil {
		return nil, s.Err(`val encoder not provided`)
	}

	return &KSource{
		encoder:               s.encoder,
		topic:                 s.topic,
		offsetReset:           s.offsetReset,
		coPartitionedWith:     s.coPartitionedWith,
		sourceCtxParamBinders: s.sourceCtxParamBinders,
		autoCreate:            s.autoCreate,
		internal:              s.internal,
		topicNameFormatter:    s.topicNameFormatter,
		DefaultNode:           s.DefaultNode,
	}, nil
}

func (s *KSource) Setup(ctx topology.SubTopologySetupContext) error {
	if s.global {
		return nil
	}

	if s.autoCreate.enabled {
		if s.topicNameFormatter != nil {
			s.topic = s.topicNameFormatter(s.topic)(ctx, s.Id())
		}

		numOfPartitions := ctx.MaxPartitionCount()

		s.topicConfigs.name = s.topic
		s.topicConfigs.configs.NumPartitions = numOfPartitions
		s.topicConfigs.configs.ReplicationFactor = s.autoCreate.AutoTopicOpts.replicaCount
		s.topicConfigs.configs.ConfigEntries = s.autoCreate.AutoTopicOpts.configEntries

		topic := &kafka.Topic{
			Name:              s.topicConfigs.name,
			NumPartitions:     s.topicConfigs.configs.NumPartitions,
			ReplicationFactor: s.topicConfigs.configs.ReplicationFactor,
			ConfigEntries:     s.topicConfigs.configs.ConfigEntries,
		}

		// The topology only have auto create topics. looking for
		// autoCreateOptions.partitionedAs to get the number of
		// partitions form the parent
		if ctx.MaxPartitionCount() < 1 && s.autoCreate.AutoTopicOpts.partitionAs != nil {
			topic.NumPartitions = s.autoCreate.AutoTopicOpts.partitionAs.TopicConfigs().NumPartitions
		}

		if err := ctx.Admin().StoreConfigs([]*kafka.Topic{topic}); err != nil {
			return s.WrapErr(err)
		}

		return nil
	}

	s.topicConfigs.name = s.topic

	meta, ok := ctx.TopicMeta()[s.topic]
	if !ok {
		return errors.New(fmt.Sprintf(`metadata unavailable for topic %s`, s.topic))
	}

	s.topicConfigs.configs.NumPartitions = meta.NumPartitions

	return nil
}

func (s *KSource) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	k, err := s.encoder.Key.Decode(kIn.([]byte))
	if err != nil {
		return nil, nil, false, encoding.Err{Err: s.WrapErrWith(err, `key decode error`)}
	}

	if s.sourceCtxParamBinders != nil {
		for _, bind := range s.sourceCtxParamBinders {
			ctxK, ctxV := bind(topology.RecordFromContext(ctx))
			ctx = context.WithValue(ctx, ctxK, ctxV)
		}
	}

	if len(vIn.([]byte)) < 1 {
		return s.Forward(ctx, k, nil, true)
	}

	v, err := s.encoder.Value.Decode(vIn.([]byte))
	if err != nil {
		return nil, nil, false, encoding.Err{Err: s.WrapErrWith(err, `value decode error`)}
	}

	return s.Forward(ctx, k, v, true)
}

func (s *KSource) Encoder() topology.SourceEncoder {
	return s.encoder
}

func (s *KSource) Topic() string {
	return s.topic
}

func (s *KSource) ShouldCoPartitionedWith(source topology.Source) {
	s.coPartitionedWith = source
}

func (s *KSource) CoPartitionedWith() topology.Source {
	return s.coPartitionedWith
}

func (s *KSource) RePartitionedAs() topology.Source {
	if s.autoCreate.partitionAs == nil {
		return nil
	}

	return s.autoCreate.partitionAs
}

func (s *KSource) AutoCreate() bool {
	return s.autoCreate.enabled
}

func (s *KSource) Internal() bool {
	return s.autoCreate.enabled
}

func (s *KSource) TopicConfigs() kafka.TopicConfig {
	return s.topicConfigs.configs
}

func (s *KSource) InitialOffset() kafka.Offset {
	return s.offsetReset
}

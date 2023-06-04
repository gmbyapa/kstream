package streams

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"time"
)

type HeaderExtractor func(ctx context.Context, key, val interface{}) kafka.RecordHeaders

type Tombstoner func(ctx context.Context, key, val interface{}) (tombstone bool)

type kSink struct {
	topology.DefaultNode
	encoders topology.SinkEncoder
	topic    struct {
		name            string
		numOfPartitions int32
	}
	partitioner Partitioner

	producer kafka.Producer

	logger          log.Logger
	tombstoner      Tombstoner
	headerExtractor HeaderExtractor
}

func (s *kSink) Type() topology.Type {
	partitioner := `default`
	if s.partitioner != nil {
		partitioner = `custom`
	}
	return topology.Type{
		Name: "sink",
		Attrs: map[string]string{
			`topic`:       s.topic.name,
			`partitioner`: partitioner,
		},
	}
}

func (s *kSink) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	var recordTimestamp = time.Now()
	var recordTopic = s.topic.name
	var recordPartition int32 = kafka.PartitionAny // Partition will be decided based on producers configured partitioner
	var recordValue []byte

	// apply custom headers
	headers := s.headerExtractor(ctx, kIn, vIn)

	if s.partitioner != nil {
		// TODO NOTE: this logic can cause a problem if we want to implement a dynamic topic logic
		pt, err := s.partitioner(ctx, kIn, vIn, s.topic.numOfPartitions)
		if err != nil {
			return nil, nil, false, s.WrapErrWith(err, `record partitioner error`)
		}

		recordPartition = pt
	}

	keyByt, err := s.encoders.Key.Encode(kIn)
	if err != nil {
		return nil, nil, false, s.WrapErrWith(err, `sink key encode error`)
	}

	// If the record value is null or marked as a tombstone, tombstoner set the record value as null
	tombstoned := s.tombstoner(ctx, kIn, vIn)
	if vIn == nil || tombstoned {
		recordValue = nil
	} else {
		valByt, err := s.encoders.Value.Encode(vIn)
		if err != nil {
			return nil, nil, false, s.WrapErrWith(err, `sink value encode error`)
		}
		recordValue = valByt
	}

	record := s.producer.NewRecord(ctx, keyByt, recordValue, recordTopic, recordPartition, recordTimestamp, headers, ``)
	if txProducer, ok := s.producer.(kafka.TransactionalProducer); ok {
		if err := txProducer.ProduceAsync(ctx, record); err != nil {
			return nil, nil, false, s.WrapErrWith(err, `record produce failed`)
		}

		return s.Ignore()
	}

	if _, _, err := s.producer.ProduceSync(ctx, record); err != nil {
		return nil, nil, false, s.WrapErrWith(err, `record produce failed`)
	}

	// Sink is the last node of the pipeline
	return s.Ignore()
}

func (s *kSink) AddEdge(_ topology.Node) {
	panic(`sink does not support edges`)
}

func (s *kSink) Encoder() topology.SinkEncoder {
	return s.encoders
}

func (s *kSink) Topic() string {
	return s.topic.name
}

func (s *kSink) Close() error {
	return nil
}

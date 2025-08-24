package librd

import (
	"context"
	librdKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/pkg/errors"
)

type groupSession struct {
	assignment       kafka.Assignment
	consumer         *librdKafka.Consumer
	metaFetchTimeout int
}

func (g *groupSession) Assignment() kafka.Assignment {
	return g.assignment
}

func (g *groupSession) GroupMeta() (*kafka.GroupMeta, error) {
	mta, err := g.consumer.GetConsumerGroupMetadata()
	if err != nil {
		return nil, errors.Wrap(err, `groupMeta fetch failed`)
	}

	return &kafka.GroupMeta{Meta: mta}, nil
}

func (g *groupSession) TopicMeta() (kafka.TopicMeta, error) {
	var meta kafka.TopicMeta
	mta, err := g.consumer.GetMetadata(nil, false, g.metaFetchTimeout)
	if err != nil {
		return nil, errors.Wrap(err, `topicMeta fetch failed`)
	}

	for _, tp := range mta.Topics {
		if tp.Error.Code() != librdKafka.ErrNoError {
			return nil, errors.Wrapf(tp.Error, `topicMeta fetch failed for topic [%s]`, tp.Topic)
		}

		for _, pt := range tp.Partitions {
			if pt.Error.Code() != librdKafka.ErrNoError {
				return nil, errors.Wrapf(tp.Error, `topicMeta fetch failed. partition error [%s][%d]`, tp.Topic, pt.ID)
			}
			meta = append(meta, kafka.TopicPartition{
				Topic:     tp.Topic,
				Partition: pt.ID,
			})
		}
	}

	return meta, nil
}

func (g *groupSession) MarkOffset(_ context.Context, record kafka.Record, meta string) error {
	tp := record.Topic()
	_, err := g.consumer.StoreOffsets([]librdKafka.TopicPartition{
		{Topic: &tp, Partition: record.Partition(), Offset: librdKafka.Offset(record.Offset() + 1), Metadata: &meta},
	})

	return errors.Wrapf(err, `failed to mark offset for %s[%d]@%d`, tp, record.Partition(), record.Offset())
}

func (g *groupSession) CommitOffset(_ context.Context, record kafka.Record, meta string) error {
	tp := record.Topic()
	_, err := g.consumer.CommitOffsets([]librdKafka.TopicPartition{
		{Topic: &tp, Partition: record.Partition(), Offset: librdKafka.Offset(record.Offset() + 1), Metadata: &meta},
	})

	return errors.Wrapf(err, `failed to commit offset for %s[%d]@%d`, tp, record.Partition(), record.Offset())
}

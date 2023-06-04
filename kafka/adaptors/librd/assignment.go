package librd

import (
	librdKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
)

type assignment struct {
	resets map[kafka.TopicPartition]kafka.Offset
	tps    []librdKafka.TopicPartition
}

func toLibrd(o kafka.Offset) string {
	switch o {
	case kafka.OffsetEarliest:
		return librdKafka.OffsetBeginning.String()
	case kafka.OffsetLatest:
		return librdKafka.OffsetEnd.String()
	case kafka.OffsetStored:
		return librdKafka.OffsetStored.String()
	default:
		return librdKafka.OffsetInvalid.String()
	}
}

func (a *assignment) TPs() kafka.TopicPartitions {
	var consumerTps []kafka.TopicPartition
	for _, tp := range a.tps {
		consumerTps = append(consumerTps, kafka.TopicPartition{
			Topic:     *tp.Topic,
			Partition: tp.Partition,
		})
	}

	return consumerTps
}

func (a *assignment) ResetOffset(tp kafka.TopicPartition, offset kafka.Offset) {
	a.resets[tp] = offset
}

func newAssignment(tps []librdKafka.TopicPartition) *assignment {
	asign := &assignment{
		resets: map[kafka.TopicPartition]kafka.Offset{},
		tps:    tps,
	}

	return asign
}

func (a *assignment) toLibrd() ([]librdKafka.TopicPartition, error) {
	// Apply offset resets
	for i := range a.tps {
		tp := a.tps[i]
		if offset, ok := a.resets[kafka.TopicPartition{Topic: *tp.Topic, Partition: tp.Partition}]; ok && offset != kafka.OffsetStored {
			librdOffset, err := librdKafka.NewOffset(toLibrd(offset))
			if err != nil {
				return nil, errors.Wrap(err, `Librd Offset set failed`)
			}

			tp.Offset = librdOffset
			a.tps[i] = tp
		}
	}

	return a.tps, nil
}

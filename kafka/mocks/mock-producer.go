package mocks

import (
	"context"
	librd "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
	"hash"
	"hash/fnv"
)

type MockStreamProducer struct {
	hasher hash.Hash32
	topics *Topics
	libed  *librd.Producer
}

func NewMockProducer(topics *Topics) *MockStreamProducer {
	return &MockStreamProducer{
		hasher: fnv.New32a(),
		topics: topics,
		//libed: librd.NewMockCluster(),
	}
}

func (msp *MockStreamProducer) ProduceSync(ctx context.Context, message kafka.Record) (partition int32, offset int64, err error) {
	msp.hasher.Reset()
	_, err = msp.hasher.Write(message.Key())
	if err != nil {
		return partition, offset, err
	}

	//topic, err := msp.topics.Topic(message.Topic())
	//if err != nil {
	//	return partition, offset, err
	//}

	//p := int64(msp.hasher.Sum32()) % int64(len(topic.Partitions()))
	//pt, err := topic.Partition(int(p))
	//if err != nil {
	//	return
	//}

	//message.Partition = int32(p)
	//if err = pt.Append(message); err != nil {
	//	return
	//}
	//
	//return int32(p), message.Offset(), nil

	return 0, 0, err
}

func (msp *MockStreamProducer) ProduceBatch(ctx context.Context, messages []kafka.Record) error {
	for _, msg := range messages {
		if _, _, err := msp.ProduceSync(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (msp *MockStreamProducer) Close() error {
	return nil
}

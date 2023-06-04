package mocks

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"hash"
	"hash/fnv"
)

type MockPartitionConsumer struct {
	hasher hash.Hash32
	topics *Topics
}

func (m *MockPartitionConsumer) ConsumeTopic(ctx context.Context, topic string, offset kafka.Offset) (map[int32]kafka.Partition, error) {
	panic("implement me")
}

func (m *MockPartitionConsumer) Partitions(ctx context.Context, topic string) ([]int32, error) {
	panic("implement me")
}

func (m *MockPartitionConsumer) ConsumePartition(ctx context.Context, topic string, partition int32, offset kafka.Offset) (kafka.Partition, error) {
	panic("implement me")
}

func (m *MockPartitionConsumer) OffsetValid(topic string, partition int32, offset int64) (isValid bool, err error) {
	panic("implement me")
}

func (m *MockPartitionConsumer) GetOffsetLatest(topic string, partition int32) (offset int64, err error) {
	panic("implement me")
}

func (m *MockPartitionConsumer) GetOffsetOldest(topic string, partition int32) (offset int64, err error) {
	panic("implement me")
}

func (m *MockPartitionConsumer) Close() error {
	panic("implement me")
}

func NewMockPartitionConsumer(topics *Topics) *MockStreamProducer {
	return &MockStreamProducer{
		hasher: fnv.New32a(),
		topics: topics,
	}
}

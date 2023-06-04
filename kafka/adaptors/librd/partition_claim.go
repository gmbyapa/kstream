package librd

import (
	"github.com/gmbyapa/kstream/kafka"
)

type partitionClaim struct {
	tp       kafka.TopicPartition
	messages chan kafka.Record
}

func (t *partitionClaim) TopicPartition() kafka.TopicPartition {
	return t.tp
}

func (t *partitionClaim) Records() <-chan kafka.Record {
	return t.messages
}

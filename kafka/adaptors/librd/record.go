package librd

import (
	"context"
	"fmt"
	librdKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
	"time"
)

type Record struct {
	ctx   context.Context
	librd *librdKafka.Message
}

func (r *Record) Key() []byte {
	return r.librd.Key
}

func (r *Record) Value() []byte {
	return r.librd.Value
}

func (r *Record) Topic() string {
	return *r.librd.TopicPartition.Topic
}

func (r *Record) Partition() int32 {
	return r.librd.TopicPartition.Partition
}

func (r *Record) Offset() int64 {
	return int64(r.librd.TopicPartition.Offset)
}

func (r *Record) Timestamp() time.Time {
	return r.librd.Timestamp
}

func (r *Record) Headers() kafka.RecordHeaders {
	headers := make([]kafka.RecordHeader, len(r.librd.Headers))
	for i, h := range r.librd.Headers {
		headers[i] = kafka.RecordHeader{
			Key:   []byte(h.Key),
			Value: h.Value,
		}
	}

	return headers
}

func (r *Record) Ctx() context.Context {
	return r.ctx
}

func (r *Record) String() string {
	return fmt.Sprint(fmt.Sprintf(`%s[%d]@%d`, r.Topic(), r.Partition(), r.Offset()))
}

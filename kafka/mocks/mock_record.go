package mocks

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"time"
)

type Record struct {
	MCtx       context.Context
	MTopic     string
	MPartition int32
	MOffset    int64
	MValue     []byte
	MKey       []byte
	MTimestamp time.Time
	MHeaders   []kafka.RecordHeader
}

func (r *Record) Key() []byte {
	return r.MKey
}

func (r *Record) Value() []byte {
	return r.MValue
}

func (r *Record) Topic() string {
	return r.MTopic
}

func (r *Record) Partition() int32 {
	return r.MPartition
}

func (r *Record) Offset() int64 {
	return r.MOffset
}

func (r *Record) Timestamp() time.Time {
	return r.MTimestamp
}

func (r *Record) Headers() kafka.RecordHeaders {
	return r.MHeaders
}

func (r *Record) Ctx() context.Context {
	return r.MCtx
}

func (r *Record) String() string {
	return fmt.Sprint(fmt.Sprintf(`%s[%d]@%d`, r.Topic(), r.Partition(), r.Offset()))
}

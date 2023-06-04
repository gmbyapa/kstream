package kafka

import (
	"bytes"
	"context"
	"time"
)

type RecordMeta struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Headers   RecordHeaders
}

type Record interface {
	Ctx() context.Context
	Key() []byte
	Value() []byte
	Topic() string
	Partition() int32
	Offset() int64
	Timestamp() time.Time
	Headers() RecordHeaders
	String() string
}

// RecordHeader stores key and value for a record header.
type RecordHeader struct {
	Key   []byte
	Value []byte
}

// RecordHeaders are list of key:value pairs.
type RecordHeaders []RecordHeader

// Read returns a RecordHeader by its name or nil if not exist
func (h RecordHeaders) Read(key []byte) []byte {
	for _, header := range h {
		if bytes.Equal(header.Key, key) {
			return header.Value
		}
	}

	return nil
}

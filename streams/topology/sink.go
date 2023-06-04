package topology

import (
	"github.com/gmbyapa/kstream/v2/streams/encoding"
)

type SinkEncoder struct {
	Key, Value encoding.Encoder
}

type Sink interface {
	Node
	Encoder() SinkEncoder
	Topic() string

	// Close closes the source buffers
	Close() error
}

type SinkBuilder interface {
	NodeBuilder
	Topic() string
	AutoCreate() bool
}

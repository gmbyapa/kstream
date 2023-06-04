package tasks

import (
	"github.com/gmbyapa/kstream/kafka"
)

type Record struct {
	kafka.Record
	ignore bool
}

func NewTaskRecord(record kafka.Record) *Record {
	return &Record{
		Record: record,
		ignore: false,
	}
}

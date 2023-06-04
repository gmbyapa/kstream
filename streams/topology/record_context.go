/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package topology

import (
	"context"

	"github.com/gmbyapa/kstream/kafka"
)

type RecodeContext interface {
	context.Context
}

var val struct{}

func NewRecordContext(record kafka.Record) context.Context {
	return context.WithValue(record.Ctx(), &val, record)
}

func RecordFromContext(ctx context.Context) kafka.Record {
	return ctx.Value(&val).(kafka.Record)
}

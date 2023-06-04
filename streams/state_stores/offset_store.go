package state_stores

import (
	"context"

	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/stores"
)

type OffsetStore interface {
	Commit(ctx context.Context, tp kafka.TopicPartition, offset kafka.Offset) error
	Committed(tp kafka.TopicPartition) (kafka.Offset, error)
}

type offsetStore struct {
	stores.Store
}

func newOffsetStore(store stores.Store) OffsetStore {
	return &offsetStore{Store: store}
}

func (str *offsetStore) Commit(ctx context.Context, tp kafka.TopicPartition, offset kafka.Offset) error {
	return str.Set(ctx, tp.String(), int(offset), 0)
}

func (str *offsetStore) Committed(tp kafka.TopicPartition) (kafka.Offset, error) {
	if !str.Store.Backend().Persistent() {
		return kafka.OffsetEarliest, nil
	}

	v, err := str.Get(context.Background(), tp.String())
	if err != nil {
		return kafka.OffsetUnknown, errors.Wrap(err, `committed offset fetch failed`)
	}

	if v == nil {
		return kafka.OffsetEarliest, nil
	}

	return kafka.Offset(v.(int)), err
}

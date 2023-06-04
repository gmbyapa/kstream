package state_stores

import (
	"context"
	"fmt"
	"time"

	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/topology"
)

type loggableStateStoreInstance struct {
	*StateStore
	topology.ChangeLogger
}

func (str *loggableStateStoreInstance) Set(ctx context.Context, key, value interface{}, expiry time.Duration) error {
	keyByt, err := str.KeyEncoder().Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode error`, str))
	}

	valByt, err := str.ValEncoder().Encode(value)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] value encode err `, str))
	}

	if err := str.Log(ctx, keyByt, valByt); err != nil {
		return errors.Wrapf(err, `store [%s] changelog write failed`, str)
	}

	if err := str.cache.Backend().Set(keyByt, valByt, 0); err != nil {
		return errors.Wrapf(err, `store %s cache backend write failed`, str.Store)
	}

	return nil
}

func (str *loggableStateStoreInstance) Delete(ctx context.Context, key interface{}) error {
	keyByt, err := str.KeyEncoder().Encode(key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`store [%s] key encode error`, str))
	}

	if err := str.Log(ctx, keyByt, nil); err != nil {
		return errors.Wrapf(err, `store [%s] changelog write failed`, str)
	}

	if err := str.cache.Backend().Delete(keyByt); err != nil {
		return errors.Wrapf(err, `store %s cache backend delete failed`, str.Store)
	}

	return nil
}

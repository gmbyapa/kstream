package stores

import (
	"context"
	"github.com/gmbyapa/kstream/pkg/errors"
)

func UpdateIndexes(ctx context.Context, store IndexedStore, key, val interface{}) error {
	valPrv, err := store.Get(ctx, key)
	if err != nil {
		return errors.Wrapf(err, `cannot fetch value, key:%v, store:%s`, key, store.Name())
	}

	for _, idx := range store.Indexes() {
		if err := updateIndex(ctx, idx, key, valPrv, val); err != nil {
			return errors.Wrapf(err, `index update failed. Index: %s`, idx)
		}
	}

	return nil
}

func DeleteIndexes(ctx context.Context, store IndexedStore, key interface{}) error {
	val, err := store.Get(ctx, key)
	if err != nil {
		return errors.Wrapf(err, `indexed value delete failed due to record fetch error`)
	}

	if val != nil {
		for _, idx := range store.Indexes() {
			if err := idx.Delete(key, val); err != nil {
				return errors.Wrapf(err, `indexed value delete error`)
			}
		}
	}

	return nil
}

func updateIndex(_ context.Context, idx Index, key interface{}, valPrv, val interface{}) error {
	idx.Lock()
	defer idx.Unlock()

	if valPrv != nil {
		// Check indexedKey value has changed in the value
		// eg: val.foreignKey=foo -> val.foreignKey=bar then delete it from foo index and add it to bar

		// Compare if prefix has changed. otherwise just return it. nothing has changed
		equal, err := idx.Compare(key, valPrv, val)
		if err != nil {
			return errors.Wrapf(err, `indexed value compare failed, key:%v, index:%s`, key, idx)
		}

		if equal {
			return nil
		}

		// Prefix is different lets delete the key from old index and add it to new
		if err := idx.Delete(key, valPrv); err != nil {
			return errors.Wrapf(err, `index delete failed, key:%v, index:%s`, key, idx)
		}
	}

	if err := idx.Write(key, val); err != nil {
		return errors.Wrapf(err, `index write failed, key:%v, index:%s`, key, idx)
	}

	return nil
}

package state_stores

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/stores"
	logger "github.com/tryfix/log"
)

type ChangelogStatus string

type changelogSyncer struct {
	tp          kafka.TopicPartition
	offsetStore OffsetStore
	consumer    kafka.PartitionConsumer
	logger      logger.Logger
	store       stores.Store
	stopping    chan struct{}
	running     chan struct{}

	mu *sync.Mutex
}

func (lg *changelogSyncer) Sync(ctx context.Context, synced chan struct{}) error {
	offset, err := lg.offsetStore.Committed(lg.tp)
	if err != nil {
		return errors.Wrap(err, `cannot get committed offset`)
	}

	lg.logger.Info(fmt.Sprintf(`Offset %s found for %s`, offset, lg.tp))

	partition, err := lg.consumer.ConsumePartition(ctx, lg.tp.Topic, lg.tp.Partition, offset)
	if err != nil {
		return errors.Wrapf(err, `partition consume failed on %s`, lg.tp)
	}

	return lg.startSync(ctx, partition, synced)
}

func (lg *changelogSyncer) startSync(ctx context.Context, partition kafka.Partition, synced chan struct{}) error {
	ticker := time.NewTicker(1 * time.Second)
	endOnce := sync.Once{}

	lg.logger.Info(`Syncing...`)

	defer endOnce.Do(func() {
		ticker.Stop()
	})

	defer func() {
		if err := partition.Close(); err != nil {
			lg.logger.Warn(`Partition consumer close failed`)
		} else {
			lg.logger.Info(`Consumer loop closed`)
		}

		close(lg.running)
		close(synced)
	}()

	var syncedCount int64
	go func(tic *time.Ticker) {
		for range tic.C {
			if partition.EndOffset() < 1 {
				continue
			}
			lg.logger.Info(
				fmt.Sprintf(
					`Sync progress - [%d]%% done (%d/%d)`,
					atomic.LoadInt64(&syncedCount)*100/int64(partition.EndOffset()),
					atomic.LoadInt64(&syncedCount), int64(partition.EndOffset())))
		}
	}(ticker)
	syncStarted := time.Now()

	var syncing = true

MAIN:
	for {
		select {
		case <-ctx.Done():
			lg.logger.Info(`Consumer loop stopping due to context cancel`)
			break MAIN
		case <-lg.stopping:
			lg.logger.Info(`Consumer loop stopping due to stop signal`)
			break MAIN
		case event := <-partition.Events():
			switch e := event.(type) {
			case kafka.Record:
				atomic.AddInt64(&syncedCount, 1)
				// Handle tombstones
				if idxStor, ok := lg.store.(stores.IndexedStore); ok && len(idxStor.Indexes()) > 0 {
					// If recovery is in progress
					if syncing {
						if err := lg.updateStore(e); err != nil {
							lg.logger.Error(fmt.Sprintf(`Cannot update store due to %s`, err))
						}
						continue
					}

					if err := lg.updateStoreWithIndex(idxStor, e); err != nil {
						lg.logger.Error(fmt.Sprintf(`Cannot update indexed store due to %s`, err))
					}

				} else {
					if err := lg.updateStore(e); err != nil {
						lg.logger.Error(fmt.Sprintf(`Cannot update store due to %s`, err))
					}
				}

				// Non-persistent backends cannot store the offset
				if !lg.store.Backend().Persistent() {
					continue
				}

				// TODO offset has to be offset+1
				if err := lg.offsetStore.Commit(ctx, lg.tp, kafka.Offset(e.Offset())); err != nil {
					lg.logger.Error(fmt.Sprintf(`Cannot commit offset due to %s`, err))
					continue
				}

			case *kafka.PartitionEnd:
				endOnce.Do(func() {
					lg.logger.Info(fmt.Sprintf(
						`Partition read ended. Restored %d records in %s`,
						syncedCount,
						time.Since(syncStarted).String()))
					ticker.Stop()

					// Rebuild indexes
					if idxStr, ok := lg.store.(stores.IndexedStore); ok {
						if err := lg.rebuildStoreIndexes(idxStr); err != nil {
							panic(errors.Wrapf(err, `changelog recovery failed. `).Error())
						}
					}

					// Send partition sync completed signal
					synced <- struct{}{}
					syncing = false

				})

			case *kafka.Error:
				lg.logger.Error(e)
			}
		}
	}

	return nil
}

func (lg *changelogSyncer) Stop() error {
	lg.logger.Info(`Syncer stopping...`)
	defer lg.logger.Info(`Syncer stopped`)

	close(lg.stopping)
	<-lg.running

	return nil
}

func (lg *changelogSyncer) updateStoreWithIndex(store stores.IndexedStore, record kafka.Record) error {
	if len(record.Value()) < 1 {
		key, err := lg.store.KeyEncoder().Decode(record.Key())
		if err != nil {
			return errors.Wrap(err, `store index update error due to key encode failed`)
		}

		// Delete indexes (if any)
		if err := stores.DeleteIndexes(record.Ctx(), store, key); err != nil {
			return errors.Wrapf(err, `delete indexes failed on message(%s)`, record)
		}

		// Delete record from backend (tombstone)
		return lg.store.Backend().Delete(record.Key())
	}

	// We need to decode Key and Value here to update the indexes(if any)
	key, err := lg.store.KeyEncoder().Decode(record.Key())
	if err != nil {
		return errors.Wrapf(err, `key decode failed on message(%s)`, record)
	}

	val, err := lg.store.ValEncoder().Decode(record.Value())
	if err != nil {
		return errors.Wrapf(err, `value decode failed on message(%s)`, record)
	}

	if err := stores.UpdateIndexes(record.Ctx(), store, key, val); err != nil {
		return errors.Wrapf(err, `update indexes failed on message(%s)`, record)
	}

	return lg.store.Backend().Set(record.Key(), record.Value(), 0)
}

func (lg *changelogSyncer) updateStore(record kafka.Record) error {
	if len(record.Value()) < 1 {
		if err := lg.store.Backend().Delete(record.Key()); err != nil {
			return errors.Wrapf(err, `Cannot delete(tombstone) message(%s)`, record)
		}

		return nil
	}

	if err := lg.store.Backend().Set(record.Key(), record.Value(), 0); err != nil {
		return errors.Wrapf(err, `Cannot write message(%s)`, record)
	}

	return nil
}

func (lg *changelogSyncer) rebuildStoreIndexes(stor stores.IndexedStore) error {
	lg.logger.Info(`Rebuilding indexes...`)

	var idxCount int
	defer func(t time.Time) {
		lg.logger.Info(fmt.Sprintf(`Rebuilding indexes completed(%d records) in %s`, idxCount, time.Since(t).String()))
	}(time.Now())

	itr, err := stor.Iterator(nil)
	if err != nil {
		return errors.Wrapf(err, `rebuild indexes failed. store iterator failed`)
	}

	defer itr.Close()

	for itr.SeekToFirst(); itr.Valid(); itr.Next() {
		key, err := itr.Key()
		if err != nil {
			return errors.Wrapf(err, `rebuild indexes failed. itr key failed`)
		}

		val, err := itr.Value()
		if err != nil {
			return errors.Wrapf(err, `rebuild indexes failed. itr value failed`)
		}

		if err := stores.UpdateIndexes(nil, stor, key, val); err != nil {
			return errors.Wrapf(err, `rebuild indexes failed. cannot update indexes`)
		}
		idxCount++
	}

	return nil
}

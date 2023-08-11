package tasks

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/pkg/errors"
	"github.com/gmbyapa/kstream/v2/streams/topology"
	"github.com/tryfix/metrics"
	"sync"
	"time"

	"github.com/tryfix/log"
)

type OnFlush func(records []*Record) error

type Buffer interface {
	Init() error
	Add(record *Record) error
	Flush() error
	Close() error
	Reset(dueTo error) error
	Records() []*Record
	Closing() bool
	MarkAsCosing()
}

type BufferConfig struct {
	// Size defines the min num of records before the flush
	// starts(This includes messages in the state store changelogs).
	// Please note that this value has to be lesser than the
	// producer queue.buffering.max.messages
	// Deprecated no longer applicable
	Size int
	// FlushInterval defines minimum wait time before the flush starts
	FlushInterval time.Duration
}

type commitBuffer struct {
	records   []*Record
	offsetMap map[string]kafka.ConsumerOffset

	mu *sync.Mutex

	producer    kafka.TransactionalProducer
	subTopology topology.SubTopology
	session     kafka.GroupSession

	metrics struct {
		batchSize metrics.Counter
	}
	closing bool

	logger log.Logger
}

func newCommitBuffer(topology topology.SubTopology, producer kafka.Producer, session kafka.GroupSession, logger log.Logger, reporter metrics.Reporter) *commitBuffer {
	buf := &commitBuffer{
		mu:        &sync.Mutex{},
		offsetMap: map[string]kafka.ConsumerOffset{},
		logger:    logger,

		producer:    producer.(kafka.TransactionalProducer),
		subTopology: topology,
		session:     session,
	}

	buf.metrics.batchSize = reporter.Counter(metrics.MetricConf{
		Path: `batch_size`,
	})

	return buf
}

func (b *commitBuffer) Init() error {
	if err := b.producer.InitTransactions(context.Background()); err != nil {
		b.handleTxError(b.logger, b.producer, err, `Buffer Init failed`)
	}

	if err := b.producer.BeginTransaction(); err != nil {
		b.handleTxError(b.logger, b.producer, err, `Buffer Init failed`)
	}

	return nil
}

func (b *commitBuffer) Add(record *Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, record)
	b.offsetMap[fmt.Sprintf(`%s-%d`, record.Topic(), record.Partition())] = kafka.ConsumerOffset{
		Topic:     record.Topic(),
		Partition: record.Partition(),
		Offset:    record.Offset() + 1,
	}

	b.logger.TraceContext(record.Ctx(), `Record stored in commit buffer`, record.String())

	return nil
}

func (b *commitBuffer) Records() []*Record {
	return b.records
}

func (b *commitBuffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.flush()
}

func (b *commitBuffer) flush() error {
	count := len(b.records)
	if count < 1 {
		return nil
	}

	defer func() {
		b.metrics.batchSize.Count(float64(count), nil)
	}()

	return b.commit()
}

func (b *commitBuffer) commit() error {
	offsets := make([]kafka.ConsumerOffset, 0)
	if len(b.offsetMap) > 0 {
		for i := range b.offsetMap {
			offsets = append(offsets, b.offsetMap[i])
		}

		meta, err := b.session.GroupMeta()
		if err != nil {
			b.logger.Error(fmt.Sprintf(`transaction Consumer GroupMeta fetch failed due to %s, abotring transactions`, err))
			if txAbErr := b.producer.AbortTransaction(context.Background()); txAbErr != nil {
				b.logger.Warn(fmt.Sprintf(`transaction abort failed due to %s`, txAbErr))
				return txAbErr
			}

			return err
		}

		if err := b.producer.SendOffsetsToTransaction(context.Background(), offsets, meta); err != nil {
			return errors.Wrap(err, `commit(SendOffsetsToTransaction) failed`)
		}
	}

	if err := b.producer.CommitTransaction(context.Background()); err != nil {
		return errors.Wrap(err, `commit(CommitTransaction) failed`)
	}

	for name := range b.subTopology.StateStores() {
		store := b.subTopology.StateStores()[name]
		if err := store.Flush(); err != nil {
			return errors.Wrap(err, `state stores flush failed`)
		}
		store.ResetCache()
	}

	// Begin a new transaction
	if err := b.producer.BeginTransaction(); err != nil {
		panic(err)
	}

	// Reset offsets map and records maps
	b.offsetMap = map[string]kafka.ConsumerOffset{}
	b.records = nil

	if len(offsets) > 0 {
		b.logger.Info(fmt.Sprintf(`Transaction committed(offsets %+v)`, offsets))
	}

	return nil
}

func (b *commitBuffer) Reset(dueTo error) error {
	b.logger.Warn(fmt.Sprintf(`Commit Buffer resetting due to %s...`, dueTo))
	defer b.logger.Info(`Commit Buffer rested`)

	b.mu.Lock()
	defer b.mu.Unlock()

	// Purge the store cache before the processing starts.
	// This will clear out any half processed states from state store caches.
	for name := range b.subTopology.StateStores() {
		b.subTopology.StateStores()[name].ResetCache()
	}

	if err := b.producer.AbortTransaction(context.Background()); err != nil {
		b.handleTxError(b.logger, b.producer, err, `AbortTransaction error`)
		goto OffsetReset // handleTxError will begin the transaction
	}

	if err := b.producer.BeginTransaction(); err != nil {
		b.handleTxError(b.logger, b.producer, err, `BeginTransaction error`)
	}

OffsetReset:
	b.offsetMap = map[string]kafka.ConsumerOffset{}
	b.records = nil

	return nil
}

func (b *commitBuffer) handleTxError(logger log.Logger, producer kafka.TransactionalProducer, err error, reason string) {
	if b.closing {
		logger.Warn(`handleTxError ignoring err (%s) due to CommitBuffer closing`)
		return
	}

	producerErr := errors.UnWrapRecursivelyUntil(err, func(err error) bool {
		_, ok := err.(kafka.ProducerErr)
		return ok
	})

	if producerErr.(kafka.ProducerErr).TxnRequiresAbort() {
		logger.Warn(fmt.Sprintf(`Transaction aborting. Reason:%s, Err:%s, retrying...`, reason, err))
		if err := producer.AbortTransaction(nil); err != nil {
			b.handleTxError(logger, producer, err, `tx abort failed`)
		}
	}

	if ok := producerErr.(kafka.ProducerErr).RequiresRestart(); ok {
		logger.Error(fmt.Sprintf(`Libkrkafka FATAL error restarting producer client. Reason:%s, Err:%s`, reason, err))

		// Re-initiate producer client
		if restartErr := producer.Restart(); restartErr != nil {
			panic(restartErr)
		}
	}

	if err := producer.InitTransactions(nil); err != nil {
		b.handleTxError(logger, producer, err, `transaction init failed`)
	}

	if err := producer.BeginTransaction(); err != nil {
		b.handleTxError(logger, producer, err, `transaction begin failed`)
	}
}

func (b *commitBuffer) Close() error {
	b.logger.Info(`Commit Buffer closing...`)
	defer b.logger.Info(`Commit Buffer closed`)

	return b.Flush()
}

func (b *commitBuffer) Closing() bool {
	return b.closing
}

func (b *commitBuffer) MarkAsCosing() {
	b.closing = true
}

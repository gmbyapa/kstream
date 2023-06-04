/**
 * Copyright 2020 TryFix Engineering.
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gmbyapa@gmail.com)
 */

package librd

import (
	"context"
	"fmt"
	librdKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
	"time"
)

type Err struct {
	error
	reInit bool
}

func (t Err) RequiresRestart() bool {
	return t.reInit
}

type librdTxProducer struct {
	*librdProducer
}

func (p *librdTxProducer) InitTransactions(ctx context.Context) error {
	defer func(begin time.Time) {
		p.metrics.transactions.initLatency.Observe(float64(time.Since(begin).Microseconds()), nil)
	}(time.Now())

	if err := p.librdProducer.librdProducer().InitTransactions(ctx); err != nil {
		return p.handleTxError(ctx, err, `transaction init failed`, func() error {
			return p.InitTransactions(ctx)
		})
	}

	p.config.Logger.Info(`Transaction inited`)

	return nil
}

func (p *librdTxProducer) BeginTransaction() error {
	if err := p.librdProducer.librdProducer().BeginTransaction(); err != nil {
		return p.handleTxError(context.Background(), err, `transaction begin failed`, func() error {
			return p.BeginTransaction()
		})
	}

	return nil
}

func (p *librdTxProducer) CommitTransaction(ctx context.Context) error {
	defer func(begin time.Time) {
		p.metrics.transactions.commitLatency.Observe(float64(time.Since(begin).Microseconds()), nil)
	}(time.Now())

	if err := p.librdProducer.librdProducer().CommitTransaction(ctx); err != nil {
		return p.handleTxError(ctx, err, `transaction commit failed`, func() error {
			return p.CommitTransaction(ctx)
		})
	}

	p.librdProducer.config.Logger.Trace(fmt.Sprintf(`transaction commited`))

	return nil
}

func (p *librdTxProducer) SendOffsetsToTransaction(ctx context.Context, offsets []kafka.ConsumerOffset, meta *kafka.GroupMeta) error {
	var kOffsets []librdKafka.TopicPartition
	for i := range offsets {
		kOffsets = append(kOffsets, librdKafka.TopicPartition{
			Topic:     &offsets[i].Topic,
			Partition: offsets[i].Partition,
			Offset:    librdKafka.Offset(offsets[i].Offset),
			Metadata:  &offsets[i].Meta,
		})
	}

	if err := p.librdProducer.librdProducer().SendOffsetsToTransaction(ctx, kOffsets, meta.Meta.(*librdKafka.ConsumerGroupMetadata)); err != nil {
		return p.handleTxError(ctx, err, `transaction SendOffsetsToTransaction failed`, func() error {
			return p.SendOffsetsToTransaction(ctx, offsets, meta)
		})
	}

	p.librdProducer.config.Logger.Trace(fmt.Sprintf(`Offsets sent, %+v`, kOffsets))

	return nil
}

func (p *librdTxProducer) AbortTransaction(ctx context.Context) error {
	defer func(begin time.Time) {
		p.metrics.transactions.abortLatency.Observe(float64(time.Since(begin).Microseconds()), nil)
	}(time.Now())

	if err := p.librdProducer.librdProducer().AbortTransaction(ctx); err != nil && err.(librdKafka.Error).Code() != librdKafka.ErrState {
		return p.handleTxError(ctx, err, `transaction abort failed`, func() error {
			return p.AbortTransaction(ctx)
		})
	}

	p.config.Logger.WarnContext(ctx, fmt.Sprintf(`Transaction aborted`))

	return nil
}

func (p *librdTxProducer) ProduceSync(ctx context.Context, message kafka.Record) (partition int32, offset int64, err error) {
	panic(`transactional producer does not support ProduceSync mode`)
}

func (p *librdTxProducer) ProduceAsync(ctx context.Context, message kafka.Record) (err error) {
	defer func(begin time.Time) {
		p.metrics.produceLatency.Observe(float64(time.Since(begin).Microseconds()), map[string]string{
			`topic`: message.Topic(),
		})
	}(time.Now())

	kMessage, err := p.prepareMessage(message)
	if err != nil {
		return p.handleTxError(ctx, err, `prepareMessage failed`, nil)
	}

	err = p.librdProducer.librdProducer().Produce(kMessage, nil)
	if err != nil {
		return p.handleTxError(ctx, err, `ProduceAsync failed`, nil)
	}

	p.config.Logger.TraceContext(ctx, fmt.Sprintf(`Record %s queued`, message))

	return nil
}

func (p *librdTxProducer) handleTxError(ctx context.Context, err error, reason string, retry func() error) error {
	if err.(librdKafka.Error).IsRetriable() {
		p.config.Logger.WarnContext(ctx, fmt.Sprintf(`%s due to (%s), retrying...`, reason, err))
		return retry()
	}

	if err.(librdKafka.Error).TxnRequiresAbort() {
		p.config.Logger.WarnContext(ctx, fmt.Sprintf(`Transaction aborting, %s due to (%s), retrying...`, reason, err))
		return p.AbortTransaction(ctx)
	}

	if fatal := p.librdProducer.librdProducer().GetFatalError(); fatal != nil || err.(librdKafka.Error).IsFatal() {
		p.config.Logger.ErrorContext(ctx, fmt.Sprintf(`%s Libkrkafka FATAL error %s`, reason, err))

		// Re-initate producer client
		if restartErr := p.Restart(); restartErr != nil {
			panic(restartErr)
		}
	}

	if err := p.librdProducer.librdProducer().InitTransactions(ctx); err != nil {
		return p.handleTxError(ctx, err, `transaction init failed`, func() error {
			return p.InitTransactions(ctx)
		})
	}

	return &Err{error: err, reInit: true}
}

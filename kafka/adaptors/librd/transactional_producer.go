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
	"github.com/gmbyapa/kstream/pkg/errors"
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
	if err := p.librdProducer.librdProducer.InitTransactions(ctx); err != nil {
		return p.handleTxError(ctx, err, `transaction init failed`, func() error {
			return p.InitTransactions(ctx)
		})
	}

	p.config.Logger.Info(`Transaction inited`)

	return nil
}

func (p *librdTxProducer) BeginTransaction() error {
	if err := p.librdProducer.librdProducer.BeginTransaction(); err != nil {
		return p.handleTxError(context.Background(), err, `transaction begin failed`, func() error {
			return p.BeginTransaction()
		})
	}

	return nil
}

func (p *librdTxProducer) CommitTransaction(ctx context.Context) error {
	if err := p.librdProducer.librdProducer.CommitTransaction(ctx); err != nil {
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

	if err := p.librdProducer.librdProducer.SendOffsetsToTransaction(ctx, kOffsets, meta.Meta.(*librdKafka.ConsumerGroupMetadata)); err != nil {
		return p.handleTxError(ctx, err, `transaction SendOffsetsToTransaction failed`, func() error {
			return p.SendOffsetsToTransaction(ctx, offsets, meta)
		})
	}

	p.librdProducer.config.Logger.Trace(fmt.Sprintf(`Offsets sent, %+v`, kOffsets))

	return nil
}

func (p *librdTxProducer) AbortTransaction(ctx context.Context) error {
	if err := p.librdProducer.librdProducer.AbortTransaction(ctx); err != nil {
		return errors.Wrap(err, `transaction abort failed`)
	}

	p.config.Logger.WarnContext(ctx, fmt.Sprintf(`transaction aborted`))

	return nil
}

func (p *librdTxProducer) ProduceSync(ctx context.Context, message kafka.Record) (partition int32, offset int64, err error) {
	panic(`transactional producer does not support ProduceSync mode`)
}

func (p *librdTxProducer) ProduceAsync(ctx context.Context, message kafka.Record) (err error) {
	kMessage := p.prepareMessage(message)
	err = p.librdProducer.librdProducer.Produce(kMessage, nil)
	if err != nil {
		return p.handleTxError(ctx, err, `ProduceAsync failed`, nil)
	}

	p.config.Logger.DebugContext(ctx, fmt.Sprintf(`Record %s produced`, message))

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

	if fatal := p.librdProducer.librdProducer.GetFatalError(); fatal != nil || err.(librdKafka.Error).IsFatal() {
		p.config.Logger.ErrorContext(ctx, fmt.Sprintf(`Libkrkafka FATAL error %s`, err))
		return &Err{error: fatal, reInit: true}
	}

	return &Err{error: err, reInit: true}
}

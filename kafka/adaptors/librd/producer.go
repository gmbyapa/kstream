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
	"github.com/gmbyapa/kstream/kafka/adaptors/sarama"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
	"time"
)

const (
	PartitionerRandom                  kafka.PartitionerType = `random`
	PartitionerCRC32                   kafka.PartitionerType = `consistent`
	PartitionerCRC32Random             kafka.PartitionerType = `consistent_random`
	PartitionerConsistentMurmur2       kafka.PartitionerType = `murmur2`
	PartitionerConsistentMurmur2Random kafka.PartitionerType = `murmur2_random`
	PartitionerConsistentFNV1a         kafka.PartitionerType = `fnv1a`
	PartitionerConsistentFNV1aRandom   kafka.PartitionerType = `fnv1a_random`
)

type librdProducer struct {
	config       *ProducerConfig
	baseProducer *librdKafka.Producer

	metrics struct {
		produceLatency metrics.Observer
		transactions   struct {
			initLatency        metrics.Observer
			sendOffsetsLatency metrics.Observer
			commitLatency      metrics.Observer
			abortLatency       metrics.Observer
		}
		produceErrors metrics.Counter
	}

	mu *sync.RWMutex

	partitionCounts *sync.Map
	adminClient     kafka.Admin
}

type producerProvider struct {
	config *ProducerConfig
}

func NewProducerProvider(config *ProducerConfig) kafka.ProducerProvider {
	return &producerProvider{config: config}
}

func (c *producerProvider) NewBuilder(conf *kafka.ProducerConfig) kafka.ProducerBuilder {
	c.config.ProducerConfig = conf

	if err := c.config.Librd.SetKey(`go.events.channel.size`, 1000); err != nil {
		panic(err)
	}

	if err := c.config.Librd.SetKey(`go.produce.channel.size`, 1000); err != nil {
		panic(err)
	}

	return func(configure func(*kafka.ProducerConfig)) (kafka.Producer, error) {
		defaultConfCopy := c.config.copy()
		configure(defaultConfCopy.ProducerConfig)

		return NewProducer(defaultConfCopy)
	}
}

func NewProducer(configs *ProducerConfig) (kafka.Producer, error) {
	if err := configs.setUp(); err != nil {
		return nil, errors.Wrap(err, `producer configs setup failed`)
	}

	if err := configs.validate(); err != nil {
		return nil, errors.Wrap(err, `invalid producer configs`)
	}

	// Create an admin client to fetch topic metadata
	admin, err := sarama.NewAdmin(configs.BootstrapServers)
	if err != nil {
		return nil, errors.Wrap(err, `metadata fetch failed`)
	}

	loggerPrefix := `Producer`
	if configs.Transactional.Enabled {
		loggerPrefix = `TransactionalProducer`
	}

	configs.Logger = configs.Logger.NewLog(log.Prefixed(fmt.Sprintf(`%s(librdkafka)`, loggerPrefix)))

	configs.Logger.Info(`Producer initiating...`)
	producer, err := librdKafka.NewProducer(configs.Librd)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`Producer(%s) init failed`, configs.Id))
	}

	defer configs.Logger.Info(`Producer initiated`)

	p := &librdProducer{
		config:          configs,
		baseProducer:    producer,
		adminClient:     admin,
		partitionCounts: new(sync.Map),
		mu:              &sync.RWMutex{},
	}

	// Drain all the delivery messages (we don't need them here. since we are relying on transaction commit).
	// This is important because if Produce() failed for a batch, the failed delivery reports will be sent to this
	// channel
	go func() {
		for ev := range producer.Events() {
			switch e := ev.(type) {
			case librdKafka.Error:
				p.config.Logger.Error(fmt.Sprintf(`Event [%s]%s`, e.Code(), e))
			case *librdKafka.Message:
				p.config.Logger.Error(fmt.Sprintf(`Event %s`, e.TopicPartition.Error))
			}
		}
	}()

	go p.printLogs()

	p.metrics.produceLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:   `kstream_producer_produced_latency_microseconds`,
		Labels: []string{`topic`},
		ConstLabels: map[string]string{
			`producer_id`: configs.Id,
			`async`:       fmt.Sprintf(`%t`, p.config.Transactional.Enabled),
		},
	})

	p.metrics.transactions.commitLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:        `kstream_producer_transaction_commit_latency_microseconds`,
		ConstLabels: map[string]string{`producer_id`: configs.Id},
	})

	p.metrics.transactions.initLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:        `kstream_producer_transaction_init_latency_microseconds`,
		ConstLabels: map[string]string{`producer_id`: configs.Id},
	})

	p.metrics.transactions.abortLatency = configs.MetricsReporter.Observer(metrics.MetricConf{
		Path:        `kstream_producer_transaction_abort_latency_microseconds`,
		ConstLabels: map[string]string{`producer_id`: configs.Id},
	})

	p.metrics.produceErrors = configs.MetricsReporter.Counter(metrics.MetricConf{
		Path:        `kstream_producer_error_count`,
		Labels:      []string{`error`},
		ConstLabels: map[string]string{`producer_id`: configs.Id},
	})

	if p.config.Transactional.Enabled {
		return &librdTxProducer{p}, nil
	}

	return p, nil
}

func (p *librdProducer) NewRecord(
	ctx context.Context,
	key []byte,
	value []byte,
	topic string,
	partition int32,
	timestamp time.Time,
	headers kafka.RecordHeaders,
	meta string) kafka.Record {

	librdHeaders := make([]librdKafka.Header, len(headers))
	for i := range headers {
		librdHeaders[i] = librdKafka.Header{
			Key:   string(headers[i].Key),
			Value: headers[i].Value,
		}
	}

	return &Record{
		librd: &librdKafka.Message{
			TopicPartition: librdKafka.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Metadata:  &meta,
			},
			Value:     value,
			Key:       key,
			Timestamp: timestamp,
			Headers:   librdHeaders,
		},
		ctx: ctx,
	}
}

func (p *librdProducer) ProduceSync(ctx context.Context, message kafka.Record) (partition int32, offset int64, err error) {
	dChan := make(chan librdKafka.Event)
	kMessage, err := p.prepareMessage(message)
	if err != nil {
		return 0, 0, errors.Wrapf(err, `message[%s] prepare error`, message)
	}

	err = p.librdProducer().Produce(kMessage, dChan)
	if err != nil {
		return 0, 0, errors.Wrap(err, `cannot send message`)
	}

	dRpt := <-dChan
	dmSg := dRpt.(*librdKafka.Message)

	if dmSg.TopicPartition.Error != nil {
		return 0, 0, errors.Wrapf(dmSg.TopicPartition.Error, `message %s delivery failed`, message)
	}

	p.metrics.produceLatency.Observe(float64(time.Since(kMessage.Timestamp).Nanoseconds()/1e3), map[string]string{
		`topic`: *dmSg.TopicPartition.Topic,
	})

	p.config.Logger.DebugContext(ctx, fmt.Sprintf("Delivered message to topic %s[%d]@%d",
		message.Topic(), dmSg.TopicPartition.Partition, dmSg.TopicPartition.Offset))

	return dmSg.TopicPartition.Partition, int64(dmSg.TopicPartition.Offset), nil
}

func (p *librdProducer) Close() error {
	p.config.Logger.Info(`Producer closing...`)
	defer p.config.Logger.Info(`Producer closed`)

	// Lets remove all the messages in librdkafka queues
	if err := p.librdProducer().Purge(
		librdKafka.PurgeInFlight |
			librdKafka.PurgeNonBlocking | librdKafka.PurgeQueue); err != nil {
		p.config.Logger.Error(err)
	}

	err := p.librdProducer().AbortTransaction(nil)
	if err != nil {
		if err.(librdKafka.Error).Code() == librdKafka.ErrState {
			// No transaction in progress, ignore the error.
			err = nil
		} else {
			return err
		}
	}

	p.librdProducer().Close()

	return nil
}

func (p *librdProducer) Restart() error {
	p.config.Logger.Info(`Producer restarting...`)
	defer p.config.Logger.Info(`Producer restarted`)

	if err := p.forceClose(); err != nil {
		p.config.Logger.Warn(err)
	}

	prd, err := librdKafka.NewProducer(p.config.Librd)
	if err != nil {
		p.config.Logger.Fatal(err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.baseProducer = prd

	return nil
}

func (p *librdProducer) printLogs() {
	logger := p.config.Logger.NewLog(log.Prefixed(`LibrdLogs`))
	for lg := range p.librdProducer().Logs() {
		switch lg.Level {
		case 0, 1, 2:
			logger.Error(lg.String(), `level`, lg.Level)
		case 3, 4, 5:
			logger.Warn(lg.String(), `level`, lg.Level)
		case 6:
			logger.Info(lg.String(), `level`, lg.Level)
		case 7:
			logger.Debug(lg.String(), `level`, lg.Level)
		}
	}
}

func (p *librdProducer) forceClose() error {
	p.config.Logger.Warn(`Producer closing forcefully...`)
	defer p.config.Logger.Warn(`Producer closed`)

	p.config.Logger.Info(`Purging producer queues kafka.PurgeInFlight|kafka.PurgeNonBlocking|kafka.PurgeQueue`)
	if err := p.librdProducer().Purge(
		librdKafka.PurgeInFlight |
			librdKafka.PurgeNonBlocking | librdKafka.PurgeQueue); err != nil {
		p.config.Logger.Error(err)
	}

	p.librdProducer().Close()

	return nil
}

func (p *librdProducer) prepareMessage(message kafka.Record) (*librdKafka.Message, error) {
	t := time.Now()
	topic := message.Topic()
	m := &librdKafka.Message{
		TopicPartition: librdKafka.TopicPartition{
			Topic: &topic,
		},
		Key:           message.Key(),
		Value:         message.Value(),
		Timestamp:     t,
		TimestampType: librdKafka.TimestampCreateTime,
	}

	// Use the partitioner defined in librdkafka config
	if message.Partition() == kafka.PartitionAny {
		m.TopicPartition.Partition = librdKafka.PartitionAny
	}

	if message.Partition() > 0 {
		m.TopicPartition.Partition = message.Partition()
		goto Headers
	}

	if p.config.PartitionerFunc != nil {
		pCount, err := p.getPartitionCount(message.Topic())
		if err != nil {
			return nil, errors.Wrapf(err, `partition count failed for %d`, pCount)
		}

		m.TopicPartition.Partition, err = p.config.PartitionerFunc(message, pCount)
		if err != nil {
			return nil, errors.Wrapf(err, `partitioner error`)
		}
	}

Headers:
	for _, header := range message.Headers() {
		m.Headers = append(m.Headers, librdKafka.Header{
			Key:   string(header.Key),
			Value: header.Value,
		})
	}

	if !message.Timestamp().IsZero() {
		m.Timestamp = message.Timestamp()
	}

	return m, nil
}

func (p *librdProducer) getPartitionCount(topic string) (int32, error) {
	//TODO refresh counts in a ticker
	v, ok := p.partitionCounts.Load(topic)
	if ok {
		return v.(int32), nil
	}

	meta, err := p.librdProducer().GetMetadata(&topic, false, 10000) //TODO make this configurable
	if err != nil {
		return 0, errors.Wrapf(err, `metadata fetch failed for %s`, topic)
	}

	count := int32(len(meta.Topics[topic].Partitions))
	p.partitionCounts.Store(topic, count)

	return count, nil
}

func (p *librdProducer) librdProducer() *librdKafka.Producer {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.baseProducer
}

func toLibrdLogLevel(level log.Level) int {
	switch level {
	case log.ERROR:
		return 2
	case log.WARN:
		return 5
	case log.INFO:
		return 6
	case log.DEBUG:
		return 7
	}

	return 0
}

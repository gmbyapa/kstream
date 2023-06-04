package librd

import (
	"context"
	"fmt"
	librdKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
	"time"
)

type partitionConsumer struct {
	consumer *librdKafka.Consumer

	partitions map[string]*partition
	mu         sync.Mutex

	logger          log.Logger
	metricsReporter metrics.Reporter
	metrics         struct {
		endToEndLatency metrics.Observer
	}
	closing chan struct{}
	closed  chan struct{}
	config  *ConsumerConfig
}

type consumerProvider struct {
	config *ConsumerConfig
}

func NewConsumerProvider(config *ConsumerConfig) kafka.ConsumerProvider {
	return &consumerProvider{config: config}
}

func (c *consumerProvider) NewBuilder(conf *kafka.ConsumerConfig) kafka.ConsumerBuilder {
	c.config.ConsumerConfig = conf
	if err := c.config.Librd.SetKey(`client.id`, c.config.Id); err != nil {
		panic(err.Error())
	}

	if c.config.EOSEnabled {
		c.config.IsolationLevel = kafka.ReadCommitted
	}

	return func(configure func(*kafka.ConsumerConfig)) (kafka.PartitionConsumer, error) {
		defaultConfCopy := c.config.copy()
		configure(defaultConfCopy.ConsumerConfig)

		return NewPartitionConsumer(defaultConfCopy)
	}
}

func NewPartitionConsumer(configs *ConsumerConfig) (kafka.PartitionConsumer, error) {
	if err := configs.setUp(); err != nil {
		return nil, errors.Wrap(err, `producer configs setup failed`)
	}

	consumer, err := librdKafka.NewConsumer(configs.Librd)
	if err != nil {
		return nil, errors.Wrap(err, `new consumer failed`)
	}

	pc := &partitionConsumer{
		consumer:        consumer,
		config:          configs,
		partitions:      map[string]*partition{},
		logger:          configs.ConsumerConfig.Logger.NewLog(log.Prefixed(`PartitionConsumer`)),
		metricsReporter: configs.MetricsReporter,
	}

	go pc.consumeMessages()

	return pc, nil
}

func (c *partitionConsumer) Partitions(_ context.Context, topic string) ([]int32, error) {
	meta, err := c.consumer.GetMetadata(&topic, false, int(c.config.TopicMetaFetchTimeout.Milliseconds())) // TODO make this configurable
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`cannot fetch partitions for topic [%s]'`, topic))
	}

	var partitions []int32
	for _, tp := range meta.Topics {
		for _, pt := range tp.Partitions {
			partitions = append(partitions, pt.ID)
		}
	}

	return partitions, nil
}

func (c *partitionConsumer) ConsumeTopic(ctx context.Context, topic string, offset kafka.Offset) (map[int32]kafka.Partition, error) {
	pts, err := c.Partitions(ctx, topic)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`cannot fetch partitions fopr topic {%s}`, topic))
	}

	partitions := map[int32]kafka.Partition{}
	for _, ptId := range pts {
		partition, err := c.ConsumePartition(ctx, topic, ptId, offset)
		if err != nil {
			return nil, err
		}

		partitions[ptId] = partition
	}

	return partitions, nil
}

func (c *partitionConsumer) ConsumePartition(ctx context.Context, topic string, ptt int32, offset kafka.Offset) (kafka.Partition, error) {
	pId := kafka.TopicPartition{
		Topic:     topic,
		Partition: ptt,
	}.String()

	// fetch offset information
	oldest, err := c.GetOffsetOldest(topic, ptt)
	if err != nil {
		return nil, errors.Wrap(err, `cannot fetch latest offset`)
	}

	latest, err := c.GetOffsetLatest(topic, ptt)
	if err != nil {
		return nil, errors.Wrap(err, `cannot fetch latest offset`)
	}

	pt := &partition{
		events:         make(chan kafka.Event, c.config.ConsumerMessageChanSize),
		consumerErrors: make(chan error, 1),
		logger:         c.logger.NewLog(log.Prefixed(fmt.Sprintf(`%s-%d`, topic, ptt))),
		metricsReporter: c.metricsReporter.Reporter(metrics.ReporterConf{
			Subsystem: "partition_consumer",
			ConstLabels: map[string]string{
				`topic_partition`: fmt.Sprintf(`%s_%d`, topic, ptt),
			},
		}),
		topic:          topic,
		partition:      ptt,
		partitionStart: oldest,
		partitionEnd:   latest,
		consumer:       c.consumer,
		mu:             new(sync.Mutex),
	}

	pt.init()

	c.mu.Lock()
	c.partitions[pId] = pt
	c.mu.Unlock()

	err = c.consumer.IncrementalAssign([]librdKafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: ptt,
			Offset:    librdKafka.Offset(offset),
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(`cannot initiate partition consumer for %s#%d`, topic, ptt))
	}

	return pt, nil
}

func (c *partitionConsumer) OffsetValid(topic string, partition int32, offset int64) (isValid bool, err error) {
	isValid, err = c.validate(topic, partition, offset)
	return
}

func (c *partitionConsumer) GetOffsetLatest(topic string, partition int32) (offset int64, err error) {
	// TODO use QueryWatermarkOffsets to get offsets
	partitionStart, err := c.consumer.OffsetsForTimes(librdKafka.TopicPartitions{
		librdKafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    librdKafka.Offset(kafka.OffsetLatest),
		},
	}, 60000) // TODO make this configurable
	if err != nil {
		return offset, fmt.Errorf(`cannot get latest offset for %s-%d due to %w`, topic, partition, err)
	}

	return int64(partitionStart[0].Offset), nil
}

func (c *partitionConsumer) GetOffsetOldest(topic string, partition int32) (offset int64, err error) {
	// TODO use QueryWatermarkOffsets to get offsets
	partitionStart, err := c.consumer.OffsetsForTimes(librdKafka.TopicPartitions{
		librdKafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    librdKafka.Offset(kafka.OffsetEarliest),
		},
	}, 60000)
	if err != nil {
		return offset, fmt.Errorf(`cannot get latest offset for %s#%d due to %w`, topic, partition, err)
	}

	return int64(partitionStart[0].Offset), nil
}

func (c *partitionConsumer) Close() error {
	c.logger.Info(`Partition consumer closing...`)

	close(c.closing)
	<-c.closed

	if err := c.consumer.Close(); err != nil {
		c.logger.Error(fmt.Sprintf("consumer close failed [%s] ", err))
	}

	c.mu.Lock()
	for _, pt := range c.partitions {
		if err := pt.Close(); err != nil {
			c.logger.Error(fmt.Sprintf("Partition close failed [%s] ", err))
		}
	}
	c.mu.Unlock()

	c.logger.Info(fmt.Sprintf(`Partition consumer closed`))

	return nil
}

func (c *partitionConsumer) consumeMessages() {
MAIN:
	for {
		select {
		case <-c.closing:
			break MAIN
		default:
			ev := c.consumer.Poll(int(c.config.MaxPollInterval.Milliseconds()))
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *librdKafka.Message:
				// find the message channel and sent
				pId := kafka.TopicPartition{
					Topic:     *e.TopicPartition.Topic,
					Partition: e.TopicPartition.Partition,
				}.String()

				record := &Record{librd: e}
				record.ctx = context.Background()

				c.mu.Lock()
				pt, ok := c.partitions[pId]
				c.mu.Unlock()
				if !ok {
					c.logger.Fatal(fmt.Sprintf(`Partition %s does not exist`, pId))
				}

				pt.metrics.endToEndLatency.Observe(float64(time.Since(record.Timestamp()).Microseconds()), nil)

				pt.Send(record)

			case librdKafka.PartitionEOF:
				ptI := kafka.TopicPartition{
					Topic:     *e.Topic,
					Partition: e.Partition,
				}
				// find the message channel and sent
				c.mu.Lock()
				pt, ok := c.partitions[ptI.String()]
				c.mu.Unlock()
				if !ok {
					c.logger.Fatal(fmt.Sprintf(`Partition %s does not exist`, ptI))
				}

				pt.events <- &kafka.PartitionEnd{Tps: []kafka.TopicPartition{ptI}}
			case librdKafka.Error:
				c.logger.Warn(fmt.Sprintf(`Consume error due to %s`, e))
			default:
				c.logger.Trace("Ignored ", e.String())
			}
		}
	}

	c.closed <- struct{}{}
}

func (c *partitionConsumer) validate(topic string, partition int32, offset int64) (isValid bool, err error) {
	startOffset, err := c.GetOffsetLatest(topic, partition)
	if err != nil {
		return false, fmt.Errorf(`offset validate failed for %s-%d due to %w`, topic, partition, err)
	}

	endOffset, err := c.GetOffsetOldest(topic, partition)
	if err != nil {
		return false, fmt.Errorf(`offset validate failed for %s-%d due to %w`, topic, partition, err)
	}

	return offset >= startOffset && offset < endOffset, nil
}

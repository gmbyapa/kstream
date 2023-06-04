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

type stopSignal struct {
	err error
}

type groupConsumer struct {
	consumer       *librdKafka.Consumer
	config         *GroupConsumerConfig
	consumerErrors chan error
	stopping       chan stopSignal
	groupHandler   kafka.RebalanceHandler

	status kafka.GroupConsumerStatus

	shutdownOnce sync.Once

	assignment *sync.Map

	metricsCollectorTicker *time.Ticker
	metrics                struct {
		consumerBufferCapacity metrics.Gauge
		endToEndLatency        metrics.Observer
		status                 metrics.Gauge
		rebalanceLatency       metrics.Observer
	}
}

type groupConsumerProvider struct {
	config *GroupConsumerConfig
}

func NewGroupConsumerProvider(config *GroupConsumerConfig) kafka.GroupConsumerProvider {
	return &groupConsumerProvider{config: config}
}

func (c *groupConsumerProvider) NewBuilder(conf *kafka.GroupConsumerConfig) kafka.GroupConsumerBuilder {
	c.config.GroupConsumerConfig = conf

	return func(configure func(*kafka.GroupConsumerConfig)) (kafka.GroupConsumer, error) {
		defaultConfCopy := c.config.copy()
		configure(defaultConfCopy.GroupConsumerConfig)

		return NewGroupConsumer(defaultConfCopy)
	}
}

func NewGroupConsumer(config *GroupConsumerConfig) (kafka.GroupConsumer, error) {
	if err := config.setUp(); err != nil {
		return nil, errors.Wrap(err, `group consumer config setup failed`)
	}

	con, err := librdKafka.NewConsumer(config.Librd)
	if err != nil {
		return nil, errors.Wrap(err, `new consumer failed`)
	}

	config.Logger = config.Logger.NewLog(log.Prefixed(`GroupConsumer`))

	return &groupConsumer{
		consumer:       con,
		config:         config,
		consumerErrors: make(chan error, 1),
		stopping:       make(chan stopSignal, 1),
		assignment:     new(sync.Map),
	}, nil
}

func (g *groupConsumer) Subscribe(tps []string, handler kafka.RebalanceHandler) error {
	g.groupHandler = handler
	g.initMetrics()
	g.config.Logger.Info(fmt.Sprintf(`Subscribing to topics %v`, tps))

	go g.printLogs()

	if err := g.consumer.SubscribeTopics(tps, g.rebalance); err != nil {
		return errors.Wrap(err, `consumer subscribe failed`)
	}

	if err := g.consumeMessages(); err != nil {
		return errors.Wrap(err, `consumer close failed`)
	}

	g.config.Logger.Info(`Consumer stopped`)

	return nil
}

func (g *groupConsumer) Unsubscribe() error {
	g.startShutdown()
	return nil
}

func (g *groupConsumer) Errors() <-chan error {
	return g.consumerErrors
}

func (g *groupConsumer) initMetrics() {
	reporter := g.config.MetricsReporter.Reporter(metrics.ReporterConf{Subsystem: `kstream_group_consumer`})
	g.metricsCollectorTicker = time.NewTicker(5 * time.Second)
	constLabels := map[string]string{`consumer_id`: g.config.Id}
	g.metrics.endToEndLatency = reporter.Observer(metrics.MetricConf{
		Path:        "end_to_end_latency_microseconds",
		ConstLabels: constLabels,
		Labels:      []string{`topic_partition`},
	})

	g.metrics.consumerBufferCapacity = reporter.Gauge(metrics.MetricConf{
		Path:        "consumer_buffer_capacity",
		ConstLabels: constLabels,
		Labels:      []string{`topic_partition`},
	})

	g.metrics.rebalanceLatency = reporter.Observer(metrics.MetricConf{
		Path: "rebalance_latency_microseconds",
	})

	g.metrics.status = reporter.Gauge(metrics.MetricConf{
		Path: "rebalance_status",
	})

	g.reportConsumerBufferMetrics()
}

func (g *groupConsumer) consumeMessages() error {
	var err error
MAIN:
	for {
		select {
		case <-g.stopping:
			g.config.Logger.Info(`Stopping consumer loop due to stop signal`)
			break MAIN
		default:
			ev := g.consumer.Poll(int(g.config.MaxPollInterval.Milliseconds()))
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *librdKafka.Message:
				t := time.Since(e.Timestamp)

				record := &Record{librd: e}
				record.ctx = context.Background()
				if g.config.ContextExtractor != nil {
					record.ctx = g.config.ContextExtractor(record)
				}

				g.config.Logger.DebugContext(record.ctx, fmt.Sprintf(`Message %s with key (%s) received in %s`,
					record, record.Key(), t))

				g.metrics.endToEndLatency.Observe(float64(t), map[string]string{
					`topic_partition`: fmt.Sprintf(`%s_%d`, record.Topic(), record.Partition()),
				})

				pId := kafka.TopicPartition{
					Topic:     *e.TopicPartition.Topic,
					Partition: e.TopicPartition.Partition,
				}.String()

				assigmnt, ok := g.assignment.Load(pId)
				if !ok {
					panic(`assignment does not exist`)
				}

				assigmnt.(chan kafka.Record) <- record

			case librdKafka.PartitionEOF:
				g.config.Logger.Info(fmt.Sprintf(`Partition end %s`, e))

			case librdKafka.Error:
				g.config.Logger.Warn(fmt.Sprintf(`Consume error due to %s`, e))
			default:
			}
		}
	}

	g.config.Logger.Info(`Consumer closing...`)
	defer g.config.Logger.Info(`Consumer closed`)

	if err := g.consumer.Close(); err != nil {
		return errors.Wrapf(err, `consumer closer error. ConsumerID: %s`, g.config.Id)
	}

	return err
}

func (g *groupConsumer) rebalance(c *librdKafka.Consumer, event librdKafka.Event) error {
	defer func(since time.Time) {
		g.metrics.rebalanceLatency.Observe(float64(time.Since(since).Microseconds()), nil)
	}(time.Now())

	g.metrics.status.Count(1, nil)

	switch ev := event.(type) {
	case librdKafka.AssignedPartitions:
		if err := g.assign(c, ev.Partitions); err != nil {
			return err
		}

	case librdKafka.RevokedPartitions:
		if c.AssignmentLost() {
			g.config.Logger.Warn(`consumer assignment lost`)
			if err := g.groupHandler.OnLost(); err != nil {
				g.config.Logger.Fatal(err)
			}
		}

		if err := g.revoke(c, ev.Partitions); err != nil {
			return err
		}

	case librdKafka.Error:
		g.metrics.status.Count(-1, nil)
		g.consumerErrors <- ev
		return nil
	}

	g.metrics.status.Count(5, nil)

	return nil
}

func (g *groupConsumer) assign(c *librdKafka.Consumer, partitions []librdKafka.TopicPartition) error {
	g.metrics.status.Count(2, nil)

	assign := newAssignment(partitions)
	g.config.Logger.Warn(fmt.Sprintf(`Partitions %s assigning...`, assign.TPs()))

	session := &groupSession{
		assignment:       assign,
		consumer:         c,
		metaFetchTimeout: int(g.config.TopicMetaFetchTimeout.Milliseconds()),
	}

	if err := g.groupHandler.OnPartitionAssigned(context.Background(), session); err != nil {
		g.config.Logger.Error(
			fmt.Sprintf(`ConsumerGroupHandler OnPartitionAssigned failed due to %s, Consumer will shut down`,
				err))
		g.startShutdownOnErr(err)
		return err
	}

	for _, tp := range session.Assignment().TPs() {
		messageChan := make(chan kafka.Record, 10) //TODO make this configurable

		g.assignment.Store(tp.String(), messageChan)

		go func(tp kafka.TopicPartition) {
			claim := &partitionClaim{
				tp:       tp,
				messages: messageChan,
			}
			if err := g.groupHandler.Consume(context.Background(), session, claim); err != nil {
				g.config.Logger.Error(
					fmt.Sprintf(`ConsumerGroupHandler Consume failed on %s due to %s, Consumer will shut down`,
						tp, err))
				g.startShutdownOnErr(err)
			}
		}(tp)
	}

	librdAssign, err := assign.toLibrd()
	if err != nil {
		g.config.Logger.Error(err.Error())
		g.startShutdownOnErr(err)
		return errors.Wrap(err, `assignment generate failed`)
	}

	err = c.Assign(librdAssign)
	if err != nil {
		g.config.Logger.Error(fmt.Sprintf(
			`SubscribeTopics Assign failed due to %s, Consumer will shut down`, err))
		g.startShutdownOnErr(err)
		return err
	}

	g.config.Logger.Info(fmt.Sprintf(`Partitions %s assigned`, assign.TPs()))

	return nil
}

func (g *groupConsumer) revoke(c *librdKafka.Consumer, partitions []librdKafka.TopicPartition) error {
	g.metrics.status.Count(3, nil)
	assign := newAssignment(partitions)

	var pts []kafka.TopicPartition
	for _, pt := range partitions {
		pts = append(pts, kafka.TopicPartition{
			Topic:     *pt.Topic,
			Partition: pt.Partition,
		})
	}

	g.config.Logger.Warn(fmt.Sprintf(`Partitions %s revoking`, assign.TPs()))
	if err := g.groupHandler.OnPartitionRevoked(context.Background(), &groupSession{
		assignment: assign,
		consumer:   c,
	}); err != nil {
		g.config.Logger.Error(fmt.Sprintf(
			`ReBalanceHandler.OnPartitionRevoked failed due to %s, Consumer will shut down`, err))
		g.startShutdownOnErr(err)
		return err
	}

	err := c.Unassign()
	if err != nil {
		g.config.Logger.Error(fmt.Sprintf(
			`Unassign failed due to %s, Consumer will shut down`, err))
		g.startShutdownOnErr(err)
	}

	for _, pt := range pts {
		ch, _ := g.assignment.LoadAndDelete(pt.String())
		close(ch.(chan kafka.Record))
	}

	g.config.Logger.Info(fmt.Sprintf(`Partitions %s revoked`, assign.TPs()))

	return nil
}

func (g *groupConsumer) startShutdown() {
	g.shutdownOnce.Do(func() {
		g.config.Logger.Info(`Consumer shutting down...`)
		g.stopping <- stopSignal{}
	})
}

func (g *groupConsumer) startShutdownOnErr(err error) {
	g.shutdownOnce.Do(func() {
		g.config.Logger.Error(fmt.Sprintf(`Consumer shutting down due to %s`, err))
		g.stopping <- stopSignal{err: err}
	})
}

func (g *groupConsumer) printLogs() {
	logger := g.config.Logger.NewLog(log.Prefixed(`LibrdKafka`))
	for lg := range g.consumer.Logs() {
		switch lg.Level {
		case 0, 1, 2:
			logger.Error(lg.String())
		case 3, 4, 5:
			logger.Warn(lg.String())
		case 6:
			logger.Info(lg.String())
		case 7:
			logger.Debug(lg.String())
		}
	}
}

func (g *groupConsumer) reportConsumerBufferMetrics() {
	go func() {
		for range g.metricsCollectorTicker.C {
			g.assignment.Range(func(key, value interface{}) bool {
				ch := value.(chan kafka.Record)
				size := float64(len(ch)) * 100 / float64(cap(ch))
				g.metrics.consumerBufferCapacity.Count(size, map[string]string{
					`topic_partition`: key.(string),
				})

				return true
			})
		}
	}()

}

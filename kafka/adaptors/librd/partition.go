package librd

import (
	"fmt"
	librdKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
)

type partition struct {
	events          chan kafka.Event
	consumerErrors  chan error
	consumer        *librdKafka.Consumer
	logger          log.Logger
	metricsReporter metrics.Reporter

	metrics struct {
		//consumerBuffer    metrics.Gauge
		//consumerBufferMax metrics.Gauge
		endToEndLatency metrics.Observer
	}

	topic                        string
	partition                    int32
	partitionStart, partitionEnd int64
	closed                       bool
	mu                           *sync.Mutex
}

func (p *partition) init() {
	// setup metrics
	p.metrics.endToEndLatency = p.metricsReporter.Observer(metrics.MetricConf{
		Path: `end_to_end_latency_microseconds`,
	})
}

func (p *partition) Events() <-chan kafka.Event {
	return p.events
}

func (p *partition) BeginOffset() kafka.Offset {
	return kafka.Offset(p.partitionStart)
}

func (p *partition) EndOffset() kafka.Offset {
	return kafka.Offset(p.partitionEnd)
}

func (p *partition) Close() error {
	p.logger.Info(`Partition closing...`)

	if err := p.consumer.IncrementalUnassign([]librdKafka.TopicPartition{{
		Topic:     &p.topic,
		Partition: p.partition,
	}}); err != nil {
		p.logger.Error(fmt.Sprintf("partition consumer [%s] failed to close", err))
	}

	p.cleanUpMetrics()

	p.logger.Info(`partition closed`)

	return nil
}

func (p *partition) Send(record kafka.Event) {
	p.events <- record
}

func (p *partition) cleanUpMetrics() {
	p.metrics.endToEndLatency.UnRegister()
}

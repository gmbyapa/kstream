package kafka

import (
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type ProducerConfig struct {
	Id               string
	BootstrapServers []string
	PartitionerFunc  PartitionerFunc
	Acks             RequiredAcks
	Transactional    struct {
		Enabled bool
		Id      string
	}
	Idempotent      bool
	Logger          log.Logger
	MetricsReporter metrics.Reporter
}

func (conf *ProducerConfig) Copy() *ProducerConfig {
	return &ProducerConfig{
		Id:               conf.Id,
		BootstrapServers: conf.BootstrapServers,
		PartitionerFunc:  conf.PartitionerFunc,
		Acks:             conf.Acks,
		Transactional:    conf.Transactional,
		Idempotent:       conf.Idempotent,
		Logger:           conf.Logger,
		MetricsReporter:  conf.MetricsReporter,
	}
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Acks:            WaitForAll,
		Logger:          log.NewNoopLogger(),
		MetricsReporter: metrics.NoopReporter(),
	}
}

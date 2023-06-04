package librd

import (
	librdKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/tryfix/log"
	"strings"
)

type ProducerConfig struct {
	Librd *librdKafka.ConfigMap
	*kafka.ProducerConfig
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Librd: &librdKafka.ConfigMap{
			`partitioner`: string(PartitionerConsistentMurmur2),
		},
		ProducerConfig: kafka.NewProducerConfig(),
	}
}

func (conf *ProducerConfig) validate() error {
	return nil
}

func (conf *ProducerConfig) setUp() error {
	if err := conf.Librd.SetKey(`client.id`, conf.Id); err != nil {
		panic(err)
	}

	if err := conf.Librd.SetKey(`bootstrap.servers`, strings.Join(conf.BootstrapServers, `,`)); err != nil {
		panic(err)
	}

	if err := conf.Librd.SetKey(`go.logs.channel.enable`, true); err != nil {
		return errors.New(err.Error())
	}

	if err := conf.Librd.SetKey(`log_level`, toLibrdLogLevel(log.INFO)); err != nil {
		return errors.New(err.Error())
	}

	// Making sure transactional properties are set
	if conf.Transactional.Enabled {
		if err := conf.Librd.SetKey(`enable.idempotence`, true); err != nil {
			panic(err)
		}

		// For transactional producers, delivery success is
		// acknowledged by producer batch commit, so we don't need
		// to listen to individual delivery reports
		if err := conf.Librd.SetKey(`go.delivery.reports`, false); err != nil {
			panic(err)
		}

		//TODO use this to recreate producer transaction timeout scenarios
		//if err := conf.Librd.SetKey(`transaction.timeout.ms`, 2000); err != nil {
		//	panic(err)
		//}

		if err := conf.Librd.SetKey(`transactional.id`, conf.Transactional.Id); err != nil {
			panic(err)
		}

		if err := conf.Librd.SetKey(`max.in.flight.requests.per.connection`, 1); err != nil {
			panic(err)
		}

		if err := conf.Librd.SetKey(`acks`, `all`); err != nil {
			panic(err)
		}
	}

	return nil
}

func (conf *ProducerConfig) copy() *ProducerConfig {
	librdCopy := librdKafka.ConfigMap{}
	for key, val := range *conf.Librd {
		librdCopy[key] = val
	}

	return &ProducerConfig{
		Librd:          &librdCopy,
		ProducerConfig: conf.ProducerConfig.Copy(),
	}
}

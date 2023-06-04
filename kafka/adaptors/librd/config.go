package librd

import (
	librdKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/google/uuid"
	"strings"
)

type GroupConsumerConfig struct {
	*kafka.GroupConsumerConfig
	Librd *librdKafka.ConfigMap
}

func (conf *GroupConsumerConfig) copy() *GroupConsumerConfig {
	librdCopy := librdKafka.ConfigMap{}
	for key, val := range *conf.Librd {
		librdCopy[key] = val
	}

	return &GroupConsumerConfig{
		GroupConsumerConfig: conf.GroupConsumerConfig.Copy(),
		Librd:               &librdCopy,
	}
}

func (conf *GroupConsumerConfig) setUp() error {
	if err := configureLibrd(conf.Librd, conf.ConsumerConfig); err != nil {
		return errors.Wrap(err, `consumer config setup failed`)
	}

	if err := conf.Librd.SetKey(`group.id`, conf.GroupId); err != nil {
		return errors.New(err.Error())
	}

	if err := conf.Librd.SetKey(`enable.auto.offset.store`, conf.Offsets.Commit.Auto); err != nil {
		return errors.New(err.Error())
	}

	if err := conf.Librd.SetKey(`enable.auto.commit`, conf.Offsets.Commit.Auto); err != nil {
		return errors.New(err.Error())
	}

	if err := conf.Librd.SetKey(`auto.commit.interval.ms`, int(conf.Offsets.Commit.Interval.Milliseconds())); err != nil {
		return errors.New(err.Error())
	}

	if err := conf.Librd.SetKey(`go.application.rebalance.enable`, true); err != nil {
		return errors.New(err.Error())
	}

	if conf.EOSEnabled {
		if err := conf.Librd.SetKey(`enable.auto.offset.store`, false); err != nil {
			return errors.New(err.Error())
		}

		if err := conf.Librd.SetKey(`enable.auto.commit`, false); err != nil {
			return errors.New(err.Error())
		}
	}

	var offset string
	switch conf.GroupConsumerConfig.Offsets.Initial {
	case kafka.OffsetLatest:
		offset = `latest`
	case kafka.OffsetEarliest:
		offset = `earliest`
	}

	if err := conf.Librd.SetKey(`auto.offset.reset`, offset); err != nil {
		panic(err)
	}

	if err := conf.Librd.SetKey(`auto.offset.reset`, offset); err != nil {
		panic(err)
	}

	return nil
}

type ConsumerConfig struct {
	*kafka.ConsumerConfig
	Librd *librdKafka.ConfigMap
}

func (conf *ConsumerConfig) copy() *ConsumerConfig {
	librdCopy := librdKafka.ConfigMap{}
	for key, val := range *conf.Librd {
		librdCopy[key] = val
	}

	return &ConsumerConfig{
		ConsumerConfig: conf.ConsumerConfig.Copy(),
		Librd:          &librdCopy,
	}
}

func (conf *ConsumerConfig) setUp() error {
	if conf.EOSEnabled {
		conf.IsolationLevel = kafka.ReadCommitted
	}

	return configureLibrd(conf.Librd, conf.ConsumerConfig)
}

func configureLibrd(librd *librdKafka.ConfigMap, conf *kafka.ConsumerConfig) error {
	if err := librd.SetKey(`client.id`, conf.Id); err != nil {
		return errors.New(err.Error())
	}

	if err := librd.SetKey(`bootstrap.servers`, strings.Join(conf.BootstrapServers, `,`)); err != nil {
		return errors.New(err.Error())
	}

	if err := librd.SetKey(`enable.auto.offset.store`, false); err != nil {
		return errors.New(err.Error())
	}

	if err := librd.SetKey(`enable.auto.commit`, false); err != nil {
		return errors.New(err.Error())
	}

	if err := librd.SetKey(`group.id`, uuid.New().String()); err != nil {
		return errors.New(err.Error())
	}

	switch conf.IsolationLevel {
	case kafka.ReadCommitted:
		if err := librd.SetKey(`isolation.level`, `read_committed`); err != nil {
			return errors.New(err.Error())
		}
	case kafka.ReadUncommitted:
		if err := librd.SetKey(`isolation.level`, `read_uncommitted`); err != nil {
			return errors.New(err.Error())
		}
	}

	return nil
}

func NewGroupConsumerConfig() *GroupConsumerConfig {
	return &GroupConsumerConfig{
		Librd:               defaultLibrdGroupConfig(),
		GroupConsumerConfig: kafka.NewConfig(),
	}
}

func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		Librd:          defaultLibrdConfig(),
		ConsumerConfig: kafka.NewPartitionConsumerConfig(),
	}
}

func defaultLibrdConfig() *librdKafka.ConfigMap {
	return &librdKafka.ConfigMap{
		"enable.partition.eof":   true,
		"go.logs.channel.enable": true,
		"log_level":              7,
		"auto.offset.reset":      "earliest",
	}
}

func defaultLibrdGroupConfig() *librdKafka.ConfigMap {
	return &librdKafka.ConfigMap{
		"session.timeout.ms":            6000,
		"partition.assignment.strategy": "range",
		"go.logs.channel.enable":        true,
		"log_level":                     7,
		"auto.offset.reset":             "latest",
	}
}

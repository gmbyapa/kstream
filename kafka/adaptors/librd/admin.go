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
	"github.com/tryfix/log"
	"strings"
	"time"
)

type adminOptions struct {
	BootstrapServers []string
	Timeout          time.Duration
	Logger           log.Logger
}

func (opts *adminOptions) apply(options ...AdminOption) {
	opts.Logger = log.NewNoopLogger()
	opts.Timeout = 10 * time.Second
	for _, opt := range options {
		opt(opts)
	}
}

type AdminOption func(*adminOptions)

func WithLogger(logger log.Logger) AdminOption {
	return func(options *adminOptions) {
		options.Logger = logger
	}
}

func WithTimeout(duration time.Duration) AdminOption {
	return func(options *adminOptions) {
		options.Timeout = duration
	}
}

type kAdmin struct {
	admin   *librdKafka.AdminClient
	logger  log.Logger
	timeout time.Duration

	tempTopicConfigs map[string]*kafka.Topic
}

func NewAdmin(bootstrapServer []string, options ...AdminOption) *kAdmin {
	opts := new(adminOptions)
	opts.apply(options...)
	config := &librdKafka.ConfigMap{
		`bootstrap.servers`: strings.Join(bootstrapServer, `,`),
	}
	logger := opts.Logger.NewLog(log.Prefixed(`kafka-admin`))
	admin, err := librdKafka.NewAdminClient(config)
	if err != nil {
		logger.Fatal(fmt.Sprintf(`cannot get controller - %+v`, err))
	}

	return &kAdmin{
		admin:            admin,
		logger:           logger,
		timeout:          opts.Timeout,
		tempTopicConfigs: map[string]*kafka.Topic{},
	}
}

func (a *kAdmin) FetchInfo(topics []string) (map[string]*kafka.Topic, error) {
	// prepare resources
	var resources []librdKafka.ConfigResource

	topicInfo, err := a.fetchInfo(topics)
	if err != nil {
		return nil, err
	}

	for _, meta := range topicInfo {
		resources = append(resources, librdKafka.ConfigResource{
			Type: librdKafka.ResourceTopic,
			Name: meta.Name,
		})
	}

	topicConfigs, err := a.admin.DescribeConfigs(context.Background(), resources,
		librdKafka.SetAdminRequestTimeout(a.timeout),
	)
	if err != nil {
		return nil, errors.Wrap(err, `cannot get metadata`)
	}

	for _, tp := range topicConfigs {
		topicInfo[tp.Name].ConfigEntries[`cleanup.policy`] = tp.Config[`cleanup.policy`].Value
		topicInfo[tp.Name].ConfigEntries[`min.insync.replicas`] = tp.Config[`min.insync.replicas`].Value
		topicInfo[tp.Name].ConfigEntries[`retention.ms`] = tp.Config[`retention.ms`].Value
	}

	return topicInfo, nil
}

func (a *kAdmin) verifyExists(topics []string) (bool, error) {
	// prepare resources
	var resources []librdKafka.ConfigResource

	for _, topic := range topics {
		resources = append(resources, librdKafka.ConfigResource{
			Type: librdKafka.ResourceTopic,
			Name: topic,
		})
	}

	topicConfigs, err := a.admin.DescribeConfigs(context.Background(), resources) //librdKafka.SetAdminRequestTimeout(a.timeout),

	if err != nil {
		return false, errors.Wrap(err, `cannot get metadata`)
	}

	return len(topicConfigs) == len(topics), nil
}

func (a *kAdmin) fetchInfo(topics []string) (map[string]*kafka.Topic, error) {
	topicMeta, err := a.admin.GetMetadata(nil, true, int(a.timeout.Milliseconds()))
	if err != nil {
		return nil, errors.Wrap(err, `cannot get metadata`)
	}

	topicInfo := map[string]*kafka.Topic{}

	for _, meta := range topicMeta.Topics {
		ignored := true
		for _, topic := range topics {
			if meta.Topic == topic {
				ignored = false
			}
		}

		if ignored {
			continue
		}

		if meta.Error.Code() != librdKafka.ErrNoError {
			return nil, err
		}

		var partitions []kafka.PartitionConf
		var replicaCount int
		for _, pt := range meta.Partitions {
			partitions = append(partitions, kafka.PartitionConf{
				Id:    pt.ID,
				Error: pt.Error,
			})
			replicaCount = len(pt.Replicas)
		}

		topicInfo[meta.Topic] = &kafka.Topic{
			Name:              meta.Topic,
			Partitions:        partitions,
			NumPartitions:     int32(len(partitions)),
			ReplicationFactor: int16(replicaCount),
			ConfigEntries:     map[string]string{},
		}
	}

	return topicInfo, nil
}

func (a *kAdmin) CreateTopics(topics []*kafka.Topic) error {
	var tpNames []string
	var specifications []librdKafka.TopicSpecification
	for _, info := range topics {
		tpNames = append(tpNames, info.Name)

		specification := librdKafka.TopicSpecification{
			Topic:             info.Name,
			NumPartitions:     int(info.NumPartitions),
			ReplicationFactor: int(info.ReplicationFactor),
			Config:            info.ConfigEntries,
		}

		specifications = append(specifications, specification)
	}

	result, err := a.admin.CreateTopics(context.Background(), specifications,
		librdKafka.SetAdminRequestTimeout(a.timeout*time.Second))
	if err != nil {
		return errors.Wrapf(err, `could not create topics [%v]`, topics)
	}

	for _, res := range result {
		if res.Error.Code() != librdKafka.ErrNoError {
			if res.Error.Code() == librdKafka.ErrTopicAlreadyExists {
				a.logger.Warn(fmt.Sprintf(`Topic already exists %s create %s`, res.Topic, res.Error))
				continue
			}

			return errors.Wrapf(res.Error, `topic create error response for [%s]`, res.Topic)
		}
	}

	// After create request returns, it might take a couple of seconds for the broker to be aware of the creation.
	// So lets wait to verify that
	a.verifyAction(`creation`, tpNames)

	a.logger.Info(`k-stream.kafkaAdmin`,
		fmt.Sprintf(`kafkaAdmin topics [%v] created`, tpNames))

	return nil
}

func (a *kAdmin) StoreConfigs(topics []*kafka.Topic) error {
	for _, topic := range topics {
		if _, ok := a.tempTopicConfigs[topic.Name]; ok {
			return errors.Errorf(`topic %s already marked for creation`, topic.Name)
		}

		a.tempTopicConfigs[topic.Name] = topic
	}

	return nil
}

func (a *kAdmin) ApplyConfigs() error {
	var topics []*kafka.Topic
	for _, tp := range a.tempTopicConfigs {
		topics = append(topics, tp)
	}

	return a.CreateTopics(topics)
}

func (a *kAdmin) DeleteTopics(topics []string) error {
	result, err := a.admin.DeleteTopics(context.Background(), topics,
		librdKafka.SetAdminOperationTimeout(a.timeout))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf(`could not delete topics [%v]`, topics))
	}

	for _, res := range result {
		if res.Error.Code() != librdKafka.ErrNoError {
			if res.Error.Code() == librdKafka.ErrUnknownTopic || res.Error.Code() == librdKafka.ErrUnknownTopicOrPart {
				a.logger.Warn(res.Error)
				continue
			}

			return errors.Wrapf(res.Error, `DeleteTopics topic delete error response for [%s]`, res.Topic)
		}
	}

	// After delete request returns, it might take a couple of seconds for the broker to be aware of the deletion.
	// So lets wait to verify that
	a.verifyAction(`deletion`, topics)

	return nil
}

func (a *kAdmin) ListTopics() ([]string, error) {
	var topics []string
	topicMeta, err := a.admin.GetMetadata(nil, true, int(a.timeout.Milliseconds()))
	if err != nil {
		return nil, errors.Wrap(err, `cannot get metadata`)
	}

	for _, meta := range topicMeta.Topics {
		topics = append(topics, meta.Topic)
	}

	return topics, nil
}

func (a *kAdmin) verifyAction(action string, topics []string) {
	a.logger.Info(fmt.Sprintf(`Topic [%s] still in progress for topics %v. Waiting...`, action, topics))
	time.Sleep(1 * time.Second)
	exists, err := a.verifyExists(topics)
	if err != nil {
		a.logger.Error(err.Error())
		a.verifyAction(action, topics)
	}

	if action == `deletion` {
		if exists {
			a.verifyAction(action, topics)
		}
	} else {
		if !exists {
			a.verifyAction(action, topics)
		}
	}
}

func (a *kAdmin) Close() {
	a.admin.Close()
}

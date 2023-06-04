package streams

import (
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"sync"
)

type streamConsumer struct {
	consumerCount int
	groupConsumer kafka.GroupConsumerBuilder
	logger        log.Logger
	ctx           topology.BuilderContext

	taskManager tasks.TaskManager

	consumers []*streamConsumerInstance
	running   chan struct{}
}

func (r *streamConsumer) Init(_ topology.Topology) error { return nil }

func (r *streamConsumer) Run(topologyBuilder topology.Topology) error {
	r.logger.Info(`StreamConsumer starting...`)

	// Get topic meta
	meta, err := r.ctx.Admin().FetchInfo(topologyBuilder.StreamTopologies().SourceTopics())
	if err != nil {
		return errors.Wrap(err, `topics meta fetch failed`)
	}

	var tps []kafka.TopicPartition
	for _, tp := range meta {
		for _, partition := range tp.Partitions {
			tps = append(tps, kafka.TopicPartition{
				Topic:     tp.Name,
				Partition: partition.Id,
			})
		}
	}

	// Generate task list
	generation := new(tasks.Generator).Generate(tps, topologyBuilder)
	r.logger.Info(fmt.Sprintf("Task list generated -> \n%s", generation.Mappings()))

	wg := &sync.WaitGroup{}
	for i := 1; i <= r.consumerCount; i++ {
		logger := r.logger.NewLog(log.Prefixed(fmt.Sprintf(`StreamConsumer#%d`, i)))
		consumer, err := r.groupConsumer(func(config *kafka.GroupConsumerConfig) {
			config.Logger = logger
			config.Id = fmt.Sprintf(`consumer#%d`, i)
		})
		if err != nil {
			return err
		}

		instance := &streamConsumerInstance{
			topologyBuilder: topologyBuilder,
			ctx:             r.ctx,
			generation:      generation,
			logger:          logger,
			taskManager:     r.taskManager,
			consumer:        consumer,
		}

		r.consumers = append(r.consumers, instance)

		wg.Add(1)
		go func(instance *streamConsumerInstance) {
			if err := instance.Subscribe(); err != nil {
				panic(err)
			}
			wg.Done()
		}(instance)
	}

	wg.Wait()

	close(r.running)

	return nil
}

func (r *streamConsumer) Ready() error {
	return nil
}

func (r *streamConsumer) Stop() error {
	r.logger.Info(`StreamConsumer stopping...`)
	defer r.logger.Info(`StreamConsumer stopped`)

	for _, instance := range r.consumers {
		go func(i *streamConsumerInstance) {
			if err := i.Unsubscribe(); err != nil {
				r.logger.Error(err)
			}
		}(instance)
	}

	<-r.running

	return nil
}

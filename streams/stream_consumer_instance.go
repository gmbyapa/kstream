package streams

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"sync"
)

type streamConsumerInstance struct {
	topologyBuilder   topology.Topology
	generation        tasks.TaskGeneration
	currentAssignment tasks.TaskAssignment
	ctx               topology.BuilderContext

	logger log.Logger

	taskManager tasks.TaskManager
	consumer    kafka.GroupConsumer
}

func (r *streamConsumerInstance) OnPartitionRevoked(ctx context.Context, session kafka.GroupSession) error {
	r.logger.Info(fmt.Sprintf("Removing tasks -> \n%s", r.currentAssignment))
	wg := sync.WaitGroup{}
	wg.Add(len(r.currentAssignment))
	for _, mapping := range r.currentAssignment {
		go func(mapping *tasks.TaskMapping) {
			defer wg.Done()
			if err := r.taskManager.RemoveTask(mapping.TaskId()); err != nil {
				r.logger.Fatal(err)
			}
		}(mapping)
	}
	wg.Wait()

	return nil
}

func (r *streamConsumerInstance) OnPartitionAssigned(_ context.Context, session kafka.GroupSession) error {
	r.currentAssignment = r.generation.Assign(session.Assignment().TPs()...)
	r.logger.Info(fmt.Sprintf("Assigning tasks -> \n%s", r.currentAssignment))

	wg := sync.WaitGroup{}
	wg.Add(len(r.currentAssignment))

	// Apply offset resets (if any)
	for _, tp := range session.Assignment().TPs() {
		if src := r.topologyBuilder.SourceByTopic(tp.Topic); src != nil {
			session.Assignment().ResetOffset(tp, src.InitialOffset())
		}
	}

	// Restore tasks
	for _, mapping := range r.currentAssignment {
		go func(wg *sync.WaitGroup, mapping *tasks.TaskMapping) {
			defer wg.Done()
			_, err := r.taskManager.AddTask(r.ctx, mapping.TaskId(), mapping.SubTopologyBuilder(), session)
			if err != nil {
				panic(err)
			}

		}(&wg, mapping)
	}
	wg.Wait()

	r.logger.Info(`Tasks restored`)

	// Init tasks
	wg = sync.WaitGroup{}
	wg.Add(len(r.currentAssignment))
	for _, mapping := range r.currentAssignment {
		go func(wg *sync.WaitGroup, mapping *tasks.TaskMapping) {
			defer wg.Done()
			tsk, err := r.taskManager.Task(mapping.TaskId())
			if err != nil {
				panic(err.Error())
			}

			if err := tsk.Init(); err != nil {
				panic(err.Error())
			}

		}(&wg, mapping)
	}
	wg.Wait()
	r.logger.Info(`Tasks inited`)

	return nil
}

func (r *streamConsumerInstance) OnLost() error {
	r.logger.Info(`Consumer assignment lost`)
	return nil
}

func (r *streamConsumerInstance) Consume(ctx context.Context, session kafka.GroupSession, claim kafka.PartitionClaim) error {
	r.logger.Info(fmt.Sprintf(`Consuming messages for %s`, claim.TopicPartition()))
	defer r.logger.Info(fmt.Sprintf(`Consuming messages stopped for %s`, claim.TopicPartition()))

	mapping := r.generation.FindMappingByTP(claim.TopicPartition())
	task, err := r.taskManager.Task(mapping.TaskId())
	if err != nil {
		return errors.Wrapf(err, `task %s error`, mapping.TaskId())
	}

	task.Start(ctx, claim, session)

	return nil
}

func (r *streamConsumerInstance) Subscribe() error {
	if err := r.consumer.Subscribe(r.topologyBuilder.StreamTopologies().SourceTopics(), r); err != nil {
		r.logger.Error(err)
		return err
	}

	return nil
}

func (r *streamConsumerInstance) Unsubscribe() error {
	if err := r.consumer.Unsubscribe(); err != nil {
		return err
	}

	r.logger.Info(`Consumer unsubscribing...`)

	return nil
}

package streams

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/async"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
)

type GlobalTableConsumer struct {
	consumer    kafka.PartitionConsumer
	logger      log.Logger
	taskManager tasks.TaskManager
	ctx         topology.BuilderContext
	runGroup    *async.RunGroup
}

// Init initializes the GlobalTableConsumer with the given topology and starts consuming
// messages from the sources of each GlobalTableTopology in the topology. For each source,
// it fetches the available partitions, creates a new task with the GlobalTableTopology and
// adds it to the task manager. Then, it creates two goroutines to run the task: one to stop
// the task when the program is shutting down and another to start the task and log any errors
// that occur. Finally, it adds the task to the runGroup.
//
// Parameters:
// - topologyBuilder: the topology builder to use for creating GlobalTableTopologies
//
// Returns:
// - error: if there was an error fetching partitions, adding a task to the task manager, or
//          if the task failed to start.
func (g *GlobalTableConsumer) Init(topologyBuilder topology.Topology) error {
	for _, builder := range topologyBuilder.GlobalTableTopologies() {
		// Get the topic for the first source in the GlobalTableTopology.
		// Assumes that each GlobalTableTopology has exactly one source.
		topic := builder.Sources()[0].Topic()

		// Fetch the available partitions for the topic
		partitions, err := g.consumer.Partitions(context.Background(), topic)
		if err != nil {
			return errors.Wrapf(err, `cannot fetch partition for topic [%s]`, topic)
		}

		for _, partition := range partitions {
			taskId := g.taskManager.NewTaskId(`Global`, kafka.TopicPartition{
				Topic:     topic,
				Partition: partition,
			})
			globalTask, err := g.taskManager.AddGlobalTask(g.ctx, taskId, builder)
			if err != nil {
				return errors.Wrapf(err, `globalTask[%s] add failed, topic: %s`, taskId, topic)
			}

			g.runGroup.Add(func(opts *async.Opts) error {
				// Create two goroutines to run the task:
				// 1. A goroutine to stop the task when the RunGroup is stopping
				// 2. A goroutine to start the task and log any errors that occur
				go func(task tasks.Task) {
					defer async.LogPanicTrace(g.logger)

					<-opts.Stopping()
					if err := task.Stop(); err != nil {
						panic(err.Error())
					}
				}(globalTask)

				go func(task tasks.Task) {
					defer async.LogPanicTrace(g.logger)

					// Once the GlobalTask is ready mark the RunGroup process as ready
					if err := task.Ready(); err != nil && !errors.Is(err, async.ErrInterrupted) {
						g.logger.Error(err.Error())
					}
					opts.Ready()
				}(globalTask)

				return globalTask.Sync()
			})
		}
	}

	return nil
}

func (g *GlobalTableConsumer) Run(_ topology.Topology) error {
	g.logger.Info(`Starting...`)
	return g.runGroup.Run()
}

func (g *GlobalTableConsumer) Ready() error {
	defer g.logger.Info(`Started`)
	return g.runGroup.Ready()
}

func (g *GlobalTableConsumer) Stop() error {
	g.runGroup.Stop()
	return nil
}

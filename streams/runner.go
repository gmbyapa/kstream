package streams

import (
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/async"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/stores"
	"github.com/gmbyapa/kstream/streams/tasks"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"sync"
)

type RunnerOpt func(runner *streamRunner)

type Runner interface {
	Run(topology topology.Topology, opts ...RunnerOpt) error
	Stop() error
}

func NotifyGlobalStoresReady(ch chan struct{}) RunnerOpt {
	return func(runner *streamRunner) {
		if len(runner.topology.GlobalTableTopologies()) < 1 {
			panic("cannot use NotifyGlobalStoreReady when there are no GlobalTableTopologies")
		}
		runner.globalStateStoresReady = ch
	}
}

// run consumer and get allocation
// on re-balance start or stop the applications
type streamRunner struct {
	ctx topology.BuilderContext

	consumerCount      int
	groupConsumer      kafka.GroupConsumerBuilder
	partitionConsumer  kafka.ConsumerBuilder
	taskManagerBuilder func(logger log.Logger, topologies topology.SubTopologyBuilders) (tasks.TaskManager, error)

	topology topology.Topology

	logger log.Logger

	consumers struct {
		global, stream Consumer
	}

	shutDownOnce           sync.Once
	globalStateStoresReady chan struct{}
}

func (r *streamRunner) Run(topology topology.Topology, opts ...RunnerOpt) error {
	r.logger.Info(`StreamRunner starting...`)
	defer r.logger.Info(`StreamRunner stopped`)

	r.topology = topology

	// Apply opts
	for _, opt := range opts {
		opt(r)
	}

	// Start StoreRegistry HTTP server
	r.ctx.StoreRegistry().StartWebServer()

	wg := &sync.WaitGroup{}
	if len(topology.GlobalTableTopologies()) > 0 {
		logger := r.logger.NewLog(log.Prefixed(`GlobalTableConsumer`))
		pc, err := r.partitionConsumer(func(config *kafka.ConsumerConfig) {
			config.Logger = logger
		})
		if err != nil {
			return err
		}

		taskManager, err := r.taskManagerBuilder(
			r.logger.NewLog(log.Prefixed(`GlobalTaskManager`)),
			topology.GlobalTableTopologies(),
		)
		if err != nil {
			return err
		}

		globalConsumer := &GlobalTableConsumer{
			consumer:    pc,
			logger:      logger,
			taskManager: taskManager,
			ctx:         r.ctx,
			runGroup:    async.NewRunGroup(r.logger),
		}

		r.consumers.global = globalConsumer

		if err := r.consumers.global.Init(topology); err != nil {
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := globalConsumer.Run(topology); err != nil {
				r.shutdown(err)
				return
			}
		}()

		err = globalConsumer.Ready()
		// Signal GlobalStateStore synced, regardless of error
		r.notifyGlobalStoreSynced()
		if err != nil {
			if !errors.Is(err, async.ErrInterrupted) {
				return errors.Wrap(err, `globalConsumer error`)
			}
			r.logger.Info(`GlobalTableStream stopped`)
		} else {
			r.logger.Info(`GlobalTableStream started`)
		}
	}

	if len(topology.StreamTopologies()) > 0 {
		tManager, err := r.taskManagerBuilder(
			r.logger.NewLog(log.Prefixed(`TaskManager`)),
			topology.StreamTopologies(),
		)
		if err != nil {
			return err
		}

		consumer := &streamConsumer{
			consumerCount: r.consumerCount,
			groupConsumer: r.groupConsumer,
			logger:        r.logger,
			ctx:           r.ctx,
			taskManager:   tManager,
			running:       make(chan struct{}),
		}

		r.consumers.stream = consumer

		if err := r.registerStores(tManager); err != nil {
			return err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := consumer.Run(topology); err != nil {
				r.shutdown(err)
				return
			}
		}()
	}

	wg.Wait()

	return nil
}

func (r *streamRunner) Stop() error {
	r.shutdown(nil)
	return nil
}

func (r *streamRunner) GlobalStoresReady() chan struct{} {
	return r.globalStateStoresReady
}

func (r *streamRunner) shutdown(err error) {
	r.shutDownOnce.Do(func() {
		if err != nil {
			r.logger.Warn(fmt.Sprintf(`StreamRunner stopping due to %s ...`, err))
		} else {
			r.logger.Info(`StreamRunner stopping...`)
		}

		// first shutdown stream consumer if started
		if r.consumers.stream != nil {
			if err := r.consumers.stream.Stop(); err != nil {
				r.logger.Error(err)
			}
		}

		if r.consumers.global != nil {
			if err := r.consumers.global.Stop(); err != nil {
				r.logger.Error(err)
			}
		}

		r.logger.Info(`StreamRunner stopped`)
	})
}

func (r *streamRunner) getStores() []topology.LoggableStoreBuilder {
	// get the store builder
	var builders []topology.LoggableStoreBuilder
	for _, subTp := range r.topology.StreamTopologies() {
		for _, store := range subTp.StateStores() {
			builders = append(builders, store)
		}
	}

	return builders
}

func (r *streamRunner) registerStores(taskManager tasks.TaskManager) error {
	storeBuilders := r.getStores()
	for _, builder := range storeBuilders {
		wrapper := &LocalQueryableStoreWrapper{
			storeBuilder: builder,
			taskManager:  taskManager,
		}

		if err := r.ctx.StoreRegistry().Register(wrapper); err != nil {
			return err
		}

		if err := r.ctx.StoreRegistry().RegisterDynamic(wrapper.Name(), func() []stores.ReadOnlyStore {
			var rdStrs []stores.ReadOnlyStore
			for _, str := range wrapper.Instances() {
				rdStrs = append(rdStrs, str)
			}

			return rdStrs
		}); err != nil {
			return err
		}
	}

	return nil
}

func (r *streamRunner) notifyGlobalStoreSynced() {
	if r.globalStateStoresReady != nil {
		close(r.globalStateStoresReady)
	}
}

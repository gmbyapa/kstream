package tasks

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/pkg/async"
	"github.com/gmbyapa/kstream/v2/pkg/errors"
	"github.com/gmbyapa/kstream/v2/streams/topology"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics/v2"
	"sync"
)

type TaskManager interface {
	NewTaskId(prefix string, tp kafka.TopicPartition) TaskID
	AddTask(ctx topology.BuilderContext, consumerID string, id TaskID, topology topology.SubTopologyBuilder, session kafka.GroupSession) (Task, error)
	AddGlobalTask(ctx topology.BuilderContext, id TaskID, topology topology.SubTopologyBuilder) (Task, error)
	RemoveTask(id TaskID) error
	Task(id TaskID) (Task, error)
	StoreInstances(name string) []topology.StateStore
}

type taskManager struct {
	logger            log.Logger
	tasks             *sync.Map
	partitionConsumer kafka.PartitionConsumer
	builderCtx        topology.BuilderContext
	transactional     bool
	topicConfigs      map[string]*kafka.Topic

	ctxCancel context.CancelFunc
	//mu        sync.Mutex
	taskOpts []TaskOpt
}

func NewTaskManager(
	builderCtx topology.BuilderContext,
	logger log.Logger,
	partitionConsumer kafka.PartitionConsumer,
	topologies topology.SubTopologyBuilders,
	transactional bool,
	taskOpts ...TaskOpt,
) (TaskManager, error) {
	// Get topic meta for all the topics
	tpConfigs, err := builderCtx.Admin().FetchInfo(topologies.Topics())
	if err != nil {
		return nil, errors.Wrapf(err, `Topic meta fetch failed for %v`, topologies.Topics())
	}

	return &taskManager{
		tasks:             &sync.Map{},
		builderCtx:        builderCtx,
		partitionConsumer: partitionConsumer,
		logger:            logger,
		transactional:     transactional,
		topicConfigs:      tpConfigs,
		taskOpts:          taskOpts,
	}, nil
}

func (t *taskManager) AddTask(ctx topology.BuilderContext, consumerID string, id TaskID, tp topology.SubTopologyBuilder, session kafka.GroupSession) (Task, error) {
	return t.addTask(ctx, consumerID, id, tp, session)
}

func (t *taskManager) AddGlobalTask(ctx topology.BuilderContext, id TaskID, tp topology.SubTopologyBuilder) (Task, error) {
	return t.addGlobalTask(ctx, id, tp)
}

func (t *taskManager) addTask(ctx topology.BuilderContext, consumerID string, id TaskID, subTopology topology.SubTopologyBuilder, session kafka.GroupSession) (Task, error) {
	// If the task already exists, close it
	if tsk, ok := t.tasks.Load(id.String()); ok {
		t.logger.Warn(fmt.Sprintf(`task %s already exists. closing...`, id))
		if err := tsk.(Task).Stop(); err != nil {
			return nil, errors.Wrap(err, `task close error`)
		}
	}

	metricsPrefix := "task_manager_task"

	logger := t.logger.NewLog(log.Prefixed(id.String()))
	producer, err := ctx.ProducerBuilder()(func(config *kafka.ProducerConfig) {
		txID := fmt.Sprintf(`%s-%s(%s)`, ctx.ApplicationId(), id.String(), id.UniqueID())
		config.Id = txID
		config.Transactional.Id = txID
		config.Logger = logger
		config.MetricsReporter = ctx.MetricsReporter().Reporter(metrics.ReporterConf{
			Subsystem: metricsPrefix,
			ConstLabels: map[string]string{
				`task_id`: id.String(),
			},
		})
	})
	if err != nil {
		return nil, errors.Wrap(err, `task build failed`)
	}

	topologyCtx := topology.NewSubTopologyContext(
		context.Background(),
		id.Partition(),
		ctx,
		producer,
		t.partitionConsumer,
		logger,
		t.topicConfigs,
	)
	subTp, err := subTopology.Build(topologyCtx)
	if err != nil {
		return nil, err
	}

	taskOpts := new(taskOptions)
	taskOpts.setDefault()
	taskOpts.failedMessageHandler = func(err error, record kafka.Record) {
		t.logger.ErrorContext(record.Ctx(), fmt.Sprintf(`Message %s failed due to %s`, record, err))
	}
	taskOpts.apply(t.taskOpts...)

	tsk := &task{
		id:                    id,
		logger:                logger,
		ctx:                   topologyCtx,
		session:               session,
		subTopology:           subTp,
		producer:              producer,
		processingStopping:    make(chan struct{}),
		processingLoopStopped: make(chan struct{}),
		closing:               make(chan struct{}),
		ready:                 make(chan struct{}),
		dataChan:              make(chan *Record, 10),
		changelogs:            async.NewRunGroup(logger),
		options:               taskOpts,
	}

	tsk.metrics.reporter = ctx.MetricsReporter().Reporter(metrics.ReporterConf{
		Subsystem: metricsPrefix,
		ConstLabels: map[string]string{
			`sub_topology_id`: subTopology.Id().String(),
			`consumer_id`:     consumerID,
			`task_id`:         id.String(),
		},
	})

	tsk.commitBuffer = newCommitBuffer(
		subTp,
		producer,
		session,
		logger.NewLog(log.Prefixed(`Buffer`)),
		tsk.metrics.reporter.Reporter(metrics.ReporterConf{
			System:      "buffer",
			ConstLabels: map[string]string{},
		}),
	)

	tsk.setup()

	var task Task = tsk

	if err := task.Restore(); err != nil {
		return nil, errors.Wrap(err, `task restore failed`)
	}

	t.tasks.Store(id.String(), task)

	return task, nil
}

func (t *taskManager) addGlobalTask(ctx topology.BuilderContext, id TaskID, subTpB topology.SubTopologyBuilder) (Task, error) {
	logger := t.logger.NewLog(log.Prefixed(id.String()))

	txManageCtx, cancel := context.WithCancel(context.Background())
	t.ctxCancel = cancel

	topologyCtx := topology.NewSubTopologyContext(
		txManageCtx,
		id.Partition(),
		ctx,
		nil,
		t.partitionConsumer,
		logger,
		t.topicConfigs,
	)
	subTp, err := subTpB.Build(topologyCtx)
	if err != nil {
		return nil, err
	}

	taskOpts := new(taskOptions)
	taskOpts.setDefault()
	taskOpts.failedMessageHandler = func(err error, record kafka.Record) {
		t.logger.ErrorContext(record.Ctx(), fmt.Sprintf(`Message %s failed due to %s`, record, err))
	}
	taskOpts.apply(t.taskOpts...)

	tsk := &task{
		id:                 id,
		logger:             logger,
		subTopology:        subTp,
		global:             true,
		ctx:                topologyCtx,
		processingStopping: make(chan struct{}),
		closing:            make(chan struct{}),
		changelogs:         async.NewRunGroup(logger),
		options:            taskOpts,
	}

	tsk.metrics.reporter = ctx.MetricsReporter().Reporter(metrics.ReporterConf{
		Subsystem: "task_manager_global_task",
		ConstLabels: map[string]string{
			`task_id`: id.String(),
		},
	})

	globalKTask := &globalTask{tsk}
	globalKTask.setup()

	if err := globalKTask.Init(); err != nil {
		return nil, err
	}

	t.tasks.Store(id.String(), globalKTask)

	return globalKTask, nil
}

func (t *taskManager) RemoveTask(id TaskID) error {
	tsk, ok := t.tasks.Load(id.String())
	if !ok {
		t.logger.Warn(fmt.Sprintf(`task [%s] doesn't exists. Ignoring`, id))
		return nil
	}

	if err := tsk.(Task).Stop(); err != nil {
		return errors.Wrap(err, `task stop failed`)
	}

	t.tasks.Delete(id.String())

	t.logger.Info(fmt.Sprintf(`%s successfully removed`, id))

	return nil
}

func (t *taskManager) Task(id TaskID) (Task, error) {
	tsk, ok := t.tasks.Load(id.String())
	if !ok {
		return nil, errors.Errorf(`task [%s] doesn't exists`, id)
	}

	return tsk.(Task), nil
}

func (t *taskManager) NewTaskId(prefix string, tp kafka.TopicPartition) TaskID {
	if prefix != `` {
		prefix = fmt.Sprintf(`%s-`, prefix)
	}

	return taskId{
		prefix:    fmt.Sprintf(`%sTask(%s)`, prefix, tp),
		partition: tp.Partition,
	}
}

func (t *taskManager) StoreInstances(name string) []topology.StateStore {
	var stors []topology.StateStore
	t.tasks.Range(func(key, value interface{}) bool {
		if stor := value.(Task).Store(name); stor != nil {
			stors = append(stors, stor)
		}
		return true
	})

	return stors
}

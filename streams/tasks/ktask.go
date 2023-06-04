package tasks

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/pkg/errors"
	"sync"
	"time"

	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/async"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
)

type FailedMessageHandler func(err error, record kafka.Record)

var recordContextTaskIDKey string

type TaskContext struct {
	context.Context
}

func (ctx TaskContext) TaskID() string {
	return ctx.Value(&recordContextTaskIDKey).(string)
}

type taskOptions struct {
	buffer               BufferConfig
	failedMessageHandler FailedMessageHandler
}

func (tOpts *taskOptions) setDefault() {
	// Set default opts
	tOpts.buffer.FlushInterval = 1 * time.Second
	tOpts.buffer.Size = 1000
	tOpts.failedMessageHandler = func(err error, record kafka.Record) {}
}

func (tOpts *taskOptions) apply(opts ...TaskOpt) {
	for _, opt := range opts {
		opt(tOpts)
	}
}

type TaskOpt func(*taskOptions)

func WithBufferFlushInterval(interval time.Duration) TaskOpt {
	return func(options *taskOptions) {
		options.buffer.FlushInterval = interval
	}
}

func WithBufferSize(size int) TaskOpt {
	return func(options *taskOptions) {
		options.buffer.Size = size
	}
}

func WithFailedMessageHandler(handler FailedMessageHandler) TaskOpt {
	return func(options *taskOptions) {
		options.failedMessageHandler = handler
	}
}

type TaskID interface {
	String() string
	UniqueID() string
	Partition() int32
	Topics() string
}

type Task interface {
	ID() TaskID
	Init() error
	Restore() error
	Sync() error
	Ready() error
	Start(ctx context.Context, claim kafka.PartitionClaim, groupSession kafka.GroupSession)
	Store(name string) topology.StateStore
	Stop() error
}

type taskId struct {
	id        int
	hash      string
	prefix    string
	partition int32
	topics    string
}

func (t taskId) UniqueID() string {
	return t.hash
}

func (t taskId) String() string {
	return fmt.Sprintf(`%s#%d`, t.prefix, t.id)
}

func (t taskId) Topics() string {
	return t.topics
}

func (t taskId) Partition() int32 {
	return t.partition
}

type task struct {
	id                 TaskID
	subTopology        topology.SubTopology
	global             bool
	session            kafka.GroupSession
	blockingMode       bool
	logger             log.Logger
	processingStopping chan struct{}
	closing            chan struct{}
	ready              chan struct{}

	dataChan chan *Record

	commitBuffer Buffer

	options *taskOptions

	ctx topology.SubTopologyContext

	consumerLoops sync.WaitGroup

	producer kafka.Producer
	metrics  struct {
		reporter                              metrics.Reporter
		initCount                             metrics.Counter
		stateStoreRecoveryLatencyMilliseconds metrics.Observer
		processLatencyMicroseconds            metrics.Observer
		batchProcessLatencyMicroseconds       metrics.Observer
		batchFlushLatencyMicroseconds         metrics.Observer
		batchSize                             metrics.Gauge
	}

	shutDownOnce sync.Once
	changelogs   *async.RunGroup
}

func (t *task) ID() TaskID {
	return t.id
}

func (t *task) Restore() error {
	labels := map[string]string{`topics`: t.ID().Topics(), `partition`: fmt.Sprintf(`%d`, t.ID().Partition())}
	t.metrics.stateStoreRecoveryLatencyMilliseconds = t.metrics.reporter.Observer(metrics.MetricConf{
		Path:        `state_store_recovery_latency_milliseconds`,
		ConstLabels: labels,
		Labels:      []string{`store`},
	})

	// Each StateStore instance in the task has to be restored before the processing start
	for _, store := range t.subTopology.StateStores() {
		changelog := store
		t.changelogs.Add(func(opts *async.Opts) error {
			defer func(start time.Time) {
				t.metrics.stateStoreRecoveryLatencyMilliseconds.Observe(float64(time.Since(start).Milliseconds()),
					map[string]string{`store`: changelog.Name()})
				opts.Ready()
			}(time.Now())

			stateSynced := make(chan struct{}, 1)
			go func() {
				defer async.LogPanicTrace(t.logger)

				select {
				// Once the state is synced we can stop the ChangelogSyncer
				case <-stateSynced:
					if err := changelog.Stop(); err != nil {
						panic(err.Error())
					}
				// Task has received the stop signal. RunGroup is stopping
				case <-opts.Stopping():
					if err := changelog.Stop(); err != nil {
						panic(err.Error())
					}
				}
			}()

			return changelog.Sync(t.ctx, stateSynced)
		})
	}

	if err := t.Sync(); err != nil {
		return errors.Wrapf(err, `state restore error. TaskId:%s`, t.ID())
	}

	if err := t.Ready(); err != nil {
		return errors.Wrapf(err,
			`state restore error occurred while waiting for task to be ready. TaskId:%s`, t.ID())
	}

	return nil
}

func (t *task) Init() error {
	t.metrics.processLatencyMicroseconds = t.metrics.reporter.Observer(metrics.MetricConf{
		Path:        `process_latency_microseconds`,
		ConstLabels: map[string]string{`partition`: fmt.Sprintf(`%d`, t.ID().Partition())},
		Labels:      []string{`topic`},
	})

	labels := map[string]string{`topics`: t.ID().Topics(), `partition`: fmt.Sprintf(`%d`, t.ID().Partition())}
	t.metrics.batchProcessLatencyMicroseconds = t.metrics.reporter.Observer(metrics.MetricConf{
		Path:        `batch_process_latency_microseconds`,
		ConstLabels: labels,
	})
	t.metrics.batchFlushLatencyMicroseconds = t.metrics.reporter.Observer(metrics.MetricConf{
		Path:        `batch_flush_latency_microseconds`,
		ConstLabels: labels,
	})

	t.metrics.batchSize = t.metrics.reporter.Gauge(metrics.MetricConf{
		Path:        `batch_size`,
		ConstLabels: labels,
	})

	if err := t.subTopology.Init(t.ctx); err != nil {
		return errors.Wrapf(err, `sub-topology init failed. TaskId:%s`, t.ID())
	}

	// Init and start commitBuffer
	if err := t.commitBuffer.Init(); err != nil {
		return errors.Wrap(err, `commitBuffer init failed`)
	}

	go t.start()

	return nil
}

func (t *task) Sync() error {
	return t.changelogs.Run()
}

func (t *task) Chan() chan *Record {
	return t.dataChan
}

func (t *task) Ready() error {
	if err := t.changelogs.Ready(); err != nil {
		return err
	}

	t.logger.Info(`State Restored`)
	return nil
}

func (t *task) process(record *Record) error {
	defer func(since time.Time) {
		t.metrics.processLatencyMicroseconds.Observe(float64(time.Since(since).Microseconds()),
			map[string]string{`topic`: record.Topic()})
	}(time.Now())

	ctx := TaskContext{context.WithValue(topology.NewRecordContext(record), &recordContextTaskIDKey, t.ID().String())}
	_, _, _, err := t.subTopology.Source(record.Topic()).
		Run(ctx, record.Key(), record.Value())
	if err != nil {
		// if this is a kafka producer error, return it(will be retried), otherwise ignore and exclude from
		// re-processing(only the kafka errors can be retried here)
		assert := func(err error) bool {
			_, ok := err.(kafka.ProducerErr)
			return ok
		}
		if producerErr := errors.UnWrapRecursivelyUntil(err, assert); producerErr != nil {
			return producerErr
		}

		// send record to DLQ handler and mark record as ignored,
		// so it will be excluded from next batch
		t.options.failedMessageHandler(err, record)
		record.ignore = true

		t.logger.ErrorContext(ctx, fmt.Sprintf(`record %s process failed due to %s`, record, err))

		return err
	}

	return nil
}

func (t *task) Start(ctx context.Context, claim kafka.PartitionClaim, _ kafka.GroupSession) {
	t.blockingMode = true

	stopping := make(chan struct{}, 1)
	t.consumerLoops.Add(1)

	go func() {
		<-t.processingStopping
		t.logger.Info(fmt.Sprintf(`Stop signal received. Stopping message loop %s`, claim.TopicPartition()))
		stopping <- struct{}{}
	}()

MAIN:
	for {
		select {
		case <-stopping:
			break MAIN
		case record, ok := <-claim.Records():
			if !ok {
				// channel is closed
				break MAIN
			}

			t.dataChan <- NewTaskRecord(record)
			t.logger.TraceContext(record.Ctx(),
				`Record send to processing chan`, `DataChan length`, len(t.dataChan))

		}
	}

	t.consumerLoops.Done()
	t.logger.Error(fmt.Sprintf(`Message loop stopped for %s`, claim.TopicPartition()))
}

func (t *task) start() {
	stopping := make(chan struct{}, 1)
	go func() {
		<-t.processingStopping
		t.logger.Info(fmt.Sprintf(`Stop signal received. Stopping processing loop %s`, t.ID()))
		stopping <- struct{}{}
	}()

	var once sync.Once
	tick := time.NewTicker(t.options.buffer.FlushInterval)

MAIN:
	for {
		select {
		case <-stopping:
			break MAIN
		case <-tick.C:
			if err := t.commitBuffer.Flush(); err != nil {
				t.reProcessCommitBuffer(err, nil)
			}
		case record := <-t.dataChan:
			once.Do(func() {
				t.logger.Info(fmt.Sprintf(`Starting offset %s`, record))
			})

			// Process the record
			taskRecord := NewTaskRecord(record)
			if err := t.process(taskRecord); err != nil {
				// Ignored recodes (due to a processing error) cannot be retried
				if !record.ignore {
					if err := t.commitBuffer.Add(taskRecord); err != nil {
						t.options.failedMessageHandler(err, record)
					}
				}

				t.reProcessCommitBuffer(err, nil)
				continue
			}

			t.logger.TraceContext(record.Ctx(),
				`Record processed. Sending to commit buffer`, record.String())

			if err := t.commitBuffer.Add(taskRecord); err != nil {
				t.options.failedMessageHandler(err, record)
			}
		}
	}

	close(t.dataChan)

	t.logger.Info(fmt.Sprintf(`Processing loop stopped for %s`, t.ID()))
}

func (t *task) reProcessCommitBuffer(err error, records []*Record) {
	t.logger.Warn(fmt.Sprintf(`Reprocessing commit buffer due to %s`, err))

	if len(records) < 1 {
		records = make([]*Record, len(t.commitBuffer.Records()))
		copy(records, t.commitBuffer.Records())
	}

	if rsetErr := t.commitBuffer.Reset(err); rsetErr != nil {
		t.logger.Warn(`Buffer reset failed while reprocessing batch, retrying ...`)
		t.reProcessCommitBuffer(rsetErr, records)
		return
	}

	for _, record := range records {
		if record.ignore {
			continue
		}

		if processErr := t.process(record); processErr != nil {
			t.reProcessCommitBuffer(processErr, records)
			return
		}

		if err := t.commitBuffer.Add(record); err != nil {
			t.options.failedMessageHandler(err, record)
		}
	}
}

func (t *task) Stop() error {
	t.shutdown(nil)
	<-t.closing
	return nil
}

func (t *task) shutdown(err error) {
	t.shutDownOnce.Do(func() {
		// close sub topology
		if err := t.subTopology.Close(); err != nil {
			panic(fmt.Sprintf(`sub-topology close error due to %s`, err))
		}

		if err != nil {
			t.logger.Info(fmt.Sprintf(`Stopping..., due to %s`, err))
		} else {
			t.logger.Info(`Stopping...`)
		}

		defer t.logger.Info(`Stopped`)

		t.changelogs.Stop()

		if t.blockingMode {
			t.logger.Info(`Waiting until processing stopped...`)
			close(t.processingStopping)
			t.consumerLoops.Wait()
			t.logger.Info(`Processing stopped`)
		}

		// Wait until processing loop exit
		if err := t.commitBuffer.Close(); err != nil {
			t.logger.Warn(err)
		}

		// Close all the state stores
		wg := &sync.WaitGroup{}
		for _, store := range t.subTopology.StateStores() {
			wg.Add(1)
			go func(wg *sync.WaitGroup, ins topology.StateStore) {
				defer wg.Done()

				if err := ins.Close(); err != nil {
					t.logger.Warn(fmt.Sprintf(`LoggableStateStore close error due to %s`, err))
				}
			}(wg, store)
		}
		wg.Wait()

		if t.producer != nil {
			if err := t.producer.Close(); err != nil {
				t.logger.Error(fmt.Sprintf(`Producer close error due to %s`, err))
			}
		}

		close(t.closing)
	})
}

func (t *task) Store(name string) topology.StateStore {
	return t.subTopology.StateStores()[name]
}

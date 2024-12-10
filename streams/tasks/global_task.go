package tasks

import (
	"context"
	"github.com/gmbyapa/kstream/v2/kafka"
	"github.com/gmbyapa/kstream/v2/pkg/async"
	"github.com/tryfix/metrics/v2"
	"sync"
	"time"
)

type globalTask struct {
	*task
}

func (t *globalTask) Init() error {
	t.metrics.taskStatus.Count(float64(TaskStatusStateRestoring), nil)

	restoreProgressWg := sync.WaitGroup{}
	restoreProgressWg.Add(len(t.subTopology.StateStores()))

	go func(start time.Time) {
		restoreProgressWg.Wait()
		t.metrics.taskStatus.Set(float64(TaskStatusRunning), nil)
		t.metrics.stateRecoveryDurationMilliseconds.Set(float64(time.Since(start).Milliseconds()), nil)
	}(time.Now())

	defer func() {
		for _, store := range t.subTopology.StateStores() {
			stateStore := store
			t.changelogs.Add(func(opts *async.Opts) error {
				stateSynced := make(chan struct{}, 1)
				go func() {
					defer async.LogPanicTrace(t.logger)

					// Once the state is synced signal the RunGroup the process is ready
					<-stateSynced
					restoreProgressWg.Done()
					opts.Ready()
				}()

				go func() {
					defer async.LogPanicTrace(t.logger)

					<-opts.Stopping()
					if err := stateStore.Stop(); err != nil {
						panic(err.Error())
					}
				}()

				return stateStore.Sync(t.task.ctx, stateSynced)
			})
		}
	}()

	return t.subTopology.Init(t.task.ctx)
}

func (t *globalTask) Start(ctx context.Context, claim kafka.PartitionClaim, s kafka.GroupSession) {
	panic(`GlobalTask does not support processing`)
}

func (t *globalTask) Stop() error {
	t.changelogs.Stop()
	return nil
}

func (t *globalTask) setup() {
	labels := map[string]string{`topics`: t.ID().Topics()}
	t.metrics.taskStatus = t.metrics.reporter.Gauge(metrics.MetricConf{
		Path:        `status`,
		ConstLabels: labels,
	})

	t.metrics.stateRecoveryDurationMilliseconds = t.metrics.reporter.Gauge(metrics.MetricConf{
		Path:        `state_recovery_duration_milliseconds`,
		ConstLabels: labels,
	})
}

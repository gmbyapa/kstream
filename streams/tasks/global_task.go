package tasks

import (
	"context"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/async"
)

type globalTask struct {
	*task
}

func (t *globalTask) Init() error {
	defer func() {
		for _, store := range t.subTopology.StateStores() {
			stateStore := store
			t.changelogs.Add(func(opts *async.Opts) error {
				stateSynced := make(chan struct{}, 1)
				go func() {
					defer async.LogPanicTrace(t.logger)

					// Once the state is synced signal the RunGroup the process is ready
					<-stateSynced
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

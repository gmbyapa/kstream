package async

import (
	"errors"
	"fmt"
	"github.com/tryfix/log"
	"sync"
)

// Fn is a function that can be run asynchronously.
type Fn func(*Opts) error

// Opts contains options for running a function.
type Opts struct {
	// stopping is a channel that can be used to signal that the function should stop.
	stopping <-chan struct{}

	// readyOnce ensures that Ready() can only be called once.
	readyOnce sync.Once

	// ready is a channel that is closed when the function is ready(eg: bootstrap process, state recovery, etc) to run.
	ready chan struct{}
}

// Stopping returns a channel that can be used to signal that the function should stop.
func (opts *Opts) Stopping() <-chan struct{} {
	return opts.stopping
}

// Ready signals that the function is ready to run.
func (opts *Opts) Ready() {
	opts.readyOnce.Do(func() {
		close(opts.ready)
	})
}

var ErrInterrupted = errors.New(`interrupted`)

// RunGroup runs a group of functions asynchronously.
type RunGroup struct {
	fns          []Fn
	wg           *sync.WaitGroup
	readyWg      *sync.WaitGroup
	stopping     chan struct{}
	stopped      chan struct{}
	shutDownOnce *sync.Once
	err          error
	logger       log.Logger
	shuttingDown bool
}

func NewRunGroup(logger log.Logger, fns ...Fn) *RunGroup {
	return &RunGroup{
		fns:          fns,
		wg:           new(sync.WaitGroup),
		readyWg:      new(sync.WaitGroup),
		stopping:     make(chan struct{}),
		stopped:      make(chan struct{}),
		shutDownOnce: &sync.Once{},
		logger:       logger.NewLog(log.Prefixed(`AsyncGroup`)),
	}
}

// Add adds a function to the RunGroup. The function will be executed when the Run method is called.
// Note: RunGroup does not support dynamically adding functions to a running group.
func (tg *RunGroup) Add(fn Fn) *RunGroup {
	tg.readyWg.Add(1)
	tg.fns = append(tg.fns, fn)
	return tg
}

func (tg *RunGroup) Run() error {
	notifyErrOnce := &sync.Once{}

	tg.wg.Add(len(tg.fns))

	// Run each function on a separate go-routine.
	for _, fn := range tg.fns {
		ready := make(chan struct{}, 1)

		// Wait for the function to become ready before starting it.
		go func() {
			<-ready
			tg.readyWg.Done()
		}()

		go func(fn Fn) {
			defer LogPanicTrace(tg.logger)

			opts := &Opts{
				stopping: tg.stopping,
				ready:    ready,
			}

			if err := fn(opts); err != nil {
				// Only the first error needs to be notified
				notifyErrOnce.Do(func() {
					tg.err = err
				})
				tg.notifyShutDown(err)
			}

			// When function returns make it ready anyway
			opts.Ready()
			tg.wg.Done()
		}(fn)
	}

	tg.wg.Wait()

	close(tg.stopped)

	return tg.err
}

func (tg *RunGroup) notifyShutDown(err error) {
	tg.shutDownOnce.Do(func() {
		if err != nil {
			tg.logger.Error(fmt.Sprintf(`Processes stopping due to %s`, err))
		} else {
			tg.logger.Info(`Interrupted, Processes stopping...`)
		}

		tg.shuttingDown = true
		close(tg.stopping)
	})
}

func (tg *RunGroup) Ready() error {
	tg.readyWg.Wait()
	if tg.err == nil && tg.shuttingDown {
		return ErrInterrupted
	}
	return tg.err
}

func (tg *RunGroup) Stop() {
	tg.notifyShutDown(nil)
	defer tg.logger.Info(`Processes stopped`)

	<-tg.stopped
}

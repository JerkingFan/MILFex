package shredder

import (
	"errors"
	mathrand "math/rand"
	"sync/atomic"
	"time"

	"milf"
)

var ErrSchedulerStopped = errors.New("scheduler stopped")
var ErrSchedulerStopTimeout = errors.New("scheduler stop timeout")

type Config struct {
	GlobalQueueSize int
	LocalQueueSize  int
}

type Stats struct {
	Scheduled      int64
	Executed       int64
	Failed         int64
	Stolen         int64
	RoutedAffinity int64
	RoutedRandom   int64
	InFlight       int64
}

const (
	defaultGlobalQueueSize = 100
	defaultLocalQueueSize  = 64
)

type Scheduler struct {
	tasks    chan Task
	workers  []*Worker
	group    milf.Group
	stopChan chan struct{}

	stateMu *milf.MILFex
	started int32
	stopped int32

	dispatcherDone chan struct{}
	affinity       map[string]int
	stats          Stats
	rng            *mathrand.Rand
}

func NewScheduler(numWorkers int) *Scheduler {
	return NewSchedulerWithConfig(numWorkers, Config{})
}

func NewSchedulerWithConfig(numWorkers int, cfg Config) *Scheduler {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	if cfg.GlobalQueueSize <= 0 {
		cfg.GlobalQueueSize = defaultGlobalQueueSize
	}
	if cfg.LocalQueueSize <= 0 {
		cfg.LocalQueueSize = defaultLocalQueueSize
	}

	s := &Scheduler{
		tasks:    make(chan Task, cfg.GlobalQueueSize),
		workers:  make([]*Worker, 0, numWorkers),
		stopChan: make(chan struct{}),
		stateMu:  milf.NewMILFex(),
		affinity: make(map[string]int, numWorkers*2),
		rng:      mathrand.New(mathrand.NewSource(time.Now().UnixNano())),
	}

	for i := 0; i < numWorkers; i++ {
		s.workers = append(s.workers, NewWorker(i, make(chan Task, cfg.LocalQueueSize), s.stopChan, s))
	}
	for _, w := range s.workers {
		w.peers = s.workers
	}

	return s
}

func (s *Scheduler) Start() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.started != 0 {
		return
	}
	s.started = 1
	s.dispatcherDone = make(chan struct{})

	s.group.Go(func() {
		defer close(s.dispatcherDone)
		s.dispatch()
	})

	for _, w := range s.workers {
		worker := w
		s.group.Go(func() { worker.Run() })
	}
}

func (s *Scheduler) Stop() {
	_ = s.StopWithTimeout(0)
}

func (s *Scheduler) StopWithTimeout(timeout time.Duration) error {
	s.stateMu.Lock()
	if s.stopped != 0 {
		s.stateMu.Unlock()
		return nil
	}
	s.stopped = 1
	s.stateMu.Unlock()

	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	for {
		if atomic.LoadInt64(&s.stats.InFlight) == 0 {
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			close(s.stopChan)
			return ErrSchedulerStopTimeout
		}
		time.Sleep(1 * time.Millisecond)
	}

	close(s.stopChan)

	if timeout <= 0 {
		s.group.Wait()
		return nil
	}

	waitDone := make(chan struct{})
	milf.Go(func() {
		s.group.Wait()
		close(waitDone)
	})
	select {
	case <-waitDone:
		return nil
	case <-time.After(timeout):
		return ErrSchedulerStopTimeout
	}
}

func (s *Scheduler) Schedule(task Task) error {
	if atomic.LoadInt32(&s.stopped) != 0 {
		return ErrSchedulerStopped
	}
	atomic.AddInt64(&s.stats.Scheduled, 1)
	atomic.AddInt64(&s.stats.InFlight, 1)

	select {
	case s.tasks <- task:
		return nil
	case <-s.stopChan:
		atomic.AddInt64(&s.stats.InFlight, -1)
		return ErrSchedulerStopped
	}
}

func (s *Scheduler) Stats() Stats {
	return Stats{
		Scheduled:      atomic.LoadInt64(&s.stats.Scheduled),
		Executed:       atomic.LoadInt64(&s.stats.Executed),
		Failed:         atomic.LoadInt64(&s.stats.Failed),
		Stolen:         atomic.LoadInt64(&s.stats.Stolen),
		RoutedAffinity: atomic.LoadInt64(&s.stats.RoutedAffinity),
		RoutedRandom:   atomic.LoadInt64(&s.stats.RoutedRandom),
		InFlight:       atomic.LoadInt64(&s.stats.InFlight),
	}
}

func (s *Scheduler) dispatch() {
	for {
		select {
		case task := <-s.tasks:
			if len(s.workers) == 0 {
				atomic.AddInt64(&s.stats.InFlight, -1)
				continue
			}
			idx := s.selectWorker(task)
			select {
			case s.workers[idx].local <- task:
			case <-s.stopChan:
				atomic.AddInt64(&s.stats.InFlight, -1)
				return
			}
		case <-s.stopChan:
			return
		}
	}
}

func (s *Scheduler) selectWorker(task Task) int {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if task.ID != "" {
		if idx, ok := s.affinity[task.ID]; ok && idx >= 0 && idx < len(s.workers) {
			atomic.AddInt64(&s.stats.RoutedAffinity, 1)
			return idx
		}
	}

	idx := s.rng.Intn(len(s.workers))
	atomic.AddInt64(&s.stats.RoutedRandom, 1)
	return idx
}

func (s *Scheduler) markDone(task Task, workerID int, failed bool, stolen bool) {
	if task.ID != "" {
		s.stateMu.Lock()
		s.affinity[task.ID] = workerID
		s.stateMu.Unlock()
	}

	if failed {
		atomic.AddInt64(&s.stats.Failed, 1)
	} else {
		atomic.AddInt64(&s.stats.Executed, 1)
	}
	if stolen {
		atomic.AddInt64(&s.stats.Stolen, 1)
	}
	atomic.AddInt64(&s.stats.InFlight, -1)
}

package shredder

import "context"

type Worker struct {
	id        int
	local     chan Task
	peers     []*Worker
	stopChan  <-chan struct{}
	scheduler *Scheduler
}

func NewWorker(id int, local chan Task, stopChan <-chan struct{}, scheduler *Scheduler) *Worker {
	return &Worker{
		id:        id,
		local:     local,
		stopChan:  stopChan,
		scheduler: scheduler,
	}
}

func (w *Worker) Run() {
	for {
		select {
		case task := <-w.local:
			w.execute(task, false)
		case <-w.stopChan:
			return
		default:
			if task, ok := w.trySteal(); ok {
				w.execute(task, true)
				continue
			}
			select {
			case task := <-w.local:
				w.execute(task, false)
			case <-w.stopChan:
				return
			}
		}
	}
}

func (w *Worker) trySteal() (Task, bool) {
	var zero Task
	if len(w.peers) <= 1 {
		return zero, false
	}

	for i := 0; i < len(w.peers); i++ {
		peer := w.peers[(w.id+i+1)%len(w.peers)]
		if peer == nil || peer.id == w.id {
			continue
		}
		select {
		case task := <-peer.local:
			return task, true
		default:
		}
	}
	return zero, false
}

func (w *Worker) execute(task Task, stolen bool) {
	if task.Fn == nil {
		w.scheduler.markDone(task, w.id, true, stolen)
		return
	}

	ctx := task.Context
	if ctx == nil {
		ctx = context.Background()
	}

	result, err := task.Fn(ctx)
	if err != nil {
		if task.ErrorCh != nil {
			select {
			case task.ErrorCh <- err:
			default:
			}
		}
		w.scheduler.markDone(task, w.id, true, stolen)
		return
	}

	if task.ResultCh != nil {
		select {
		case task.ResultCh <- result:
		default:
		}
	}
	w.scheduler.markDone(task, w.id, false, stolen)
}
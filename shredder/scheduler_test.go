package shredder

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerFIFOOneWorker(t *testing.T) {
	s := NewScheduler(1)
	s.Start()
	defer s.Stop()

	out := make(chan int, 3)

	for i := 1; i <= 3; i++ {
		n := i
		err := s.Schedule(Task{
			Context: context.Background(),
			Fn: func(ctx context.Context) (interface{}, error) {
				out <- n
				return n, nil
			},
		})
		if err != nil {
			t.Fatalf("schedule failed: %v", err)
		}
	}

	got := []int{<-out, <-out, <-out}
	want := []int{1, 2, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("fifo violated: got=%v want=%v", got, want)
		}
	}
}

func TestSchedulerStopCompletes(t *testing.T) {
	s := NewScheduler(4)
	s.Start()

	var executed int32
	done := make(chan struct{}, 20)

	for i := 0; i < 20; i++ {
		err := s.Schedule(Task{
			Context: context.Background(),
			Fn: func(ctx context.Context) (interface{}, error) {
				atomic.AddInt32(&executed, 1)
				done <- struct{}{}
				return nil, nil
			},
		})
		if err != nil {
			t.Fatalf("schedule failed: %v", err)
		}
	}

	time.Sleep(20 * time.Millisecond)
	s.Stop()

	if atomic.LoadInt32(&executed) == 0 {
		t.Fatal("expected at least one executed task before stop")
	}
}

func TestScheduleAfterStop(t *testing.T) {
	s := NewScheduler(1)
	s.Start()
	s.Stop()

	err := s.Schedule(Task{
		Context: context.Background(),
		Fn: func(ctx context.Context) (interface{}, error) {
			return nil, nil
		},
	})
	if !errors.Is(err, ErrSchedulerStopped) {
		t.Fatalf("expected ErrSchedulerStopped, got: %v", err)
	}
}

func TestSchedulerConcurrentSchedule(t *testing.T) {
	s := NewScheduler(4)
	s.Start()
	defer s.Stop()

	const total = 200
	done := make(chan struct{}, total)

	for i := 0; i < total; i++ {
		n := i
		if err := s.Schedule(Task{
			Context: context.Background(),
			Fn: func(ctx context.Context) (interface{}, error) {
				_ = n
				done <- struct{}{}
				return nil, nil
			},
		}); err != nil {
			t.Fatalf("schedule failed at %d: %v", i, err)
		}
	}

	for i := 0; i < total; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting completion: got=%d want=%d", i, total)
		}
	}
}

func TestSchedulerResultAndErrorChannels(t *testing.T) {
	s := NewScheduler(2)
	s.Start()
	defer s.Stop()

	resultCh := make(chan interface{}, 1)
	errorCh := make(chan error, 1)

	if err := s.Schedule(Task{
		Context:  context.Background(),
		ResultCh: resultCh,
		Fn: func(ctx context.Context) (interface{}, error) {
			return "ok", nil
		},
	}); err != nil {
		t.Fatalf("schedule success task failed: %v", err)
	}

	select {
	case v := <-resultCh:
		if v != "ok" {
			t.Fatalf("unexpected result: %v", v)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting success result")
	}

	if err := s.Schedule(Task{
		Context: context.Background(),
		ErrorCh: errorCh,
		Fn: func(ctx context.Context) (interface{}, error) {
			return nil, fmt.Errorf("boom")
		},
	}); err != nil {
		t.Fatalf("schedule error task failed: %v", err)
	}

	select {
	case err := <-errorCh:
		if err == nil || err.Error() != "boom" {
			t.Fatalf("unexpected error value: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting error result")
	}
}

func TestSchedulerConfigurableQueueSize(t *testing.T) {
	s := NewSchedulerWithConfig(2, Config{
		GlobalQueueSize: 7,
		LocalQueueSize:  3,
	})
	if cap(s.tasks) != 7 {
		t.Fatalf("unexpected global queue capacity: got=%d want=%d", cap(s.tasks), 7)
	}
	if len(s.workers) != 2 {
		t.Fatalf("unexpected workers count: %d", len(s.workers))
	}
	for i, w := range s.workers {
		if cap(w.local) != 3 {
			t.Fatalf("worker[%d] local cap got=%d want=%d", i, cap(w.local), 3)
		}
	}
}

func TestStopWithTimeout(t *testing.T) {
	s := NewScheduler(1)
	s.Start()

	block := make(chan struct{})
	err := s.Schedule(Task{
		Context: context.Background(),
		Fn: func(ctx context.Context) (interface{}, error) {
			<-block
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("schedule failed: %v", err)
	}

	if err := s.StopWithTimeout(10 * time.Millisecond); !errors.Is(err, ErrSchedulerStopTimeout) {
		t.Fatalf("expected ErrSchedulerStopTimeout, got: %v", err)
	}
	close(block)
}

func TestSchedulerStatsAndAffinityRouting(t *testing.T) {
	s := NewSchedulerWithConfig(2, Config{
		GlobalQueueSize: 32,
		LocalQueueSize:  8,
	})
	s.Start()
	defer s.Stop()

	res := make(chan interface{}, 1)
	taskID := "sticky-task"

	err := s.Schedule(Task{
		ID:       taskID,
		Context:  context.Background(),
		ResultCh: res,
		Fn: func(ctx context.Context) (interface{}, error) {
			return "ok", nil
		},
	})
	if err != nil {
		t.Fatalf("schedule failed: %v", err)
	}
	<-res

	err = s.Schedule(Task{
		ID:       taskID,
		Context:  context.Background(),
		ResultCh: res,
		Fn: func(ctx context.Context) (interface{}, error) {
			return "ok", nil
		},
	})
	if err != nil {
		t.Fatalf("schedule failed: %v", err)
	}
	<-res

	stats := s.Stats()
	if stats.Scheduled < 2 {
		t.Fatalf("scheduled stats too low: %d", stats.Scheduled)
	}
	if stats.Executed < 2 {
		t.Fatalf("executed stats too low: %d", stats.Executed)
	}
	if stats.RoutedRandom < 1 {
		t.Fatalf("expected at least one random route, got %d", stats.RoutedRandom)
	}
	if stats.RoutedAffinity < 1 {
		t.Fatalf("expected affinity route for second task, got %d", stats.RoutedAffinity)
	}
}

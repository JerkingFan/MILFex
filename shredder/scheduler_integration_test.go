package shredder

import (
	"context"
	"testing"
	"time"
)

func TestSchedulerIntegrationEndToEnd(t *testing.T) {
	s := NewSchedulerWithConfig(1, Config{
		GlobalQueueSize: 8,
		LocalQueueSize:  4,
	})
	s.Start()
	defer func() {
		if err := s.StopWithTimeout(1 * time.Second); err != nil {
			t.Fatalf("stop with timeout failed: %v", err)
		}
	}()

	for i := 0; i < 3; i++ {
		n := i
		resultCh := make(chan interface{}, 1)
		err := s.Schedule(Task{
			ID:       "sticky-demo",
			Context:  context.Background(),
			ResultCh: resultCh,
			Fn: func(ctx context.Context) (interface{}, error) {
				return n, nil
			},
		})
		if err != nil {
			t.Fatalf("schedule failed on %d: %v", i, err)
		}

		select {
		case v := <-resultCh:
			if v != n {
				t.Fatalf("unexpected result: got=%v want=%d", v, n)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting result for task %d", i)
		}
	}

	stats := s.Stats()
	if stats.Scheduled < 3 || stats.Executed < 3 {
		t.Fatalf("unexpected stats: scheduled=%d executed=%d", stats.Scheduled, stats.Executed)
	}
}

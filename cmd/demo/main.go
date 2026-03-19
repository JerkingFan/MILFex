package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"milf"
	"milf/shredder"
)

func main() {
	mu := milf.NewMILFex()
	var counter int64

	var g milf.Group
	workers := 1
	iterations := 10000

	for i := 0; i < workers; i++ {
		g.Go(func() {
			for j := 0; j < iterations; j++ {
				mu.Lock()
				atomic.AddInt64(&counter, 1)
				mu.Unlock()
			}
		})
	}

	done := milf.Go(func() { g.Wait() })
	done.Wait()

	mu.Lock()
	counterSnapshot := counter
	mu.Unlock()

	expected := int64(workers * iterations)
	fmt.Printf("counter=%d snapshot=%d expected=%d ok=%v\n", counter, counterSnapshot, expected, counter == expected)

	s := shredder.NewSchedulerWithConfig(1, shredder.Config{
		GlobalQueueSize: 16,
		LocalQueueSize:  8,
	})
	s.Start()

	for i := 1; i <= 3; i++ {
		n := i
		resultCh := make(chan interface{}, 1)
		err := s.Schedule(shredder.Task{
			ID:       "demo-affinity",
			Context:  context.Background(),
			ResultCh: resultCh,
			Fn: func(ctx context.Context) (interface{}, error) {
				return fmt.Sprintf("task-%d done", n), nil
			},
		})
		if err != nil {
			fmt.Printf("schedule error: %v\n", err)
			continue
		}

		select {
		case res := <-resultCh:
			fmt.Printf("scheduler result: %v\n", res)
		case <-time.After(1 * time.Second):
			fmt.Println("scheduler timeout waiting result")
		}
	}

	if err := s.StopWithTimeout(1 * time.Second); err != nil {
		fmt.Printf("scheduler stop error: %v\n", err)
	}

	st := s.Stats()
	fmt.Printf(
		"scheduler stats: scheduled=%d executed=%d failed=%d affinity=%d random=%d stolen=%d inflight=%d\n",
		st.Scheduled, st.Executed, st.Failed, st.RoutedAffinity, st.RoutedRandom, st.Stolen, st.InFlight,
	)
}

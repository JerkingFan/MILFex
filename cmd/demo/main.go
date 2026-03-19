package main

import (
	"fmt"
	"sync/atomic"

	"milf"
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
}

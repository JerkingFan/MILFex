package milf

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestGoWait(t *testing.T) {
	var x int32
	r := Go(func() {
		atomic.StoreInt32(&x, 1)
	})
	r.Wait()

	if atomic.LoadInt32(&x) != 1 {
		t.Fatal("task did not run")
	}
}

func TestGoSafeRecovers(t *testing.T) {
	done := make(chan struct{}, 1)

	r := GoSafe(func() {
		panic("boom")
	}, func(v any) {
		done <- struct{}{}
	})

	r.Wait()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("panic handler was not called")
	}
}

func TestGroupWait(t *testing.T) {
	var c int32
	var g Group

	for i := 0; i < 100; i++ {
		g.Go(func() {
			atomic.AddInt32(&c, 1)
		})
	}
	g.Wait()

	if atomic.LoadInt32(&c) != 100 {
		t.Fatalf("want 100 got %d", c)
	}
}

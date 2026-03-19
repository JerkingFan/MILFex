package milf

import (
	"runtime"
	"sync/atomic"
)

func (m *MILFex) Lock() {

	if atomic.CompareAndSwapInt32(&m.state, 0, mutexLocked) {
		atomic.StoreInt64(&m.master, 1)
		return
	}

	atomic.AddInt32(&m.enqueueing, 1)

	ch := make(chan struct{}, 1)

	for {
		old := atomic.LoadInt32(&m.state)
		if old&mutexWaiters != 0 {
			break
		}
		if atomic.CompareAndSwapInt32(&m.state, old, old|mutexWaiters) {
			break
		}
	}

	m.slaves.Enqueue(ch)
	atomic.AddInt32(&m.enqueueing, -1)
	<-ch
	atomic.StoreInt64(&m.master, 1)

}

func (m *MILFex) Unlock() {
	atomic.StoreInt64(&m.master, 0)

	for {
		old := atomic.LoadInt32(&m.state)

		if old&mutexWaiters == 0 {
			if atomic.CompareAndSwapInt32(&m.state, old, 0) {
				return
			}
			continue
		}

		if ch, ok := m.slaves.Dequeue(); ok {
			ch <- struct{}{}
			atomic.StoreInt64(&m.master, 1)

			if atomic.LoadInt32(&m.enqueueing) == 0 {
				s := atomic.LoadInt32(&m.state)
				if s&mutexWaiters != 0 {
					atomic.CompareAndSwapInt32(&m.state, s, s&^int32(mutexWaiters))
				}
			}

			return
		}
		if atomic.LoadInt32(&m.enqueueing) != 0 {
			runtime.Gosched()
			continue
		}

		atomic.CompareAndSwapInt32(&m.state, old, old&^int32(mutexWaiters))
	}

}

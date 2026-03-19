package milf

import (
	"sync/atomic"
	"unsafe"
)

type node[T any] struct {
	value T
	next  unsafe.Pointer
}

type LockFreeQueue[T any] struct {
	head unsafe.Pointer
	tail unsafe.Pointer
}

func New[T any]() *LockFreeQueue[T] {
	dummy := &node[T]{}
	return &LockFreeQueue[T]{
		head: unsafe.Pointer(dummy),
		tail: unsafe.Pointer(dummy),
	}
}

func (q *LockFreeQueue[T]) Enqueue(val T) {
	n := &node[T]{value: val}
	for {
		tail := atomic.LoadPointer(&q.tail)
		tailNode := (*node[T])(tail)
		next := atomic.LoadPointer(&tailNode.next)

		if tail == atomic.LoadPointer(&q.tail) {
			if next == nil {
				if atomic.CompareAndSwapPointer(
					&tailNode.next,
					nil,
					unsafe.Pointer(n),
				) {
					atomic.CompareAndSwapPointer(&q.tail, tail, unsafe.Pointer(n))
					return
				}
			} else {
				atomic.CompareAndSwapPointer(&q.tail, tail, next)
			}
		}
	}
}

func (q *LockFreeQueue[T]) Dequeue() (T, bool) {
	for {
		head := atomic.LoadPointer(&q.head)
		tail := atomic.LoadPointer(&q.tail)
		headNode := (*node[T])(head)
		next := atomic.LoadPointer(&headNode.next)

		if head == atomic.LoadPointer(&q.head) {
			if head == tail {
				if next == nil {
					var zero T
					return zero, false
				}
				atomic.CompareAndSwapPointer(&q.tail, tail, next)
			} else {
				nextNode := (*node[T])(next)
				val := nextNode.value
				if atomic.CompareAndSwapPointer(&q.head, head, next) {
					return val, true
				}
			}
		}
	}
}

package milf

import (
	"sync"
	"unsafe"
)

//go:linkname runtimeNewproc runtime.newproc
func runtimeNewproc(fn *funcval)

type funcval struct {
	fn uintptr
}

func spawn(fn func()) {
	fv := *(*unsafe.Pointer)(unsafe.Pointer(&fn))
	runtimeNewproc((*funcval)(fv))
}

type Routine struct {
	done chan struct{}
}

func Go(fn func()) *Routine {
	r := &Routine{done: make(chan struct{})}
	spawn(func() {
		defer close(r.done)
		fn()
	})
	return r
}

func GoSafe(fn func(), onPanic func(any)) *Routine {
	r := &Routine{done: make(chan struct{})}
	spawn(func() {
		defer close(r.done)
		defer func() {
			if v := recover(); v != nil && onPanic != nil {
				onPanic(v)
			}
		}()
		fn()
	})
	return r
}

func (r *Routine) Wait() {
	if r == nil {
		return
	}
	<-r.done
}

type Group struct {
	wg sync.WaitGroup
}

func (g *Group) Go(fn func()) {
	g.wg.Add(1)
	spawn(func() {
		defer g.wg.Done()
		fn()
	})
}

func (g *Group) GoSafe(fn func(), onPanic func(any)) {
	g.wg.Add(1)
	spawn(func() {
		defer g.wg.Done()
		defer func() {
			if v := recover(); v != nil && onPanic != nil {
				onPanic(v)
			}
		}()
		fn()
	})
}

func (g *Group) Wait() {
	g.wg.Wait()
}

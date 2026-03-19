package milf

const (
	mutexLocked  = 1 << iota // 1
	mutexWaiters             // 2
)

type MILFex struct {
	state int32

	master int64

	enqueueing int32

	slaves LockFreeQueue[chan struct{}]
}

func NewMILFex() *MILFex {
	return &MILFex{
		slaves: *New[chan struct{}](),
	}
}

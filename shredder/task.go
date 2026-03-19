package shredder

import (
	"context"
	"time"
)

type Task struct {
	ID        string
	Name      string
	Priority  int
	CreatedAt time.Time
	Context   context.Context
	Fn        func(ctx context.Context) (interface{}, error)
	ResultCh  chan interface{}
	ErrorCh   chan error
}

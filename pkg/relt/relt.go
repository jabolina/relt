package relt

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrInvalidGroupAddress = errors.New("group address cannot be empty")
	ErrInvalidMessage      = errors.New("message cannot be nil")
	ErrContextClosed       = errors.New("cannot send message on closed transport")
)

// Holds information about the transport context.
// Such as keeping track of spawned goroutines.
type invoker struct {
	// Used to track goroutines.
	group *sync.WaitGroup
}

// Spawn a new goroutine and controls it with
// the wait group.
func (c *invoker) spawn(f func()) {
	c.group.Add(1)
	go func() {
		defer c.group.Done()
		f()
	}()
}

// The implementation for the Transport interface
// providing reliable communication between hosts.
type Relt struct {
	// Context for the transport, used internally only.
	ctx *invoker

	// Information about shutdown the transport.
	cancel context.Context

	// Cancel the transport context.
	finish context.CancelFunc

	// Holds the configuration about the core
	// and the Relt transport.
	configuration ReltConfiguration

	// Holds the core structure.
	core *core
}

// Run will lock the transport waiting for a shutdown
// notification.
// When a shutdown notification arrives all goroutines
// must be finished and only then the returns
func (r *Relt) run() {
	<-r.cancel.Done()
}

// Implements the Transport interface.
func (r Relt) Consume() <-chan Recv {
	return r.core.received
}

// Implements the Transport interface.
func (r Relt) Broadcast(message Send) error {
	select {
	case <-r.cancel.Done():
		return ErrContextClosed
	default:
		if len(message.Address) == 0 {
			return ErrInvalidGroupAddress
		}

		if message.Data == nil {
			return ErrInvalidMessage
		}

		r.ctx.spawn(func() {
			r.core.sending <- message
		})

		return nil
	}
}

// Implements the Transport interface.
func (r *Relt) Close() {
	r.finish()
	r.core.close()
	r.ctx.group.Wait()
}

// Creates a new instance of the reliable transport,
// and start all needed routines.
func NewRelt(configuration ReltConfiguration) (*Relt, error) {
	ctx, done := context.WithCancel(context.Background())
	relt := &Relt{
		ctx: &invoker{
			group: &sync.WaitGroup{},
		},
		cancel:        ctx,
		finish:        done,
		configuration: configuration,
	}
	c, err := newCore(*relt)
	if err != nil {
		return nil, err
	}
	relt.core = c
	relt.ctx.spawn(relt.run)
	return relt, nil
}

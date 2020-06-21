package relt

import (
	"errors"
	"sync"
)

var (
	ErrInvalidGroupAddress = errors.New("group address cannot be empty")
	ErrInvalidMessage      = errors.New("message cannot be nil")
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

// Context to enable the transport shutdown.
type shutdown struct {
	off    bool
	notify chan bool
	mutex  *sync.Mutex
}

// The implementation for the Transport interface
// providing reliable communication between hosts.
type Relt struct {
	// Context for the transport, used internally only.
	ctx *invoker

	// Information about shutdown the transport.
	off shutdown

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
	r.off.mutex.Lock()
	defer func() {
		close(r.off.notify)
		r.off.mutex.Unlock()
	}()

	<-r.off.notify
	r.off.off = true
}

// Implements the Transport interface.
func (r Relt) Consume() <-chan Recv {
	return r.core.received
}

// Implements the Transport interface.
func (r Relt) Broadcast(message Send) error {
	if len(message.Address) == 0 {
		return ErrInvalidGroupAddress
	}

	if message.Data == nil {
		return ErrInvalidMessage
	}

	return r.core.publish(message)
}

// Implements the Transport interface.
func (r *Relt) Close() {
	r.core.close()
	r.off.notify <- true
	r.ctx.group.Wait()
}

// Creates a new instance of the reliable transport,
// and start all needed routines.
func NewRelt(configuration ReltConfiguration) (*Relt, error) {
	relt := &Relt{
		ctx: &invoker{
			group: &sync.WaitGroup{},
		},
		off: shutdown{
			off:    false,
			notify: make(chan bool),
			mutex:  &sync.Mutex{},
		},
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

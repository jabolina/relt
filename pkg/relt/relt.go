package relt

import (
	"sync"
)

// Holds information about the transport context.
// Such as keeping track of spawned goroutines.
type reltContext struct {
	// Used to track goroutines.
	group *sync.WaitGroup
}

// Spawn a new goroutine and controls it with
// the wait group.
func (c *reltContext) spawn(f func()) {
	c.group.Add(1)
	go func() {
		defer c.group.Done()
		f()
	}()
}

// Context to enable the transport shutdown.
type shutdown struct {
	off bool
	notify chan bool
	mutex *sync.Mutex
}

// The implementation for the Transport interface
// providing reliable communication between hosts.
type Relt struct {
	// Context for the transport, used internally only.
	ctx *reltContext

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
	r.configuration.Log.Infof("starting relt %s", r.configuration.Name)
	defer func() {
		r.configuration.Log.Infof("stopping relt %s", r.configuration.Name)
		close(r.off.notify)
		r.off.mutex.Unlock()
	}()

	<-r.off.notify
	r.off.off = true
}

func (r *Relt) Consume() <-chan Recv {
	panic("implement me")
}

func (r *Relt) Broadcast(address GroupAddress, message Send) {
	panic("implement me")
}

// Implements the Transport interface.
func (r *Relt) Close() {
	r.core.close()
	r.off.notify <- true
	r.ctx.group.Wait()
}

// Creates a new instance of the reliable transport,
// and start all needed routines.
func NewRelt(configuration ReltConfiguration) *Relt {
	relt := &Relt{
		ctx: &reltContext{
			group: &sync.WaitGroup{},
		},
		off: shutdown{
			off:    false,
			notify: make(chan bool),
			mutex:  &sync.Mutex{},
		},
		configuration: configuration,
	}
	relt.core = newCore(*relt)
	relt.ctx.spawn(relt.run)
	return relt
}

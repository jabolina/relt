package relt

import (
	"crypto/sha1"
	"fmt"
	"os"
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
	ctx reltContext

	// Information about shutdown the transport.
	off shutdown
}

// Returns the transport identity for reconnection.
func (r *Relt) identity() string {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}

	hash := sha1.New()
	return fmt.Sprintf("%x", hash.Sum([]byte(hostname)))
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

// Creates a new instance of the reliable transport,
// and start all needed routines.
func NewRelt() *Relt {
	relt := &Relt{
		ctx: reltContext{
			group: &sync.WaitGroup{},
		},
		off: shutdown{
			off:    false,
			notify: make(chan bool),
			mutex:  &sync.Mutex{},
		},
	}
	relt.ctx.spawn(relt.run)
	return relt
}

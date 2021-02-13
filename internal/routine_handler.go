package internal

import (
	"sync"
)

var (
	// Global instance for the handler.
	handler *GoRoutineHandler
)

// GoRoutineHandler is responsible for handling goroutines.
// This is used so go routines do not leak and are spawned without any control.
// Using the handler to spawn new routines will guarantee that any routine that
// is not controller careful will be known when the application finishes.
type GoRoutineHandler struct {
	flag  *Flag
	group *sync.WaitGroup
	mutex *sync.Mutex
}

// Create a new instance of the GoRoutineHandler.
func NewRoutineHandler() *GoRoutineHandler {
	handler = &GoRoutineHandler{
		flag:  &Flag{},
		group: &sync.WaitGroup{},
		mutex: &sync.Mutex{},
	}
	return handler
}

// This method will increase the size of the group count and spawn
// the new go routine. After the routine is done, the group will be decreased.
//
// If the handler was already closed, the handler will silently ignore the
// request and will *not* spawn.
func (h *GoRoutineHandler) Spawn(f func()) {
	if h.flag.IsInactive() {
		return
	}

	h.mutex.Lock()
	h.group.Add(1)
	h.mutex.Unlock()
	go func() {
		defer h.group.Done()
		f()
	}()
}

// Blocks while waiting for go routines to stop. This will set the
// working mode to off, so after this is called any spawned go routine will panic.
func (h *GoRoutineHandler) Close() {
	if h.flag.Inactivate() {
		h.mutex.Lock()
		h.group.Wait()
		h.mutex.Unlock()
	}
}

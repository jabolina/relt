package internal

import (
	"context"
	"io"
)

// Configuration for the coordinator.
type CoordinatorConfiguration struct {
	// Each Coordinator will handle only a single partition.
	// This will avoid peers with overlapping partitions.
	Partition string

	// Address for etcd server.
	Server string

	// Parent context that the Coordinator will derive it's own context.
	Ctx context.Context

	// Handler for managing goroutines.
	Handler *GoRoutineHandler
}

// Coordinator interface that should be implemented by the
// atomic broadcast handler.
// Commands should be issued through the coordinator to be delivered
// to other peers.
type Coordinator interface {
	io.Closer

	// Watch for changes on the partition.
	// After called, this method will start a new goroutine that only
	// returns when the Coordinator context is done.
	Watch(received chan<- Event) error

	// Issues an Event.
	// This will have the same effect as broadcasting a message
	// for every participant on the destination.
	Write(ctx context.Context, event Event) error
}

// Create a new Coordinator using the given configuration.
// The current implementation is the EtcdCoordinator, backed by etcd.
func NewCoordinator(configuration CoordinatorConfiguration) (Coordinator, error) {
	return newEtcdCoordinator(configuration)
}

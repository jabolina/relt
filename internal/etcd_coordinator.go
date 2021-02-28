package internal

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"time"
)

// EtcdCoordinator will use etcd for atomic broadcast.
type EtcdCoordinator struct {
	// Configuration parameters.
	configuration CoordinatorConfiguration

	// Current Coordinator context, created from the parent context.
	ctx context.Context

	// Function to cancel the current context.
	cancel context.CancelFunc

	// A client for interacting with the etcd server.
	cli *clientv3.Client
}

// Creates a new coordinator that is backed by the etcd atomic broadcast.
// This method will connect to the etcd server configured, so a chance of failure
// exists at this step.
// Only a simple configuration is available here.
func newEtcdCoordinator(configuration CoordinatorConfiguration) (Coordinator, error) {
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: 5 * time.Second,
		Endpoints:   []string{configuration.Server},
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(configuration.Ctx)
	coord := &EtcdCoordinator{
		configuration: configuration,
		cli:           cli,
		ctx:           ctx,
		cancel:        cancel,
	}
	return coord, nil
}

// Starts a new coroutine for watching the Coordinator partition.
// All received information will be published back through the channel
// received as parameter.
//
// After calling a routine will run bounded to the application lifetime.
func (e *EtcdCoordinator) Watch(received chan<- Event) error {
	watchChan := e.cli.Watch(e.ctx, e.configuration.Partition)
	watchChanges := func() {
		for response := range watchChan {
			select {
			case <-e.ctx.Done():
				return
			default:
				e.handleResponse(response, received)
			}
		}
	}
	e.configuration.Handler.Spawn(watchChanges)
	return nil
}

// Write the given event issuing a PUT request through the client.
func (e *EtcdCoordinator) Write(ctx context.Context, event Event) error {
	_, err := e.cli.Put(ctx, event.Key, string(event.Value))
	return err
}

// Stop the etcd client connection and stop the goroutines.
func (e *EtcdCoordinator) Close() error {
	e.cancel()
	return e.cli.Close()
}

// This method is responsible for handling events from the etcd client.
//
// This method will transform each received event into Event object and
// publish it back using the given channel. This method will hang while
// the messages are not consumed.
func (e *EtcdCoordinator) handleResponse(response clientv3.WatchResponse, received chan<- Event) {
	for _, event := range response.Events {
		received <- Event{
			Key:   string(event.Kv.Key),
			Value: event.Kv.Value,
			Error: nil,
		}
	}
}

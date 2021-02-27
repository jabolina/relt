package internal

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"io"
	"time"
)

// A single write requests to be applied to etcd.
type request struct {
	// Issuer writer context.
	ctx context.Context

	// Event to be sent to etcd.
	event Event

	// Channel to send response back.
	response chan error
}

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
// to other peers
type Coordinator interface {
	io.Closer

	// Watch for changes on the partition.
	// After called, this method will start a new goroutine that only
	// returns when the Coordinator context is done.
	Watch(received chan<- Event) error

	// Issues an Event.
	Write(ctx context.Context, event Event) <-chan error
}

// Create a new Coordinator using the given configuration.
// The current implementation is the EtcdCoordinator, backed by etcd.
func NewCoordinator(configuration CoordinatorConfiguration) (Coordinator, error) {
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: 30 * time.Second,
		Endpoints:   []string{configuration.Server},
	})
	if err != nil {
		return nil, err
	}
	kv := clientv3.NewKV(cli)
	ctx, cancel := context.WithCancel(configuration.Ctx)
	coord := &EtcdCoordinator{
		configuration: configuration,
		cli:           cli,
		kv:            kv,
		ctx:           ctx,
		cancel:        cancel,
		writeChan:     make(chan request),
	}
	configuration.Handler.Spawn(coord.writer)
	return coord, nil
}

// EtcdCoordinator will use etcd for atomic broadcast.
type EtcdCoordinator struct {
	// Configuration parameters.
	configuration CoordinatorConfiguration

	// Current Coordinator context, created from the parent context.
	ctx context.Context

	// Function to cancel the current context.
	cancel context.CancelFunc

	// A client for the etcd server.
	cli *clientv3.Client

	// The key-value entry point for issuing requests.
	kv clientv3.KV

	// Channel to receive write requests.
	writeChan chan request
}

// Listen and apply write requests.
// This will keep running while the application context is available.
// Receiving commands through the channel will ensure that they are
// applied synchronously to the etcd.
func (e *EtcdCoordinator) writer() {
	for {
		select {
		case <-e.ctx.Done():
			return
		case req := <-e.writeChan:
			_, err := e.kv.Put(req.ctx, req.event.Key, string(req.event.Value))
			req.response <- err
		}
	}
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

// Write the given event using the KV interface.
func (e *EtcdCoordinator) Write(ctx context.Context, event Event) <-chan error {
	res := make(chan error)
	e.writeChan <- request{
		ctx:      ctx,
		event:    event,
		response: res,
	}
	return res
}

// Stop the etcd client connection.
func (e *EtcdCoordinator) Close() error {
	e.cancel()
	return e.cli.Close()
}

// This method is responsible for handling events from the etcd client.
//
// This method will transform each received event into Event object and
// publish it back using the given channel. A buffered channel will be created
// and a goroutine will be spawned, so we can publish the received messages
// asynchronously without blocking. This can cause the Close to hold, if there
// exists pending messages to be consumed by the channel, this method can cause a deadlock.
func (e *EtcdCoordinator) handleResponse(response clientv3.WatchResponse, received chan<- Event) {
	buffered := make(chan Event, len(response.Events))
	defer close(buffered)

	e.configuration.Handler.Spawn(func() {
		for ev := range buffered {
			received <- ev
		}
	})

	for _, event := range response.Events {
		buffered <- Event{
			Key:   string(event.Kv.Key),
			Value: event.Kv.Value,
			Error: nil,
		}
	}
}

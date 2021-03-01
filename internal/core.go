package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	// Thrown when an action is performed on a core that was closed.
	coreWasShutdown = errors.New("core application was shutdown")

	// Thrown when trying to watch more than once.
	coreAlreadyWatching = errors.New("already watching partition")
)

// Configuration for the Core interface.
type CoreConfiguration struct {
	// Partition the Coordinator will work with.
	Partition string

	// Server address for the Coordinator.
	Server string

	// Default timeout to be applied when handling channels and
	// asynchronous operations.
	DefaultTimeout time.Duration
}

// Holds all flags used to manage the Core state.
// This is the same as an AtomicBoolean and is used internally
// to manage the core states.
type CoreFlags struct {
	// Flag for the shutdown state.
	shutdown Flag

	// Flag for watching state.
	watching Flag
}

// Core is the interface that will create the link between Relt requests
// and the Coordinator.
// Every command issued will be parsed here, and every command received should
// be handled here before going back to the client.
// Everything after this stage should care only about the atomic broadcast protocol
// and everything before should be abstracted as a simple communication primitive.
// This means that any parsing or state handling for the client should be done here.
type Core interface {
	io.Closer

	// Start listening for new messages.
	// This will receive messages from the atomic broadcast protocol
	// and parse to an object the client can handle.
	Listen() (<-chan Message, error)

	// Send a message asynchronously for the given partition.
	Send(ctx context.Context, dest string, data []byte) <-chan error
}

// Implements the Core interface.
// Holds all needed configuration for receiving commands
// from external world and sending through the Coordinator.
//
// This structure will be alive throughout the whole lifetime,
// opening some routines for handling requests and responses.
type ReltCore struct {
	// Context for bounding the application life.
	ctx context.Context

	// Called when the application Closes.
	finish context.CancelFunc

	// Manager for spawning goroutines and avoid leaking.
	handler *GoRoutineHandler

	// Coordinator to issues commands and receive Event.
	// The coordinator is the interface to reach the atomic
	// broadcast protocol.
	coord Coordinator

	// Channel for sending Message to the client.
	output chan Message

	// Flags for handling internal state.
	flags CoreFlags

	// Core configuration parameters.
	configuration CoreConfiguration
}

// Create a new ReltCore using the given configuration.
// As an effect, this will instantiate a Coordinator a failure
// can happen while handling connection to the atomic broadcast server.
func NewCore(configuration CoreConfiguration) (Core, error) {
	ctx, cancel := context.WithCancel(context.TODO())
	handler := NewRoutineHandler()
	coordinatorConf := CoordinatorConfiguration{
		Partition: configuration.Partition,
		Server:    configuration.Server,
		Ctx:       ctx,
		Handler:   handler,
	}
	coord, err := NewCoordinator(coordinatorConf)
	if err != nil {
		cancel()
		return nil, err
	}
	core := &ReltCore{
		ctx:     ctx,
		finish:  cancel,
		handler: handler,
		coord:   coord,
		output:  make(chan Message, 100),
		flags: CoreFlags{
			shutdown: Flag{},
			watching: Flag{},
		},
		configuration: configuration,
	}
	return core, nil
}

// After receiving an event from the Coordinator, the element
// should be parsed and sent back through the output channel.
func (r *ReltCore) publishMessageToListener(message Message) {
	select {
	case <-r.ctx.Done():
		return
	case r.output <- message:
		return
	case <-time.After(r.configuration.DefaultTimeout):
		// TODO: create something to handle timed out responses.
		fmt.Printf("not published %#v\n", message)
		return
	}
}

// The Listen method can be called only once.
// This will start a new goroutine that receives updates
// from the Coordinator and parse the information to a Message object.
//
// This goroutine will run while the application is not closed.
func (r *ReltCore) Listen() (<-chan Message, error) {
	if r.flags.shutdown.IsInactive() {
		return nil, coreWasShutdown
	}

	if r.flags.watching.Inactivate() {
		events := make(chan Event)
		if err := r.coord.Watch(events); err != nil {
			return nil, err
		}

		receiveAndParse := func() {
			for {
				select {
				case <-r.ctx.Done():
					return
				case event := <-events:
					r.publishMessageToListener(event.toMessage())
				}
			}
		}
		r.handler.Spawn(receiveAndParse)
		return r.output, nil
	}

	return nil, coreAlreadyWatching
}

// Send the given data to the peers that listen to the given partition.
// This is a broadcast message, which means that if _N_ nodes are
// subscribed for a partition, every node will receive the message.
func (r *ReltCore) Send(ctx context.Context, dest string, data []byte) <-chan error {
	response := make(chan error)
	writeRequest := func() {
		if r.flags.shutdown.IsActive() {
			event := Event{
				Key:   dest,
				Value: data,
			}
			response <- r.coord.Write(ctx, event)
		} else {
			response <- coreWasShutdown
		}
	}
	r.handler.Spawn(writeRequest)
	return response
}

// The Close method can be called only once.
// This will cancel the application context and start
// shutting down all goroutines.
//
// *This method will block until everything is finished.*
func (r *ReltCore) Close() error {
	if r.flags.shutdown.Inactivate() {
		defer close(r.output)
		if err := r.coord.Close(); err != nil {
			return err
		}
		r.finish()
		r.handler.Close()
		return nil
	}
	return coreWasShutdown
}

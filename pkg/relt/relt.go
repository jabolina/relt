package relt

import (
	"context"
	"errors"
	"github.com/jabolina/relt/internal"
)

var (
	ErrInvalidGroupAddress = errors.New("group address cannot be empty")
	ErrInvalidMessage      = errors.New("message cannot be nil")
	ErrContextClosed       = errors.New("cannot send message on closed transport")
	ErrPublishTimeout      = errors.New("took to long to publish message")
)

// The implementation for the Transport interface
// providing reliable communication between hosts.
//
// Every command must be issued through this struct,
// where a single object instance represents a peer that
// participates on the atomic broadcast protocol.
type Relt struct {
	// Holds the configuration.
	configuration Configuration

	// Holds the Core structure.
	// Every command received will be prepared and sent
	// through the core structure.
	core internal.Core
}

// Implements the Transport interface.
func (r *Relt) Consume() (<-chan internal.Message, error) {
	return r.core.Listen()
}

// Implements the Transport interface.
// Will broadcast a message to all peers that listen to the destination.
// This method is bounded by the given context lifetime and by the
// configured timeout.
func (r *Relt) Broadcast(ctx context.Context, message Send) error {
	if len(message.Address) == 0 {
		return ErrInvalidGroupAddress
	}

	if message.Data == nil {
		return ErrInvalidMessage
	}

	timeout, cancel := context.WithTimeout(ctx, r.configuration.DefaultTimeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return ErrContextClosed
	case <-timeout.Done():
		return ErrPublishTimeout
	case err := <-r.core.Send(timeout, string(message.Address), message.Data):
		return err
	}
}

// Implements the Transport interface.
func (r *Relt) Close() error {
	return r.core.Close()
}

// Creates a new instance of the reliable transport,
// and start all needed routines.
func NewRelt(configuration Configuration) (*Relt, error) {
	conf := internal.CoreConfiguration{
		Partition:      string(configuration.Exchange),
		Server:         configuration.Url,
		DefaultTimeout: configuration.DefaultTimeout,
	}
	core, err := internal.NewCore(conf)
	if err != nil {
		return nil, err
	}
	relt := &Relt{
		configuration: configuration,
		core:          core,
	}
	return relt, nil
}

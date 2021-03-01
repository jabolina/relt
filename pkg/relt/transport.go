package relt

import (
	"context"
	"github.com/jabolina/relt/internal"
	"io"
)

// When broadcasting a message must provide the group address.
type GroupAddress string

// Denotes an information that will be sent.
// By design, the group address cannot be empty and the Data to be
// sent cannot be nil, must be at least an empty slice.
type Send struct {
	// Which group the message will be sent.
	// This is the name of the exchange that must receive the message
	// and not an actual IP address.
	// If multiple peers are listening, all of them will receive the
	// message in the same order.
	Address GroupAddress

	// Data to be sent to the group.
	Data []byte
}

// A interface to offer a high level API for transport.
type Transport interface {
	io.Closer

	// A channel for consuming received messages.
	Consume() (<-chan internal.Message, error)

	// Broadcast a message to a given group.
	// See that this not work in the request - response model,
	// is just asynchronous message exchanges.
	//
	// The message is verified upon the required fields and an
	// error will be returned if something did not match the required
	// needs.
	//
	// A goroutine will be spawned and the message will be sent
	// through a channel, this channel is only closed when the
	// Relt transport is closed.
	Broadcast(ctx context.Context, message Send) error
}

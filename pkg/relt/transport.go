package relt

// When broadcasting or multicasting a message must provide
// the group address.
type GroupAddress string

// Denotes a received information or an error
// that occurred in the channel.
type Recv struct {
	// Received data or nil if is an error event.
	Data []byte

	// Returns an error back to the listener.
	Error error
}

// Denotes an information that will be sent.
// By design, the group address cannot be empty
// and the Data to be sent cannot be nil, must be at least
// and empty slice.
type Send struct {
	// Which group the message will be sent.
	// This is the name of the exchange that must receive
	// the message and not an actual IP address.
	Address GroupAddress

	// Data to be sent to the group.
	Data    []byte
}

// A interface to offer a high level API for transport.
// This transport is backed by a RabbitMQ using the quorum queues,
// that uses the Raft Protocol for consistency.
// Everything sent by this transport will not receive a direct
// response back, everything is just message passing simulating
// events that can occur.
type Transport interface {
	// A channel for consuming received messages.
	Consume() <-chan Recv

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
	// Relt transport is closed, so if the the transport is already
	// closed this function will panic.
	Broadcast(message Send) error

	// To shutdown the transport and stop consuming and
	// publishing new messages.
	Close()
}

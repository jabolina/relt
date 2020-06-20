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
type Send struct {
	Data []byte
}


// A interface to offer a high level API for transport.
// This transport is backed by a RabbitMQ using the quorum queues,
// that uses the Raft Protocol for consistency.
// Everything sent by this transport will not receive a direct
// response back, everything is just message passing simulating
// events that can occur.
type Transport interface {
	// A channel for consuming received messages.
	Consume() <- chan Recv

	// Broadcast a message to a given group.
	// See that this not work in the request - response model,
	// is just asynchronous message exchanges.
	Broadcast(address GroupAddress, message Send)

	// To shutdown the transport and stop consuming and
	// publishing new messages.
	Close()
}

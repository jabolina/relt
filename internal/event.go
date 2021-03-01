package internal

// Event is a structure handled by the Coordinator.
// Events are received and issued through the atomic broadcast.
type Event struct {
	// Key affected by the event.
	Key string

	// Value that should be applied if is sending the event or
	// applied value if the event was received.
	Value []byte

	// Only used when is a received event from the atomic broadcast,
	// this transport any error that happened.
	Error error
}

// Verify if the event is an error event.
func (e Event) IsError() bool {
	return e.Error != nil
}

// Parse the Event to the Message object.
func (e Event) toMessage() Message {
	return Message{
		Origin: e.Key,
		Data:   e.Value,
		Error:  e.Error,
	}
}

// Message is the structure handled by the Core.
// Messages are the available data sent to the client.
type Message struct {
	// Where the message was originated.
	// If a client `A` sends a messages to client `B`,
	// this value will be `B`.
	Origin string

	// Data transported.
	Data []byte

	// If an error happened.
	Error error
}

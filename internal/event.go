package internal

// Event is a structure handled by the Coordinator.
// Events are received and issued through the atomic broadcast.
type Event struct {
	Key   string
	Value []byte
	Error error
}

func (e Event) isError() bool {
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
	Origin string
	Data   []byte
	Error  error
}

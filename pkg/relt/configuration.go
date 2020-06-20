package relt

// Configuration used for creating a new instance
// of the Relt.
type ReltConfiguration struct {
	// The Relt name. Is not required, if empty a
	// random string will be generated to be used.
	// This must be unique, since it will be used to declare
	// the peer queue for consumption.
	Name string

	// Only plain auth is supported. The username + password
	// will be passed in the connection URL.
	Url string

	// This will be used to create an exchange on the RabbitMQ
	// broker. If the user wants to declare its own name for the
	// exchange, if none is passed, the value 'relt' will be used.
	//
	// When declaring multiple partitions, this must be configured
	// properly, since this will dictate which peers received the
	// messages. If all peers are using the same exchange then
	// is the same as all peers are a single partition.
	Exchange string
}

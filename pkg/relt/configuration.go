package relt

import (
	"errors"
	"strings"
)

var (
	ErrInvalidConfiguration = errors.New("invalid AMQP URL for connection")
	ErrInvalidReplication   = errors.New("replication value is invalid")
	DefaultExchangeName     = GroupAddress("relt")
)

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

	// How many peers should be replicated.
	Replication int

	// This will be used to create an exchange on the RabbitMQ
	// broker. If the user wants to declare its own name for the
	// exchange, if none is passed, the value 'relt' will be used.
	//
	// When declaring multiple partitions, this must be configured
	// properly, since this will dictate which peers received the
	// messages. If all peers are using the same exchange then
	// is the same as all peers are a single partition.
	Exchange GroupAddress
}

// Creates the default configuration for the Relt.
// The peer, thus the queue name will be randomly generated,
// the connection Url will connect to a local broker using
// the user `guest` and password `guest`.
// The default exchange will fallback to `relt`.
func DefaultReltConfiguration() *ReltConfiguration {
	return &ReltConfiguration{
		Name:        GenerateUID(),
		Url:         "amqp://guest:guest@127.0.0.1:5672/",
		Replication: 3,
		Exchange:    DefaultExchangeName,
	}
}

// Verify if the given Relt configuration is valid.
func (c *ReltConfiguration) ValidateConfiguration() error {
	if len(c.Name) == 0 {
		c.Name = GenerateUID()
	}

	if len(c.Exchange) == 0 {
		c.Exchange = DefaultExchangeName
	}

	if c.Replication <= 0 {
		return ErrInvalidReplication
	}

	if len(c.Url) == 0 {
		return ErrInvalidConfiguration
	}

	if strings.HasPrefix(c.Url, "amqp://") {
		return ErrInvalidConfiguration
	}

	return nil
}

// Configuration to be applied on a single publisher.
type publisherConfiguration struct {
	ReltConfiguration
}

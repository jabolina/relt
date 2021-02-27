package relt

import (
	"errors"
	"time"
)

var (
	ErrInvalidConfiguration = errors.New("invalid URL for connection")
	DefaultExchangeName     = GroupAddress("relt")
)

// Configuration used for creating a new instance
// of the Relt.
type Configuration struct {
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
	Exchange GroupAddress

	// Default timeout to be applied when handling asynchronous methods.
	DefaultTimeout time.Duration
}

// Creates the default configuration for the Relt.
// The peer, thus the queue name will be randomly generated,
// the connection Url will connect to a local broker using
// the user `guest` and password `guest`.
// The default exchange will fallback to `relt`.
func DefaultReltConfiguration() *Configuration {
	return &Configuration{
		Name:           GenerateUID(),
		Url:            "localhost:2379",
		Exchange:       DefaultExchangeName,
		DefaultTimeout: time.Second,
	}
}

// Verify if the given Relt configuration is valid.
func (c *Configuration) ValidateConfiguration() error {
	if len(c.Name) == 0 {
		c.Name = GenerateUID()
	}

	if len(c.Exchange) == 0 {
		c.Exchange = DefaultExchangeName
	}

	if len(c.Url) == 0 {
		return ErrInvalidConfiguration
	}

	if !IsUrl(c.Url) {
		return ErrInvalidConfiguration
	}

	if c.DefaultTimeout.Microseconds() <= 0 {
		return ErrInvalidConfiguration
	}

	return nil
}

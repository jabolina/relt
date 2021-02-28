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
	// The Relt name. Is not required, if empty a random string will
	// be generated to be used.
	Name string

	// URL used for connecting to the atomic broadcast protocol.
	Url string

	// This is the partition where the transport will act to. The client
	// will listen for only messages where the destination is the configured
	// partition.
	//
	// When declaring multiple partitions, this must be configured
	// properly, since this will dictate which peers received the
	// messages. If all peers are using the same exchange then
	// the clients will act as a single unity, where every peer
	// will receive all messages in the same order.
	Exchange GroupAddress

	// Default timeout to be applied when handling asynchronous methods.
	DefaultTimeout time.Duration
}

// Creates the default configuration for the Relt.
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

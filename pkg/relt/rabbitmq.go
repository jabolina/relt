package relt

import (
	"context"
	"github.com/streadway/amqp"
	"log"
)

// Compose struct of an AMQP connection and channel.
type session struct {
	*amqp.Connection
	*amqp.Channel
}

// The core structure responsible for
// sending and receiving messages through RabbitMQ.
type core struct {
	// A reference for the Relt context.
	ctx *reltContext

	// Context for the core structure.
	cancellable context.Context

	// Function for closing the core.
	cancel context.CancelFunc

	// Configuration for both Relt and AMQP.
	configuration ReltConfiguration

	// When connected or when a sessions arrives, publish into
	// the channel.
	connections chan chan session

	// Channel for publishing received messages.
	received chan Recv

	// Channel for receiving messages to be published.
	sending chan Send
}

// Subscribe to a queue and starts consuming.
//
// The queue will be declared as a quorum queue, since we
// need reliable communication and total order delivery.
// The quorum queues are backed by the Raft protocol, more
// information can be found https://www.rabbitmq.com/quorum-queues.html.
//
// The routing exchange will be to consume any message published,
// since we will only use this transport for broadcasting messages,
// if needed it can later be changed to emulate reliable unicast
// or multicast messages using the broadcast primitive, but there
// is no need at the moment.
//
// To any messages received, it will be publish onto the messages channel, and
// this method will be executed until the connections channel is not closed.
func (c core) subscribe() {
	for conn := range c.connections {
		sub := <-conn

		args := make(amqp.Table)
		args["x-queue-type"] = "quorum"
		if _, err := sub.QueueDeclare(c.configuration.Name, true, false, false, true, args); err != nil {
			log.Fatalf("failed declaring queue %s: %v", c.configuration.Name, err)
		}

		if err := sub.QueueBind(c.configuration.Name, "*", c.configuration.Exchange, false, nil); err != nil {
			log.Fatalf("failed binding queue %s: %v", c.configuration.Name, err)
		}

		consume, err := sub.Consume(c.configuration.Name, "", false, true, false, false, nil)
		if err != nil {
			log.Fatalf("failed consuming queue %s: %v", c.configuration.Name, err)
		}

	Consume:
		for {
			select {
			case packet, ok :=<-consume:
				if !ok {
					break Consume
				}
				c.received <- Recv{
					Data:  packet.Body,
					Error: nil,
				}
				sub.Ack(packet.DeliveryTag, false)
			case <-c.cancellable.Done():
				return

			}
		}
	}
}

// Publish a message into the RabbitMQ exchange.
//
// This method will keep polling until the sending channel
// is open. Any message received from the channel will be
// published into the configured exchange, thus broadcasting
// the messages to all connected consumers.
func (c core) publish() {
	for conn := range c.connections {
		var pending = make(chan Send, 1)
		pub := <-conn
		if pub.Confirm(false) == nil {
			pub.NotifyPublish(make(chan amqp.Confirmation, 1))
		}

	Publish:
		for {
			select {
			case body := <-pending:
				err := pub.Publish(c.configuration.Exchange, "*", false, false, amqp.Publishing{
					Body: body.Data,
				})
				if err != nil {
					pending <- body
					pub.Connection.Close()
					break Publish
				}
			case body, running := <-c.sending:
				if !running {
					return
				}
				pending <-body
			case <-c.cancellable.Done():
				return
			}
		}
	}
}

// Keeps running forever providing connections
// through the channel.
// This method will stop when the core context
// is done.
func (c core) connect() {
	sess := make(chan session)

	for {
		select {
		case c.connections <- sess:
		case <-c.cancellable.Done():
			return
		}

		conn, err := amqp.Dial(c.configuration.Url)
		if err != nil {
			log.Fatalf("failed connection [%s]: %v", c.configuration.Url, err)
		}


		ch, err := conn.Channel()
		if err != nil {
			log.Fatalf("could not defined channel: %v", err)
		}

		err = ch.ExchangeDeclare(c.configuration.Exchange, "fanout", true, false, false, false, nil)
		if err != nil {
			log.Fatalf("error declaring exchange %s: %v", c.configuration.Exchange, err)
		}

		select {
		case sess <- session{conn, ch}:
		case <-c.cancellable.Done():
			return
		}
	}
}

// Start all goroutines for publishing and consuming
// values for the queues.
// All spawned routines will be handled by the wait
// group from the Relt structure and this will stop
// when the context is canceled.
func (c core) start() {
	defer func() {
		close(c.connections)
		close(c.sending)
		close(c.received)
	}()

	c.ctx.spawn(c.connect)
	c.ctx.spawn(c.subscribe)
	c.ctx.spawn(c.publish)

	<-c.cancellable.Done()
}

// Cancel the context for the core structure.
func (c core) close() {
	c.cancel()
}

// Creates a new instance of the core structure.
// This will also start running and consuming messages.
func newCore(relt Relt) *core {
	ctx, cancel := context.WithCancel(context.Background())
	c := &core{
		ctx:           relt.ctx,
		cancellable:   ctx,
		cancel:        cancel,
		configuration: relt.configuration,
		connections:   make(chan chan session),
		received:      make(chan Recv),
		sending:       make(chan Send),
	}
	c.ctx.spawn(c.start)
	return c
}

package relt

import (
	"github.com/streadway/amqp"
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

	// Information about AMQP sessions.
	session session

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
func (c core) subscribe() error {
	for conn := range c.connections {
		sub := <-conn

		args := make(amqp.Table)
		args["x-queue-type"] = "quorum"
		if _, err := sub.QueueDeclare(c.configuration.Name, true, false, true, true, args); err != nil {
			return err
		}

		if err := sub.QueueBind(c.configuration.Name, "*", c.configuration.Exchange, false, nil); err != nil {
			return err
		}

		consume, err := sub.Consume(c.configuration.Name, "", false, true, false, false, nil)
		if err != nil {
			return err
		}

		for packet := range consume {
			c.received <- Recv{
				Data:  packet.Body,
				Error: nil,
			}
			sub.Ack(packet.DeliveryTag, false)
		}
	}
	return nil
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
			}
		}
	}
}

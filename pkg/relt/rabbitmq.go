package relt

import (
	"context"
	"github.com/streadway/amqp"
	"log"
	"time"
)

// The core structure responsible for
// sending and receiving messages through RabbitMQ.
type core struct {
	// A reference for the Relt context.
	invoker *invoker

	// Context for the core structure.
	ctx context.Context

	// Configuration for both Relt and AMQP.
	configuration ReltConfiguration

	// Channel to access the RabbitMQ broker.
	broker *amqp.Channel

	// Connection to the RabbitMQ broker.
	connection *amqp.Connection

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
func (c core) subscribe(consumer <-chan amqp.Delivery) {
	defer func() {
		log.Println("closing rabbitmq consumer")
		recover()
	}()

	for {
		select {
		case packet, ok := <-consumer:
			if !ok {
				log.Printf("consumer channel closed.")
				break
			}
			for err := c.broker.Ack(packet.DeliveryTag, false); err != nil; {
				log.Printf("failed acking. %v", err)
			}
			recv := Recv{
				Data:  packet.Body,
				Error: nil,
			}
			c.received <- recv
		case <-c.ctx.Done():
			return
		}
	}
}

// Publish a message on the RabbitMQ exchange.
// See that this will publish in a fanout exchange,
// this means that all queues related to the exchange
// will receive the message all will ignore the configured key.
//
// This will keep polling until the context is cancelled, and
// will receive messages to be published through the channel.
func (c core) publish(confirm <-chan amqp.Confirmation) {
	defer func() {
		log.Println("closing rabbitmq publisher")
	}()

	for {
		select {
		case _, ok := <-confirm:
			if !ok {
				log.Println("failed confirmation")
				continue
			}
		case body, running := <-c.sending:
			if !running {
				return
			}

			c.invoker.spawn(func() {
				err := c.broker.Publish(string(body.Address), "*", false, false, amqp.Publishing{
					Body: body.Data,
				})
				if err != nil {
					time.Sleep(150 * time.Millisecond)
					select {
					case <-time.After(150 * time.Millisecond):
						return
					case c.sending <- body:
						return
					}
				}
			})
		case <-c.ctx.Done():
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
		close(c.received)
		c.broker.Close()
		c.connection.Close()
	}()

	fails := make(chan *amqp.Error)
	c.connection.NotifyClose(fails)

	for !c.connection.IsClosed() {
		select {
		case <-c.ctx.Done():
			return
		case err := <-fails:
			log.Printf("error from connection. %v", err)
			c.received <- Recv{
				Data:  nil,
				Error: err,
			}
		}
	}
}

func (c *core) declarations() error {
	conn, err := amqp.Dial(c.configuration.Url)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	c.connection = conn
	c.broker = ch

	err = ch.ExchangeDeclare(string(c.configuration.Exchange), "fanout", false, true, false, false, nil)
	if err != nil {
		log.Fatalf("error declaring exchange %s: %v", c.configuration.Exchange, err)
	}

	_, err = ch.QueueDeclare(c.configuration.Name, true, false, true, false, nil)
	if err != nil {
		return err
	}

	if err = ch.QueueBind(c.configuration.Name, "*", string(c.configuration.Exchange), false, nil); err != nil {
		return err
	}

	consumer, err := ch.Consume(c.configuration.Name, "", false, true, false, false, nil)
	if err != nil {
		return err
	}

	confirm := make(chan amqp.Confirmation, 1)
	if ch.Confirm(false) == nil {
		ch.NotifyPublish(confirm)
	} else {
		close(confirm)
	}

	c.invoker.spawn(func() {
		c.subscribe(consumer)
	})

	c.invoker.spawn(func() {
		c.publish(confirm)
	})

	c.invoker.spawn(c.start)

	return nil
}

// Creates a new instance of the core structure.
// This will also start running and consuming messages.
func newCore(relt Relt) (*core, error) {
	c := &core{
		invoker:       relt.ctx,
		ctx:           relt.cancel,
		configuration: relt.configuration,
		received:      make(chan Recv),
		sending:       make(chan Send),
	}
	err := c.declarations()
	if err != nil {
		return nil, err
	}
	return c, nil
}

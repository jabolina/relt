package relt

import (
	"context"
	"github.com/hashicorp/raft"
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
	ctx *invoker

	// All publishers created, this will have the same
	// length as the Replication value configured.
	publishers []*publisher

	// Context for the core structure.
	cancellable context.Context

	// Function for closing the core.
	cancel context.CancelFunc

	// Configuration for both Relt and AMQP.
	configuration ReltConfiguration

	// When connected or when a sessions arrives, publish into
	// the channel.
	connections chan chan session

	// Channel to confirm that the publisher is ready
	// for the next message.
	ask chan bool

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
		var sub session
		select {
		case sub = <-conn:
		case <-c.cancellable.Done():
			return
		}

		args := make(amqp.Table)
		args["x-queue-type"] = "quorum"
		if _, err := sub.QueueDeclare(c.configuration.Name, true, false, false, true, args); err != nil {
			log.Fatalf("failed declaring queue %s: %v", c.configuration.Name, err)
		}

		if err := sub.QueueBind(c.configuration.Name, "*", string(c.configuration.Exchange), false, nil); err != nil {
			log.Fatalf("failed binding queue %s: %v", c.configuration.Name, err)
		}

		consume, err := sub.Consume(c.configuration.Name, "", false, true, false, false, nil)
		if err != nil {
			log.Fatalf("failed consuming queue %s: %v", c.configuration.Name, err)
		}

	Consume:
		for {
			select {
			case packet, ok := <-consume:
				c.configuration.Log.Info("packet arrived!!")
				if !ok {
					c.configuration.Log.Info("consume channel closed!!")
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

		c.configuration.Log.Info("stop recv for")
	}
}

// When the publisher confirms a message,
// it will notify through the channel to consume the next
// message. So retrieve the message from the queue head and
// delivers the message on the sending channel.
//
// This will be executed while the application is not closed.
func (c core) readQueue() {
	for {
		select {
		case <-c.ask:
			for _, p := range c.publishers {
				if p.amILeader() {
					next, err := p.Next()
					if err != nil {
						c.configuration.Log.Errorf("failed reading queue head: %v", err)
						continue
					}
					if next != nil {
						c.sending <- next.(Send)
					}
				}
			}
		case <-c.cancellable.Done():
			return
		}
	}
}

// Sends a message to the Raft protocol to be committed
// on the distributed queue.
// Once the message is committed, send through the message
// channel so any publisher can send the message to the
// RabbitMQ broker.
func (c core) publish(message Send) error {
	for _, p := range c.publishers {
		if p.amILeader() {
			err := p.Offer(message)
			if err != nil {
				return err
			}
			c.sending <- message
			return nil
		}
	}
	return ErrLeaderNotFound
}

// Keeps running forever providing connections
// through the channel.
// This method will stop when the core context
// is done.
func (c core) connect() {
	sess := make(chan session)
	defer func() {
		close(sess)
		close(c.connections)
	}()

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

		err = ch.ExchangeDeclare(string(c.configuration.Exchange), "fanout", true, false, false, false, nil)
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
		close(c.sending)
		close(c.received)
		close(c.ask)
	}()

	c.ctx.spawn(c.connect)
	c.ctx.spawn(c.subscribe)

	<-c.cancellable.Done()
}

// Cancel the context for the core structure.
func (c core) close() {
	for _, p := range c.publishers {
		p.stop()
	}
	c.cancel()
}

// Creates a new instance of the core structure.
// This will also start running and consuming messages.
func newCore(relt Relt) (*core, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &core{
		ctx:           relt.ctx,
		cancellable:   ctx,
		cancel:        cancel,
		configuration: relt.configuration,
		connections:   make(chan chan session),
		received:      make(chan Recv),
		sending:       make(chan Send),
		ask:           make(chan bool),
	}

	var peers []*publisher
	var servers []raft.Server
	for i := 0; i < relt.configuration.Replication; i++ {
		peer, err := newPublisher(c)
		if err != nil {
			return nil, err
		}
		peers = append(peers, peer)
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(peer.address),
			Address: raft.ServerAddress(peer.address),
		})
	}

	errorChan := make(chan bool, len(peers))
	bootstrap := func(peer *publisher) {
		err := peer.join(servers)
		errorChan <- err != nil
	}

	for _, peer := range peers {
		go bootstrap(peer)
	}

	responses := 0
	for failed := range errorChan {
		responses++
		if failed {
			return nil, ErrBoostrappingCluster
		}

		if responses == len(peers) {
			close(errorChan)
		}
	}

	c.publishers = peers
	c.ctx.spawn(c.start)
	return c, nil
}

package relt

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/streadway/amqp"
	"net"
	"os"
	"time"
)

// A publisher is a single member responsible for
// receiving messages from the distributed queue and
// publishing into the RabbitMQ.
// Each publisher is also a member on the Raft protocol,
// thus being able to change the values on the distributed queue.
type publisher struct {
	// The core RabbitMQ for listen for connections.
	rabbit *core

	// Holds a raft instance to issue new requests.
	raft *raft.Raft

	// The random generated address for the publisher
	// bind when using the raft protocol.
	address string

	// The distributed queue instance.
	queue *messageQueueStateMachine

	// The publisher cancellable context.
	context context.Context

	// Function the close the publisher
	done context.CancelFunc
}

// Publish a message into the RabbitMQ exchange.
//
// This method will keep polling until the sending channel
// is open. Any message received from the channel will be
// published into the configured exchange, thus broadcasting
// the messages to all connected consumers.
func (p publisher) publish() {
	for conn := range p.rabbit.connections {
		var pending = make(chan Send, 1)
		var pub session

		select {
		case pub = <-conn:
		case <-p.context.Done():
			return
		}

		confirm := make(chan amqp.Confirmation, 1)
		if pub.Confirm(false) == nil {
			pub.NotifyPublish(confirm)
		} else {
			close(confirm)
		}

	Publish:
		for {
			select {
			case confirmed, ok := <-confirm:
				if !ok {
					break Publish
				}

				p.rabbit.ask <- confirmed.Ack
			case body := <-pending:
				err := pub.Publish(string(body.Address), "*", false, false, amqp.Publishing{
					Body: body.Data,
				})
				if err != nil {
					pending <- body
					pub.Connection.Close()
					break Publish
				}
			case body, running := <-p.rabbit.sending:
				if !running {
					return
				}
				pending <- body
			case <-p.context.Done():
				return
			}
		}
	}
}

// Verify if the current publisher is the Raft leader.
func (p publisher) amILeader() bool {
	return p.raft.State() == raft.Leader
}

// Send an operation to be executed on the Raft protocol.
func (p publisher) apply(cmd *command) (interface{}, error) {
	if !p.amILeader() {
		return nil, ErrNotLeader
	}

	buff, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	fut := p.raft.Apply(buff, time.Second)
	if err := fut.Error(); err != nil {
		return nil, err
	}
	return fut.Response(), nil
}

// Publisher implements DQueue
func (p *publisher) Offer(value interface{}) error {
	cmd := &command{
		Op:    setop,
		Value: value.(Send),
	}
	_, err := p.apply(cmd)
	return err
}

// Publisher implements DQueue
func (p *publisher) Next() (interface{}, error) {
	cmd := &command{
		Op: nexop,
	}
	return p.apply(cmd)
}

// Publisher implements DQueue
func (p *publisher) Remove() error {
	cmd := &command{
		Op: delop,
	}
	_, err := p.apply(cmd)
	return err
}

// Start the publisher.
func (p publisher) start() {
	p.rabbit.ctx.spawn(p.publish)
	<-p.context.Done()
}

// Stop the peer.
func (p publisher) stop() {
	fut := p.raft.Shutdown()
	fut.Error()
	p.done()
}

// Start the Raft cluster.
func (p publisher) join(cluster []raft.Server) error {
	fut := p.raft.BootstrapCluster(raft.Configuration{Servers: cluster})
	return fut.Error()
}

// Creates a new publisher and prepare a new instance
// to join a Raft cluster.
// The created peer will bind to a random address to use
// the Raft protocol, and the directory to be used by the
// protocol will be placed at the /tmp folder.
func newPublisher(rabbitmq *core) (*publisher, error) {
	raftConf := raft.DefaultConfig()
	bind, err := GenerateRandomIP()
	if err != nil {
		return nil, err
	}

	raftConf.LocalID = raft.ServerID(bind)

	addr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		return nil, err
	}

	trans, err := raft.NewTCPTransport(bind, addr, 3, time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	dataDir := "/tmp/" + rabbitmq.configuration.Name
	if _, err = os.Stat(dataDir); os.IsNotExist(err) {
		if err = os.Mkdir(dataDir, 0755); err != nil {
			return nil, err
		}
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	mem := raft.NewInmemStore()
	queue := newStateMachine()
	ra, err := raft.NewRaft(raftConf, queue, mem, mem, snapshots, trans)
	if err != nil {
		return nil, err
	}

	ctx, done := context.WithCancel(context.Background())
	p := &publisher{
		rabbit:  rabbitmq,
		raft:    ra,
		address: bind,
		queue:   queue,
		context: ctx,
		done:    done,
	}
	rabbitmq.ctx.spawn(p.start)
	return p, nil
}

package relt

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"sync"
)

var (
	// Used when a write is executed on a peer that is not
	// the current raft leader.
	ErrNotLeader = errors.New("peer is not leader")

	// Used when no leader is found amongst the peers.
	ErrLeaderNotFound = errors.New("not found leader to publish message")

	// Operation used to insert a new value on the queue.
	setop operation = "set"

	// Used to get the head of the queue.
	nexop operation = "next"

	// Operation used to delete a value from the queue.
	delop operation = "del"
)

type operation string

// A type interface for building a distributed queue.
// This must be thread safe and available for accepting
// writes.
type DQueue interface {
	// Insert a new item into the queue.
	Offer(interface{}) error

	// Request for the next element of the queue.
	Next() (interface{}, error)

	// Remove the item at the given index and
	// returns the next value.
	Remove() error
}

// Command received from the Raft protocol.
type command struct {
	Op    operation `json:"op,omitempty"`
	Value Send      `json:"value,omitempty"`
}

// A queue for adding messages to be published.
// This queue will be distributed and will use the Raft protocol
// for consistency across all publishers.
type messageQueueStateMachine struct {
	// Mutex to control operations on the queue.
	mutex *sync.Mutex

	// The in memory FIFO queue.
	values *list.List
}

// Add a new element on the back of the queue.
func (q *messageQueueStateMachine) set(value Send) interface{} {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.values.PushBack(value)
	return value
}

// Return the head element of the queue.
func (q *messageQueueStateMachine) next() Send {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	value := q.values.Front()
	return value.Value.(Send)
}

// Remove the head element from the queue.
func (q *messageQueueStateMachine) delete() Send {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	value := q.values.Front()
	q.values.Remove(value)
	return value.Value.(Send)
}

// messageQueueStateMachine implements FSM
func (q *messageQueueStateMachine) Apply(log *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(log.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal cmd: %v", err))
	}
	switch c.Op {
	case setop:
		return q.set(c.Value)
	case nexop:
		return q.next()
	case delop:
		return q.delete()
	default:
		panic(fmt.Sprintf("command unknown: %s", c.Op))
	}
}

// messageQueueStateMachine implements FSM
func (q *messageQueueStateMachine) Snapshot() (raft.FSMSnapshot, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	o := list.New()
	head := q.values.Front()
	if head != nil {
		o.PushBack(head.Value)
		for head = head.Next(); head != nil; {
			o.PushBack(head.Value)
		}
	}
	return &snapshot{values: o}, nil
}

func newStateMachine() *messageQueueStateMachine {
	return &messageQueueStateMachine{
		mutex:  &sync.Mutex{},
		values: list.New(),
	}
}

// Snapshot of the concrete queue.
type snapshot struct {
	values *list.List
}

// messageQueueStateMachine implements FSM
func (q *messageQueueStateMachine) Restore(closer io.ReadCloser) error {
	o := list.New()
	if err := json.NewDecoder(closer).Decode(&o); err != nil {
		return err
	}
	q.values = o
	return nil
}

// snapshot implements FSMSnapshot
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		buff, err := json.Marshal(s.values)
		if err != nil {
			return err
		}
		if _, err := sink.Write(buff); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		return sink.Cancel()
	}

	return err
}

// snapshot implements FSMSnapshot
func (s *snapshot) Release() {}

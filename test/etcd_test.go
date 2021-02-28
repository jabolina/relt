package test

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/jabolina/relt/internal"
	"go.uber.org/goleak"
	"sync"
	"testing"
	"time"
)

func Test_ShouldReceiveAllCommands(t *testing.T) {
	defer goleak.VerifyNone(t)
	partition := "etcd-coordinator"
	testSize := 100
	clusterSize := 30
	ctx, cancel := context.WithCancel(context.Background())
	listenersGroup := &sync.WaitGroup{}
	writerGroup := &sync.WaitGroup{}
	initializeReplica := func(c internal.Coordinator, history *MessageHist) {
		msgChan := make(chan internal.Event)
		err := c.Watch(msgChan)
		if err != nil {
			t.Fatalf("failed starting to listen. %#v", err)
		}

		go func() {
			defer listenersGroup.Done()
			for {
				select {
				case recv := <-msgChan:
					if recv.Value == nil || len(recv.Value) == 0 {
						t.Errorf("received wrong data")
					}

					if recv.Error != nil {
						t.Errorf("error on consumed response. %v", recv.Error)
					}

					history.insert(string(recv.Value))
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	initializeCluster := func(size int) ([]internal.Coordinator, []*MessageHist) {
		var coordinators []internal.Coordinator
		var history []*MessageHist
		for i := 0; i < size; i++ {
			conf := internal.CoordinatorConfiguration{
				Partition: partition,
				Server:    "localhost:2379",
				Ctx:       ctx,
				Handler:   internal.NewRoutineHandler(),
			}
			coord, err := internal.NewCoordinator(conf)
			if err != nil {
				t.Fatalf("failed starting coordinator. %#v", err)
			}
			h := NewHistory()
			initializeReplica(coord, h)

			coordinators = append(coordinators, coord)
			history = append(history, h)
		}
		return coordinators, history
	}

	listenersGroup.Add(clusterSize)
	replicas, history := initializeCluster(clusterSize)

	entry := replicas[0]
	writerGroup.Add(testSize)
	for i := 0; i < testSize; i++ {
		write := func(data []byte) {
			defer writerGroup.Done()
			err := entry.Write(ctx, internal.Event{
				Key:   partition,
				Value: data,
				Error: nil,
			})

			if err != nil {
				t.Errorf("failed broadcasting. %#v", err)
			}
		}
		go write([]byte(fmt.Sprintf("%d", i)))
	}

	writerGroup.Wait()
	time.Sleep(10 * time.Second)
	cancel()
	listenersGroup.Wait()

	truth := history[0]
	if truth.size() != testSize {
		t.Errorf("should have size %d, found %d", testSize, truth.size())
	}

	for i, messageHist := range history {
		diff := truth.compare(*messageHist)
		if diff != 0 {
			t.Errorf("history differ at %d with %d different commands", i, diff)
		}
	}

	for _, replica := range replicas {
		err := replica.Close()
		if err != nil {
			t.Errorf("failed closing replica. %#v", err)
		}
	}
}

func Test_ShouldHaveEventsWhenDirectConnection(t *testing.T) {
	defer goleak.VerifyNone(t)
	partition := "etcd-coordinator"
	testSize := 100
	clusterSize := 30
	ctx, cancel := context.WithCancel(context.Background())
	listenersGroup := &sync.WaitGroup{}
	writerGroup := &sync.WaitGroup{}
	initializeReplica := func(c *clientv3.Client, history *MessageHist) {
		watchChan := c.Watch(ctx, partition)
		go func() {
			defer listenersGroup.Done()
			for {
				select {
				case res := <-watchChan:
					for _, event := range res.Events {
						history.insert(string(event.Kv.Value))
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	initializeCluster := func(size int) ([]*clientv3.Client, []*MessageHist) {
		var replicas []*clientv3.Client
		var history []*MessageHist
		for i := 0; i < size; i++ {
			e, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{"localhost:2379"},
				DialTimeout: 5 * time.Second,
			})
			if err != nil {
				t.Fatalf("failed connecting. %#v", err)
			}
			h := NewHistory()
			initializeReplica(e, h)

			replicas = append(replicas, e)
			history = append(history, h)
		}
		return replicas, history
	}

	listenersGroup.Add(clusterSize)
	replicas, history := initializeCluster(clusterSize)

	entry := replicas[0]
	writerGroup.Add(testSize)
	for i := 0; i < testSize; i++ {
		write := func(data string) {
			defer writerGroup.Done()
			_, err := entry.Put(ctx, partition, data)
			if err != nil {
				t.Errorf("failed broadcasting. %v", err)
			}
		}
		go write(fmt.Sprintf("%d", i))
	}

	writerGroup.Wait()
	time.Sleep(5 * time.Second)
	cancel()
	listenersGroup.Wait()

	truth := history[0]
	if truth.size() != testSize {
		t.Errorf("should have size %d, found %d", testSize, truth.size())
	}

	for i, messageHist := range history {
		diff := truth.compare(*messageHist)
		if diff != 0 {
			t.Errorf("history differ at %d with %d different commands", i, diff)
		}
	}

	for _, replica := range replicas {
		err := replica.Close()
		if err != nil {
			t.Errorf("failed closing replica. %#v", err)
		}
	}
}

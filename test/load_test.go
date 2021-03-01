package test

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jabolina/relt/pkg/relt"
	"sync"
	"testing"
	"time"
)

func Benchmark_LoadTestMultipleReplicas(b *testing.B) {
	partition := "bench-replicas-" + uuid.New().String()
	testSize := 200
	clusterSize := 50
	ctx, cancel := context.WithCancel(context.TODO())
	listenersGroup := &sync.WaitGroup{}
	writerGroup := &sync.WaitGroup{}
	initializeReplica := func(r *relt.Relt, history *MessageHist) {
		listener, err := r.Consume()
		if err != nil {
			b.Fatalf("failed starting consumer. %#v", err)
		}

		go func() {
			defer listenersGroup.Done()
			for {
				select {
				case recv := <-listener:
					if recv.Data == nil || len(recv.Data) == 0 {
						b.Errorf("received wrong data")
					}

					if recv.Error != nil {
						b.Errorf("error on consumed response. %v", recv.Error)
					}

					history.insert(string(recv.Data))
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	initializeCluster := func(size int) ([]*relt.Relt, []*MessageHist) {
		var replicas []*relt.Relt
		var history []*MessageHist
		for i := 0; i < size; i++ {
			conf := relt.DefaultReltConfiguration()
			conf.Name = partition + fmt.Sprintf("%d", i)
			conf.Exchange = relt.GroupAddress(partition)
			r, err := relt.NewRelt(*conf)
			if err != nil {
				b.Fatalf("failed connecting. %v", err)
			}
			h := NewHistory()
			initializeReplica(r, h)

			replicas = append(replicas, r)
			history = append(history, h)
		}
		return replicas, history
	}

	listenersGroup.Add(clusterSize)
	replicas, history := initializeCluster(clusterSize)

	entry := replicas[0]
	writerGroup.Add(testSize)
	for i := 0; i < testSize; i++ {
		write := func(data []byte) {
			defer writerGroup.Done()
			err := entry.Broadcast(ctx, relt.Send{
				Address: relt.GroupAddress(partition),
				Data:    data,
			})

			if err != nil {
				b.Errorf("failed broadcasting. %v", err)
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
		b.Errorf("should have size %d, found %d", testSize, truth.size())
	}

	for i, messageHist := range history {
		diff := truth.compare(*messageHist)
		if diff != 0 {
			b.Errorf("history differ at %d with %d different commands", i, diff)
		}
	}

	for _, replica := range replicas {
		err := replica.Close()
		if err != nil {
			b.Errorf("failed closing replica. %#v", err)
		}
	}
}

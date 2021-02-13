package test

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jabolina/relt/pkg/relt"
	"go.uber.org/goleak"
	"sync"
	"testing"
	"time"
)

// This test should validate that both replicas will be synchronized after
// a sequence of concurrent writes. At the end, both replicas should have
// the same message history, applied in the same order.
func Test_ReplicasShouldReceiveSameOrder(t *testing.T) {
	defer goleak.VerifyNone(t)
	partition := "synchronized-replicas-" + uuid.New().String()
	testSize := 100

	conf1 := relt.DefaultReltConfiguration()
	conf1.Name = partition + "-first"
	conf1.Exchange = relt.GroupAddress(partition)
	first, err := relt.NewRelt(*conf1)
	if err != nil {
		t.Fatalf("failed connecting. %v", err)
		return
	}
	defer first.Close()

	conf2 := relt.DefaultReltConfiguration()
	conf2.Name = partition + "-second"
	conf2.Exchange = relt.GroupAddress(partition)
	second, err := relt.NewRelt(*conf2)
	if err != nil {
		t.Fatalf("failed connecting. %v", err)
		return
	}
	defer second.Close()

	group := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.TODO())
	firstHistory := NewHistory()
	secondHistory := NewHistory()

	initialize := func(r *relt.Relt, history *MessageHist) {
		listener, err := r.Consume()
		if err != nil {
			t.Fatalf("failed starting consumer. %#v", err)
		}

		go func() {
			for {
				select {
				case recv := <-listener:
					if recv.Data == nil || len(recv.Data) == 0 {
						t.Errorf("received wrong data")
					}

					if recv.Error != nil {
						t.Errorf("error on consumed response. %v", recv.Error)
					}

					history.insert(string(recv.Data))
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	initialize(first, firstHistory)
	initialize(second, secondHistory)

	group.Add(testSize)
	for i := 0; i < testSize; i++ {
		write := func(data []byte) {
			defer group.Done()
			err := first.Broadcast(ctx, relt.Send{
				Address: conf2.Exchange,
				Data:    data,
			})

			if err != nil {
				t.Errorf("failed broadcasting. %v", err)
			}
		}
		go write([]byte(fmt.Sprintf("%d", i)))
	}

	group.Wait()
	time.Sleep(time.Second)
	cancel()
	diff := firstHistory.compare(*secondHistory)
	if diff > 0 {
		t.Fatalf("message history do not match. %d messages do not match", diff)
	}
}

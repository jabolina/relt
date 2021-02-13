package test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jabolina/relt/pkg/relt"
	"go.uber.org/goleak"
	"runtime"
	"sync"
	"testing"
	"time"
)

type hist struct {
	mutex *sync.Mutex
	data  map[string]bool
}

func (h *hist) validate(interval int) bool {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	for i := 0; i < interval; i++ {
		key := fmt.Sprintf("%d", i)
		_, ok := h.data[key]
		if !ok {
			return false
		}
	}
	return true
}

func (h *hist) exists(key string) bool {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	_, ok := h.data[key]
	return ok
}

func (h *hist) insert(key string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.data[key] = true
}

func PrintStackTrace(t *testing.T) {
	t.Log("get stacktrace\n")
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	t.Errorf("%s", buf)
}

func TestRelt_StartAndStop(t *testing.T) {
	defer goleak.VerifyNone(t)
	conf := relt.DefaultReltConfiguration()
	r, err := relt.NewRelt(*conf)
	if err != nil {
		t.Fatalf("failed connecting. %v", err)
		return
	}
	r.Close()
}

func TestRelt_PublishAndReceiveMessage(t *testing.T) {
	defer goleak.VerifyNone(t)
	conf := relt.DefaultReltConfiguration()
	conf.Name = "random-test-name"
	r, err := relt.NewRelt(*conf)
	if err != nil {
		t.Fatalf("failed connecting. %v", err)
		return
	}
	defer r.Close()

	data := []byte("hello")
	group := &sync.WaitGroup{}
	group.Add(1)
	listener, err := r.Consume()
	if err != nil {
		t.Fatalf("failed listening. %#v", err)
	}
	go func() {
		defer group.Done()
		select {
		case recv := <-listener:
			if !bytes.Equal(data, recv.Data) {
				t.Fatalf("data not equals. expected %s found %s", string(data), string(recv.Data))
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out receiving")
		}
	}()

	err = r.Broadcast(context.TODO(), relt.Send{
		Address: conf.Exchange,
		Data:    data,
	})

	if err != nil {
		t.Fatalf("failed broadcasting. %v", err)
	}

	group.Wait()
}

func Test_MultipleConnections(t *testing.T) {
	defer goleak.VerifyNone(t)
	var connections []*relt.Relt
	testSize := 150
	for i := 0; i < testSize; i++ {
		conf := relt.DefaultReltConfiguration()
		conf.Name = fmt.Sprintf("random-test-name-%d", i)
		r, err := relt.NewRelt(*conf)
		if err != nil {
			t.Errorf("failed connecting %s. %v", conf.Name, err)
		}
		connections = append(connections, r)
	}

	time.Sleep(time.Second)

	for i, connection := range connections {
		done := make(chan bool)
		go func() {
			connection.Close()
			done <- true
		}()
		select {
		case <-done:
			break
		case <-time.After(time.Second):
			t.Errorf("too long disconnecting %d", i)
			break
		}
		close(done)
	}
}

func Test_LoadPublishAndReceiveMessage(t *testing.T) {
	defer goleak.VerifyNone(t)
	conf := relt.DefaultReltConfiguration()
	conf.Name = "random-test-name"
	r, err := relt.NewRelt(*conf)
	if err != nil {
		t.Fatalf("failed connecting. %v", err)
		return
	}
	defer r.Close()
	group := &sync.WaitGroup{}
	testSize := 1000

	listener, err := r.Consume()
	if err != nil {
		t.Fatalf("failed listening. %#v", err)
	}

	group.Add(testSize)
	received := hist{
		mutex: &sync.Mutex{},
		data:  make(map[string]bool),
	}
	for i := 0; i < testSize; i++ {
		data := []byte(fmt.Sprintf("%d", i))
		go func() {
			defer group.Done()
			select {
			case recv := <-listener:
				if recv.Error != nil {
					t.Errorf("error on consumed response. %v", recv.Error)
				}
				key := string(recv.Data)
				if received.exists(key) {
					t.Errorf("data %s already received", key)
				}
				received.insert(key)
				return
			case <-time.After(time.Second):
				t.Errorf("timed out receiving")
				return
			}
		}()

		err = r.Broadcast(context.TODO(), relt.Send{
			Address: conf.Exchange,
			Data:    data,
		})

		if err != nil {
			t.Errorf("failed broadcasting. %v", err)
		}
	}

	done := make(chan bool)
	go func() {
		group.Wait()
		done <- true
	}()

	select {
	case <-done:
		received.validate(testSize)
		return
	case <-time.After(20 * time.Second):
		t.Errorf("too long to routines to finish")
		return
	}
}

func Test_LoadPublishAndReceiveMultipleConnections(t *testing.T) {
	defer goleak.VerifyNone(t)
	conf1 := relt.DefaultReltConfiguration()
	conf1.Name = "random-test-name-st"
	first, err := relt.NewRelt(*conf1)
	if err != nil {
		t.Fatalf("failed connecting. %v", err)
		return
	}
	defer first.Close()

	conf2 := relt.DefaultReltConfiguration()
	conf2.Name = "random-test-name-snd"
	conf2.Exchange = "relt-exch-2"
	second, err := relt.NewRelt(*conf2)
	if err != nil {
		t.Fatalf("failed connecting. %v", err)
		return
	}
	defer second.Close()

	group := &sync.WaitGroup{}
	testSize := 150

	listener, err := second.Consume()
	if err != nil {
		t.Fatalf("failed listening. %#v", err)
	}

	done, cancel := context.WithCancel(context.TODO())
	received := hist{
		mutex: &sync.Mutex{},
		data:  make(map[string]bool),
	}
	// See that when handling multiple request concurrently,
	// the relt application is bounded by how much a client can
	// consume. We can only publish through a channel while the client
	// can consume the channel.
	// In this example we have only a single lonely consumer handling
	// all messages. This is why the testSize is not an excessive value.
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

				key := string(recv.Data)
				if received.exists(key) {
					t.Errorf("data %s already received", key)
				}
				received.insert(key)
			case <-done.Done():
				return
			}
		}
	}()

	group.Add(testSize)
	for i := 0; i < testSize; i++ {
		write := func(data []byte) {
			defer group.Done()
			err := first.Broadcast(done, relt.Send{
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
	received.validate(testSize)
}

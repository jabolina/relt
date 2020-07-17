package test

import (
	"bytes"
	"fmt"
	"github.com/jabolina/relt/pkg/relt"
	"sync"
	"testing"
	"time"
)

func TestRelt_StartAndStop(t *testing.T) {
	conf := relt.DefaultReltConfiguration()
	relt, err := relt.NewRelt(*conf)
	if err != nil {
		t.Fatalf("failed connecting. %v", err)
		return
	}
	relt.Close()
}

func TestRelt_PublishAndReceiveMessage(t *testing.T) {
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
	go func() {
		defer group.Done()
		select {
		case recv := <-r.Consume():
			if !bytes.Equal(data, recv.Data) {
				t.Fatalf("data not equals. expected %s found %s", string(data), string(recv.Data))
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out receiving")
		}
	}()

	err = r.Broadcast(relt.Send{
		Address: conf.Exchange,
		Data:    data,
	})

	if err != nil {
		t.Fatalf("failed broadcasting. %v", err)
	}

	group.Wait()
}

func Test_MultipleConnections(t *testing.T) {
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

	for i := 0; i < testSize; i++ {
		group.Add(1)
		data := []byte(fmt.Sprintf("%d", i))
		go func() {
			defer group.Done()
			select {
			case recv := <-r.Consume():
				if recv.Error != nil {
					t.Errorf("error on consumed response. %v", recv.Error)
				}
				return
			case <-time.After(time.Second):
				t.Errorf("timed out receiving")
				return
			}
		}()

		err = r.Broadcast(relt.Send{
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
		return
	case <-time.After(20 * time.Second):
		t.Errorf("too long to routines to finish")
		return
	}
}

func Test_LoadPublishAndReceiveMultipleConnection(t *testing.T) {
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
	testSize := 1000

	for i := 0; i < testSize; i++ {
		group.Add(1)
		read := func() {
			defer group.Done()
			select {
			case recv := <-second.Consume():
				if recv.Data == nil || len(recv.Data) == 0 {
					t.Errorf("received wrong data")
				}

				if recv.Error != nil {
					t.Errorf("error on consumed response. %v", recv.Error)
				}
				return
			case <-time.After(500 * time.Millisecond):
				t.Errorf("timed out receiving")
				return
			}
		}

		data := []byte(fmt.Sprintf("%d", i))
		go read()

		err = first.Broadcast(relt.Send{
			Address: conf2.Exchange,
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
		return
	case <-time.After(20 * time.Second):
		t.Errorf("too long to routines to finish")
		return
	}
}

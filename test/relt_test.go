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

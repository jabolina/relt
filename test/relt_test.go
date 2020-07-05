package test

import (
	"bytes"
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

package main

import (
	"bufio"
	"context"
	"github.com/jabolina/relt/pkg/relt"
	"github.com/prometheus/common/log"
	"io"
	"os"
	"os/signal"
)

func produce(r *relt.Relt, reader io.Reader, ctx context.Context) {
	for {
		println("Write message:")
		scan := bufio.NewScanner(reader)
		for scan.Scan() {
			message := relt.Send{
				Address: relt.DefaultExchangeName,
				Data:    scan.Bytes(),
			}
			log.Infof("Publishing message %s to group %s", string(message.Data), message.Address)
			if err := r.Broadcast(ctx, message); err != nil {
				log.Errorf("failed sending %#v: %v", message, err)
			}
		}
	}
}

func consume(r *relt.Relt, ctx context.Context) {
	listener, err := r.Consume()
	if err != nil {
		return
	}

	for {
		select {
		case message := <-listener:
			if message.Error != nil {
				log.Errorf("message with error: %#v", message)
			}
			log.Infof("Received [%s]", string(message.Data))
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	conf := relt.DefaultReltConfiguration()
	conf.Name = "local-test"
	r, _ := relt.NewRelt(*conf)
	ctx, done := context.WithCancel(context.Background())

	go func() {
		produce(r, os.Stdin, ctx)
	}()

	go func() {
		consume(r, ctx)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			done()
		}
	}()

	<-ctx.Done()
	r.Close()
}

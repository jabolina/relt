package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"relt/pkg/relt"
	"time"
)

var log = relt.NewDefaultLogger()

func produce(r *relt.Relt, reader io.Reader) {
	for {
		scan := bufio.NewScanner(reader)
		for scan.Scan() {
			message := relt.Send{
				Address: relt.DefaultExchangeName,
				Data:    scan.Bytes(),
			}
			log.Infof("Publishing message %s to group %s", string(message.Data), message.Address)
			if err := r.Broadcast(message); err != nil {
				log.Errorf("failed sending %#v: %v", message, err)
			}
		}
	}
}

func consume(r *relt.Relt) {
	for {
		message := <-r.Consume()
		if message.Error != nil {
			log.Errorf("message with error: %#v", message)
		}
		log.Infof("Received [%s]", string(message.Data))
	}
}

func main() {
	conf := relt.DefaultReltConfiguration()
	conf.Name = "local-test"
	relt := relt.NewRelt(*conf)
	ctx, done := context.WithCancel(context.Background())

	go func() {
		produce(relt, os.Stdin)
	}()

	go func() {
		consume(relt)
	}()

	go func() {
		time.Sleep(time.Minute)
		done()
	}()

	<-ctx.Done()
	relt.Close()
}

package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {
	addr := "localhost:9092"

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	broker := NewBroker(addr)
	server, err := sarama.NewServer(addr, broker)

	if err != nil {
		panic(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	if err := server.Close(); err != nil {
		panic(err)
	}
}

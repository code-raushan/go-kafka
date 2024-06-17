package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main(){
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic: "broker_topic",
		GroupID: "broker_group",	
	})

	defer kafkaReader.Close()

	fmt.Println("Starting to read the message from the topic: broker_topic")

	for {
		msg, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("could not read message: %v", err)
		}

		fmt.Printf("received msg %s\n", string(msg.Value))
	}
}
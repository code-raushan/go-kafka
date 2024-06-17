package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
)

type CustomBalancer struct{}

func (b *CustomBalancer) Balance(msg kafka.Message, partitions ...int) int {
	firstChar := strings.ToLower(string(msg.Value[0]))
	if firstChar > "n" {
		return partitions[1%len(partitions)]
	}
	return partitions[0]
}

func main(){
	m := flag.String("m", "", "message to send to the topic")
	flag.Parse()

	if *m == ""{
		fmt.Println("message not passed")
	}

	CustomBalancer := &CustomBalancer{}

	kafkaWriter := kafka.Writer{
		Addr: kafka.TCP("localhost:9092"),
		Topic: "broker_topic",
		Balancer: CustomBalancer,	
	}

	defer kafkaWriter.Close()

	err := kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(*m),
	})

	if err != nil {
		panic(err.Error())
	}	
}

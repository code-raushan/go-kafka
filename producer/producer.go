package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
)

func main(){
	m := flag.String("m", "", "message to send to the topic")
	flag.Parse()

	if *m == ""{
		fmt.Println("message not passed")
	}

	kafkaWriter := kafka.Writer{
		Addr: kafka.TCP("localhost:9092"),
		Topic: "broker_topic",
	}

	defer kafkaWriter.Close()

	firstChar := strings.ToLower(string((*m)[0])) 

	var partition int

	if firstChar > "n"{
		partition = 1
	}else {
		partition = 0
	}

	err := kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(*m),
		Partition: partition,
	})

	if err != nil {
		panic(err.Error())
	}

	
}
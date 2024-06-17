package main

import (
	"flag"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

func main(){
	topic := flag.String("topic", "", "Specify the topic name to create")
	partitions := flag.Int("partitions", 1, "Specify the partition count")
	flag.Parse()

	if *topic == "" {
		fmt.Println("Topic name not provided")
	}else{
		fmt.Printf("Topic : %s\n", *topic)
	}

	brokerAddr := "localhost:9092"

	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		panic(err.Error())
	}

	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}

	var controllerConn *kafka.Conn

	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}

	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic: *topic,
			NumPartitions: *partitions,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("Topic %s created", *topic)

}

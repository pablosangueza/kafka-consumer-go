package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	// create a Kafka consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// create a Kafka consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %s", err.Error())
	}
	defer consumer.Close()

	// subscribe to a Kafka topic
	partitionConsumer, err := consumer.ConsumePartition("test", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Error consuming partition: %s", err.Error())
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message with key=%s and value=%s\n", string(msg.Key), string(msg.Value))
		case err := <-partitionConsumer.Errors():
			log.Fatalf("Error receiving message from Kafka: %s", err.Error())
		}
	}

}

package main

import (
	"fmt"

	"github.com/IBM/sarama"
)

var (
	Brokers = []string{"localhost:9092"}
	Topic   = "topic-test"
)

func main() {
	publisher, _ := getProducer()

	msg := getMensage("se escreva no canal")

	partion, offset, _ := publisher.SendMessage(msg)

	fmt.Println(partion, offset)
}

func getProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	return sarama.NewSyncProducer(Brokers, config)
}

func getMensage(msg string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic:     Topic,
		Partition: -1,
		Value:     sarama.StringEncoder(msg),
	}
}

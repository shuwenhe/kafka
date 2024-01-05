package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
)

func main() {
	// 1.Create consumer
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// 2.Create consumer partition
	partitionConsumer, err := consumer.ConsumePartition("mqtt", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal(err)
	}

	// 通过 os/signal 包，捕获 interrupt 信号，优雅地关闭程序
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		// 3.Create message
	ConsumerLoop:
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				fmt.Printf("Received message: %s\n", msg.Value)
			case err := <-partitionConsumer.Errors():
				fmt.Println("Error:", err)
			case <-signals:
				fmt.Println("Interrupt signal received, shutting down consumer...")
				break ConsumerLoop
			}
		}
	}()

	wg.Wait()
	fmt.Println("Consumer shutting down.")
}

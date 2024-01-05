package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // 等待本地确认
	config.Producer.Compression = sarama.CompressionSnappy   // 使用 Snappy 压缩
	config.Producer.Flush.Frequency = 500 * time.Millisecond // 每 500 毫秒刷新一次

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	signals := make(chan os.Signal, 1) // 通过 os/signal 包，捕获 interrupt 信号，优雅地关闭程序
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup

ProducerLoop:
	for {
		select {
		case <-signals:
			fmt.Println("Interrupt signal received, shutting down...")
			break ProducerLoop
		default:
			var wg sync.WaitGroup
			countCurrentLimit := 10
			workQueue := make(chan int, countCurrentLimit)
			for i := 0; i <= countCurrentLimit; i++ { // 模拟生成100个数据
				input := fmt.Sprintf("Data %d", i)
				message := &sarama.ProducerMessage{
					Topic: "mqtt",
					Value: sarama.StringEncoder(input),
				}
				wg.Add(1)
				go func(i int, workQueue chan int, msg *sarama.ProducerMessage) { // 使用 go 协程发送消息，以避免阻塞主循环
					select {
					case producer.Input() <- msg:
						fmt.Println("Message sent to partition", msg.Partition)
					case err := <-producer.Errors():
						fmt.Println("Failed to produce message:", err)
					}
				}(i, workQueue, message)
			}
		}
	}
	wg.Wait()
	fmt.Println("Producer shutting down.")
}

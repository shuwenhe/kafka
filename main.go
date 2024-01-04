package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
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

	// 通过 os/signal 包，捕获 interrupt 信号，优雅地关闭程序
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg      sync.WaitGroup
		encoder = sarama.StringEncoder
	)

ProducerLoop:
	for {
		select {
		case input := <-yourDataChannel: // 从你的数据源读取数据
			message := &sarama.ProducerMessage{
				Topic: "mqtt",
				Value: encoder(input), // 将数据编码为字节数组
			}

			// 使用 go 协程发送消息，以避免阻塞主循环
			go func(msg *sarama.ProducerMessage) {
				select {
				case producer.Input() <- msg:
					fmt.Println("Message sent to partition", msg.Partition)
				case err := <-producer.Errors():
					fmt.Println("Failed to produce message:", err)
				}
			}(message)

		case <-signals:
			fmt.Println("Interrupt signal received, shutting down...")
			break ProducerLoop
		}
	}

	wg.Wait()
	fmt.Println("Producer shutting down.")
}

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

type Config struct {
}

type Client interface {
}

type ProducerMessage struct {
	Topic       string
	expectation chan *ProducerError
}
type asyncProducer struct {
	client    Client
	successes chan *ProducerMessage
}

type syncProducer struct {
	producer *asyncProducer
	wg       sync.WaitGroup
}

type ProducerError struct {
	Msg *ProducerMessage
	Err error
}

type AsyncProducer interface {
	Input() chan<- *ProducerMessage
}

type ProducerErrors []*ProducerError

func (pe ProducerError) Error() string {
	return fmt.Sprintf("kafka: Failed to produce message to topic %s:%s", pe.Msg.Topic, pe.Err)
}

func (pe ProducerErrors) Error() string {
	return fmt.Sprintf("kafka: Failed to deliver %d message.", len(pe))
}

func (p *asyncProducer) Input() chan<- *ProducerMessage {
	return p.successes
}

func (sp *syncProducer) SendMessages(msgs []*ProducerMessage) error {
	expectations := make(chan chan *ProducerError, len(msgs))
	go func() {
		for _, msg := range msgs {
			expectation := make(chan *ProducerError, 1)
			msg.expectation = expectation
			sp.producer.Input() <- msg
			expectations <- expectation
		}
		close(expectations)
	}()

	var errors ProducerErrors
	for expectation := range expectations {
		if pErr := <-expectation; pErr != nil {
			errors = append(errors, pErr)
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func main() {
	// 1.生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // ACK,发送完数据需要leader和follow都确认
	config.Producer.Compression = sarama.CompressionSnappy   // 使用 Snappy 压缩
	config.Producer.Flush.Frequency = 500 * time.Millisecond // 每 500 毫秒刷新一次
	// 2.连接kafka
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
			const countCurrentLimit = 10
			workQueue := make(chan int, countCurrentLimit)
			for i := 0; i <= countCurrentLimit; i++ {
				input := fmt.Sprintf("Data %d", i)
				// 3.封装消息
				message := &sarama.ProducerMessage{
					Topic: "mqtt",
					Value: sarama.StringEncoder(input),
				}
				workQueue <- i
				wg.Add(1)
				// 4.发送消息
				go func(i int, workQueue chan int, msg *sarama.ProducerMessage) { // 使用 go 协程发送消息，以避免阻塞主循环
					defer func() {
						<-workQueue
						wg.Done()
					}()
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
